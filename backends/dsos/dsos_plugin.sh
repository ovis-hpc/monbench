# dsos_plugin.sh -- DSOS backend hooks, sourced by bin/node.sh.
#
# DSOS runs one dsosd per server; a query fans out to all of them and the
# client merges the results. This plugin provides write, service and clean
# hooks, and its query adapters are in query/dsos_query.py.
#
# The DSOS writer connects to the local dsosd through a session file and
# writes records through the distributed API.  dsosd must be running
# (serve-write starts it before the write phase).  Containers end up in
# the same location the SOS writer would use, so DSOS-written data can be
# queried by either the sos or dsos backend.
#
# Hooks (see backends/README.md):
#   plugin_write WRITER MODE INDEX ID     write through dsosd; the plugin
#                                         constructs the storage name from
#                                         DS.name and the writer, and records
#                                         it in an ingest-handoff meta file at
#                                         OUTPUT_DIR/meta/ingest-handoff-<writer>.json
#   plugin_serve_start ID / plugin_serve_run ID / plugin_serve_stop /
#   plugin_serve_status                   manage dsosd on this server
#   plugin_clean ID                       delete containers and a stale rpcbind
#                                         registration; refuses while a dsosd
#                                         is still running (nodes/ and meta/
#                                         are removed by the harness)
#
# Available from node.sh: HOST, PLUGIN_DIR, log, die, run_logged, ds_name,
# dataset_args (array), and every setting from bench.conf and dsos_backend.conf.
#
# Set DRY_RUN=1 (bin/run.sh's -d flag) to log the dsosd/writer command this
# host would run (via run_logged() or an inline "+ ..." log line) without
# actually starting/stopping dsosd, writing, or deleting anything. The
# writer reads DATASET from its environment rather than a flag, so it is
# passed explicitly to run_logged (see bin/node.sh's run_logged doc comment)
# -- the logged line is then standalone-runnable, not just a record of what
# ran inside this script.

# RPC program number of the D/SOS service (program SOSDB in sosapi.x).
SOSDB_PROG=40009862
SOSDB_VERS=1
# Per-node runtime files under nodes/<hostname>/run/.
PIDFILE="${OUTPUT_DIR}/nodes/${HOST}/run/dsosd.pid"
# Daemon logs go to the shared logs/ directory.
LOGFILE="${OUTPUT_DIR}/logs/dsosd-${HOST}.log"
# The dsosd directory config is backend-level metadata (shared across nodes).
DIRFILE="${OUTPUT_DIR}/meta/dsosd-directory.json"

# This backend's own directory (for the writer and make_schema.py, which
# are shared with the sos backend).
DSOS_DIR="${BENCH_DIR}/backends/dsos"
SOS_DIR="${BENCH_DIR}/backends/sos"

container_path() { # SERVER_ID TARGET
	echo "${SOS_DATA_ROOT}/$1/$2"
}

# Session file pointing to the local dsosd, used by the writer.
# Backend-level metadata (shared across nodes).
write_session_file() {
	local path="${OUTPUT_DIR}/meta/dsos-write-session.txt"
	mkdir -p "${OUTPUT_DIR}/meta"
	echo "${HOST}" >"${path}"
	echo "${path}"
}

# ----------------------------------------------------------------- write

plugin_write() { # WRITER MODE INDEX SERVER_ID
	local writer="$1" mode="$2" index="$3" id="$4"
	# Construct the storage name from the dataset definition's name field
	# and the writer name. Live mode uses a separate target so it does not
	# overwrite the full data set.
	local ds target
	ds="$(ds_name)"
	if [[ "$mode" == "live" ]]; then
		target="${ds}_live"
	else
		target="${ds}_${writer}"
	fi
	local path template session
	path="$(container_path "$id" "$target")"
	# Start from an empty container so that reruns do not add duplicates.
	run_logged rm -rf "$path"
	mkdir -p "$(dirname "$path")" "${OUTPUT_DIR}/meta"
	# Reuse the sos backend's make_schema.py — the schema is identical.
	# The schema template is backend-level metadata (shared across nodes).
	template="${OUTPUT_DIR}/meta/sos-schema.json"
	"${PYTHON}" "${SOS_DIR}/make_schema.py" "${DATASET}" "$template"
	# Update the dsosd directory file so it knows about this target.
	write_directory "$id"
	session="$(write_session_file)"
	local args=(--session "$session" --target "$target"
		--template "$template" --node-index "$index"
		"${dataset_args[@]}")
	log "writing ${target} (dsos writer, through dsosd on ${HOST}) to ${path}"
	# Record where we stored data so the query side can find it.
	_dsos_write_meta "$writer" "$target" "$id"
	case "$writer" in
	dsos)
		run_logged DATASET="${DATASET}" -- "${PYTHON}" "${DSOS_DIR}/writer/dsos_write.py" "${args[@]}"
		;;
	*)
		die "the dsos backend has no writer '$writer' (available: dsos)"
		;;
	esac
}

# Write an ingest-handoff meta file recording where the writer stored data, so the
# query side can find it without reconstructing paths by convention.
# See EXTENDING.md, "Plugin meta files".
_dsos_write_meta() { # WRITER TARGET SERVER_ID
	local writer="$1" target="$2" id="$3"
	local meta_dir="${OUTPUT_DIR}/meta"
	mkdir -p "$meta_dir"
	cat >"${meta_dir}/ingest-handoff-${writer}.json" <<-EOF
	{
	  "target": "${target}",
	  "backend": "dsos",
	  "data_root": "${SOS_DATA_ROOT}",
	  "container_path": "$(container_path "$id" "$target")"
	}
	EOF
}

# Build the dsosd directory file that maps target names to container paths.
# Uses DS.name and WRITERS to predict the target names each writer will use.
# The directory file is backend-level metadata (shared across nodes).
#
# Multiple servers share the same DIRFILE on the shared filesystem.  Each
# call adds (or replaces) the entry for one SERVER_ID, preserving entries
# previously written by other servers.  This avoids the race where a later
# server's write_directory() overwrites an earlier server's entries, which
# would leave the earlier server's dsosd unable to find its containers if
# it re-reads the file.
write_directory() { # SERVER_ID
	local id="$1" w name
	local ds
	ds="$(ds_name)"
	mkdir -p "${OUTPUT_DIR}/meta"

	# Collect this server's target→path mappings as a JSON object.
	local fragment first=1
	fragment='{'
	for w in ${WRITERS}; do
		name="${ds}_${w}"
		[[ $first -eq 1 ]] || fragment+=','
		fragment+="\"${name}\":\"$(container_path "$id" "$name")\""
		first=0
	done
	# Include the live target so dsosd can serve live-test queries too.
	if [[ -n "${LIVE_WRITER:-}" ]]; then
		name="${ds}_live"
		[[ $first -eq 1 ]] || fragment+=','
		fragment+="\"${name}\":\"$(container_path "$id" "$name")\""
	fi
	fragment+='}'

	# Merge into the existing directory file (or create a new one).
	"${PYTHON}" -I -c "
import json, sys, os
path, sid, new = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
d = {}
if os.path.isfile(path):
    with open(path) as f:
        d = json.load(f)
d[sid] = new
with open(path, 'w') as f:
    json.dump(d, f, indent=2)
    f.write('\n')
" "${DIRFILE}" "$id" "$fragment"
}

dsosd_registered() {
	rpcinfo -p localhost 2>/dev/null | awk '{print $1}' | grep -qx "${SOSDB_PROG}"
}

# dsosd does not remove its rpcbind registration when it is killed, so the
# harness removes it; otherwise the next start finds a stale registration.
deregister() {
	dsosd_registered || return 0
	rpcinfo -d "${SOSDB_PROG}" "${SOSDB_VERS}" 2>/dev/null ||
		sudo -n rpcinfo -d "${SOSDB_PROG}" "${SOSDB_VERS}" 2>/dev/null || true
	if dsosd_registered; then
		log "could not remove the rpcbind registration; run: sudo rpcinfo -d ${SOSDB_PROG} ${SOSDB_VERS}"
		return 1
	fi
	log "removed the rpcbind registration of program ${SOSDB_PROG}"
}

plugin_serve_start() { # SERVER_ID
	local id="$1"
	command -v rpcinfo >/dev/null || die "rpcinfo not found"
	if dsosd_registered; then
		local ours="" others
		[[ -f "${PIDFILE}" ]] && kill -0 "$(cat "${PIDFILE}")" 2>/dev/null && ours="$(cat "${PIDFILE}")"
		if [[ -n "$ours" ]]; then
			log "dsosd started by this harness is already running (pid ${ours})"
			return 0
		fi
		others="$(pgrep -x dsosd | tr '\n' ' ' || true)"
		if [[ -n "$others" ]]; then
			die "a dsosd not started by this harness is running (pid ${others% }). It does not use this harness's directory file, so the checks cannot find the containers. Stop it, then rerun the serve phase."
		fi
		log "removing a stale rpcbind registration (no dsosd process is running)"
		deregister || die "could not remove the stale registration"
	fi
	write_directory "$id"
	mkdir -p "${OUTPUT_DIR}/nodes/${HOST}/run" "${OUTPUT_DIR}/logs"
	log "+ DSOSD_DIRECTORY=$(printf '%q' "${DIRFILE}") DSOSD_SERVER_ID=$(printf '%q' "$id") $(printf '%q ' ${DSOSD})"
	if [[ -n "${DRY_RUN:-}" ]]; then
		log "dry run: dsosd not started; registration not checked"
		return 0
	fi
	log "starting dsosd (server id ${id}, log ${LOGFILE})"
	DSOSD_DIRECTORY="${DIRFILE}" DSOSD_SERVER_ID="$id" \
		nohup ${DSOSD} >"${LOGFILE}" 2>&1 </dev/null &
	echo $! >"${PIDFILE}"
	local i
	for i in $(seq 1 30); do
		dsosd_registered && { log "dsosd is registered"; return 0; }
		sleep 1
	done
	die "dsosd did not register within 30 s; see ${LOGFILE}"
}

plugin_serve_run() { # SERVER_ID -- foreground, used by the Slurm launcher
	local id="$1"
	write_directory "$id"
	mkdir -p "${OUTPUT_DIR}/nodes/${HOST}/run" "${OUTPUT_DIR}/logs"
	log "+ DSOSD_DIRECTORY=$(printf '%q' "${DIRFILE}") DSOSD_SERVER_ID=$(printf '%q' "$id") $(printf '%q ' ${DSOSD})"
	if [[ -n "${DRY_RUN:-}" ]]; then
		log "dry run: not exec'ing dsosd"
		return 0
	fi
	echo $$ >"${PIDFILE}"
	log "running dsosd in the foreground (server id ${id})"
	export DSOSD_DIRECTORY="${DIRFILE}" DSOSD_SERVER_ID="$id"
	exec ${DSOSD}
}

plugin_serve_stop() {
	if [[ -n "${DRY_RUN:-}" ]]; then
		log "+ (dry run) would stop dsosd using ${PIDFILE}"
		return 0
	fi
	if [[ ! -f "${PIDFILE}" ]]; then
		log "no dsosd pid file; nothing to stop"
		return 0
	fi
	local pid i
	pid="$(cat "${PIDFILE}")"
	rm -f "${PIDFILE}"
	if ! kill "$pid" 2>/dev/null; then
		log "dsosd (pid ${pid}) was not running"
	else
		for i in $(seq 1 20); do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.5
		done
		if kill -0 "$pid" 2>/dev/null; then
			kill -KILL "$pid" 2>/dev/null || true
		fi
		log "stopped dsosd (pid ${pid})"
	fi
	if ! pgrep -x dsosd >/dev/null; then
		deregister || true
	fi
}

plugin_serve_status() {
	if dsosd_registered; then
		log "dsosd registered (program ${SOSDB_PROG})"
	else
		log "dsosd NOT registered"
		return 1
	fi
}

plugin_clean() { # SERVER_ID
	local id="$1" running
	# Containers cannot be removed from under a dsosd, and one this harness
	# did not start is not ours to kill: report it and remove nothing.
	if [[ -z "${DRY_RUN:-}" ]]; then
		running="$(pgrep -x dsosd | tr '\n' ' ' || true)"
		[[ -z "$running" ]] ||
			die "dsosd is still running (pid ${running% }) and was not stopped by this harness. Stop it, then rerun clean."
	fi
	# SOS_DATA_ROOT defaults to a directory under nodes/<hostname>/, which
	# the harness removes; this covers a root overridden to live elsewhere.
	run_logged rm -rf "${SOS_DATA_ROOT:?}/${id}"
	if [[ -n "${DRY_RUN:-}" ]]; then
		log "+ (dry run) would remove a stale rpcbind registration, if any"
	else
		deregister || true
	fi
}
