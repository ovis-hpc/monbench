# sos_plugin.sh -- SOS storage hooks, sourced by bin/node.sh when BACKEND is sos
# or dsos. The dsos plugin sources this file for its write side.
#
# This backend writes one local SOS container per server, using the SOS API.
# It also provides a direct, on-disk query adapter (query/sos_query.py,
# QUERY_ADAPTER=sos) for a single-node/shared-filesystem comparison point
# with no RPC; see backends/dsos for querying across servers through dsosd.
#
# Hooks (see backends/README.md for the contract):
#   plugin_build                          build the C writer
#   plugin_write WRITER MODE INDEX ID     write one partition into a local
#                                         SOS container; the plugin constructs
#                                         the storage name from DS.name and the
#                                         writer name, and records it in a
#                                         ingest-handoff meta file at
#                                         OUTPUT_DIR/meta/ingest-handoff-<writer>.json
#   plugin_clean ID                       delete this server's containers
#                                         (nodes/ and meta/ are removed by
#                                         the harness)
#
# Available from node.sh: HOST, PLUGIN_DIR, log, die, run_logged, ds_name,
# dataset_args (array), and every setting from bench.conf and sos_backend.conf.
#
# Debugging the C writer: set GDB=1 in the environment to launch it under
# gdbserver instead of running it directly (writer=c only; the py writer
# ignores GDB). gdbserver listens on GDBSERVER_PORT (default: 2345 + INDEX,
# so concurrent node indices on the same host don't collide) and waits for
# a debugger to attach before sos_write's main() runs -- the log line it
# prints gives you the exact `target remote HOST:PORT` to use. This blocks
# plugin_write until a debugger attaches and lets the program run to exit,
# so only set GDB=1 for a single, deliberate debugging invocation, never for
# a real benchmark run.
#
# Set DRY_RUN=1 (bin/run.sh's -d flag) to log the build/write/clean command
# this host would run, via run_logged(), without actually running it. The
# writer reads DATASET from its environment rather than a flag, so it is
# passed explicitly to run_logged (see bin/node.sh's run_logged doc comment)
# -- the logged line is then standalone-runnable, not just a record of what
# ran inside this script.

# This file's own directory, which is not PLUGIN_DIR when dsos sources it.
SOS_DIR="${BENCH_DIR}/backends/sos"

container_path() { # SERVER_ID TARGET
	echo "${SOS_DATA_ROOT}/$1/$2"
}

# ----------------------------------------------------------------- build

plugin_build() {
	run_logged make -C "${SOS_DIR}/writer" SOS_PREFIX="${SOS_PREFIX}"
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
	local path template
	path="$(container_path "$id" "$target")"
	# Start from an empty container so that reruns do not add duplicates.
	run_logged rm -rf "$path"
	mkdir -p "$(dirname "$path")" "${OUTPUT_DIR}/meta"
	# The schema template is backend-level metadata (shared across nodes),
	# so it lives in OUTPUT_DIR/meta/.
	template="${OUTPUT_DIR}/meta/sos-schema.json"
	"${PYTHON}" "${SOS_DIR}/make_schema.py" "${DATASET}" "$template"
	local args=(--path "$path" --template "$template" --node-index "$index"
		"${dataset_args[@]}" --storage "${SOS_STORAGE}")
	log "writing ${target} (${writer} writer, SOS ${SOS_STORAGE}) to ${path}"
	# Record where we stored data so the query side can find it.
	_sos_write_meta "$writer" "$target" "$id"
	case "$writer" in
	c)
		[[ -x "${SOS_DIR}/writer/sos_write" ]] ||
			die "C writer not built; run: bin/run.sh build"
		if [[ -n "${GDB:-}" ]]; then
			command -v gdbserver >/dev/null 2>&1 ||
				die "GDB=1 requested but gdbserver is not on PATH"
			local port="${GDBSERVER_PORT:-$((2345 + index))}"
			log "GDB=1: launching sos_write under gdbserver on ${HOST}:${port} -- attach with:" \
				"gdb -ex 'target remote ${HOST}:${port}' ${SOS_DIR}/writer/sos_write"
			run_logged DATASET="${DATASET}" -- \
				gdbserver ":${port}" "${SOS_DIR}/writer/sos_write" "${args[@]}"
		else
			run_logged DATASET="${DATASET}" -- "${SOS_DIR}/writer/sos_write" "${args[@]}"
		fi
		;;
	py)
		run_logged DATASET="${DATASET}" -- "${PYTHON}" "${SOS_DIR}/writer/sos_write.py" "${args[@]}"
		;;
	*)
		die "the sos backend has no writer '$writer' (available: c py)"
		;;
	esac
}

# Write an ingest-handoff meta file recording where the writer stored data, so the
# query side can find it without reconstructing paths by convention.
# See EXTENDING.md, "Plugin meta files".
_sos_write_meta() { # WRITER TARGET SERVER_ID
	local writer="$1" target="$2" id="$3"
	local meta_dir="${OUTPUT_DIR}/meta"
	mkdir -p "$meta_dir"
	cat >"${meta_dir}/ingest-handoff-${writer}.json" <<-EOF
	{
	  "target": "${target}",
	  "backend": "sos",
	  "data_root": "${SOS_DATA_ROOT}",
	  "container_path": "$(container_path "$id" "$target")"
	}
	EOF
}

# ----------------------------------------------------------------- clean

# SOS_DATA_ROOT defaults to a directory under nodes/<hostname>/, which the
# harness removes; this covers a root overridden to live elsewhere.
plugin_clean() { # SERVER_ID
	run_logged rm -rf "${SOS_DATA_ROOT:?}/$1"
}
