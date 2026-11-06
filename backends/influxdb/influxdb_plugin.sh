# influxdb_plugin.sh -- InfluxDB v3 Core hooks, sourced by bin/node.sh when BACKEND=influxdb.
#
# Hooks (see backends/README.md for the contract):
#   plugin_write       WRITER MODE INDEX ID   write one partition; the plugin
#                                             uses DS.name as the measurement
#                                             name and records it in an
#                                             ingest-handoff meta file at
#                                             OUTPUT_DIR/meta/ingest-handoff-<writer>.json
#   plugin_serve_start ID                     on the designated server host only;
#                                             remove data and token of any
#                                             earlier run, generate a token
#                                             offline, start influxdb3,
#                                             create the database
#   plugin_serve_run   ID                     foreground influxdb3
#                                             (slurm launcher)
#   plugin_serve_stop                         stop influxdb3 (no-op
#                                             where nothing was started)
#   plugin_clean       ID                     remove the data directory and
#                                             token file (no-op on other
#                                             hosts); refuses while an
#                                             influxdb3 is still running
#                                             (nodes/ and meta/ are removed
#                                             by the harness)
#
# No build step (the writer is Python).
# Available from node.sh: HOST, PLUGIN_DIR, log, die, run_logged, ds_name,
# dataset_args (array), and every setting from bench.conf and influxdb_backend.conf.
#
# Set DRY_RUN=1 (bin/run.sh's -d flag) to log the write/serve command this
# host would run (via run_logged() or an inline "+ ..." log line) without
# actually writing, starting/stopping influxdb3, or deleting anything. Under
# DRY_RUN, plugin_write logs with a placeholder token instead of reading one
# from INFLUX_TOKEN_FILE, since a dry-run serve-write never created it. The
# writer reads DATASET from its environment rather than a flag, so it is
# passed explicitly to run_logged (see bin/node.sh's run_logged doc comment)
# -- the logged line is then standalone-runnable, not just a record of what
# ran inside this script.
#
# Single-instance model:
#   InfluxDB 3 Core is single-node; there is no clustering mode. The harness
#   runs exactly one influxdb3 server -- on the designated host extracted from
#   INFLUX_HOST -- and has every other SERVER_HOSTS entry send its writes to
#   that host over HTTP. The serve hooks (start, run, stop) and clean no-op
#   on non-designated hosts; plugin_write runs on every host (it only sends
#   HTTP requests, it does not need a local server).

INFLUX_DIR="${BENCH_DIR}/backends/influxdb"

# PID file under the designated host's nodes/<hostname>/run/.
_INFLUX_PID="${OUTPUT_DIR}/nodes/${HOST}/run/influxdb3.pid"
# Daemon log goes to the shared logs/ directory.
_INFLUX_LOG="${OUTPUT_DIR}/logs/influxdb3-${HOST}.log"

# The designated InfluxDB host: extracted from the INFLUX_HOST URL. Only
# this hostname runs influxdb3; every other SERVER_HOSTS entry is a remote
# writer that sends data over HTTP to this host.
_INFLUX_SERVER_HOST="$(echo "${INFLUX_HOST}" | sed -E 's|^https?://||; s|:[0-9]+$||')"

# True when the current host ($HOST, set by node.sh) is the designated
# InfluxDB server host. Used by the serve and clean hooks to no-op on
# non-designated hosts.
_influx_is_server_host() {
	[[ "${HOST}" == "${_INFLUX_SERVER_HOST}" ]]
}

# ----------------------------------------------------------------- write

plugin_write() { # WRITER MODE INDEX SERVER_ID
	log "token file: ${INFLUX_TOKEN_FILE}"
	local writer="$1" mode="$2" index="$3"
	[[ "$writer" == "influx" ]] ||
		die "the influxdb backend has no writer '$writer' (available: influx)"

	local measurement
	measurement="$(ds_name)"

	# dataset_args carries a generic --batch sized for SOS transactions.
	# InfluxDB's write batch is INFLUX_WRITE_BATCH, so drop --batch and its
	# value and pass the InfluxDB one instead. --rate is kept for the live test.
	local args=() skip=0 a
	for a in "${dataset_args[@]}"; do
		if [[ "$skip" == 1 ]]; then skip=0; continue; fi
		if [[ "$a" == "--batch" ]]; then skip=1; continue; fi
		args+=("$a")
	done

	local token="<token>"
	[[ -n "${DRY_RUN:-}" ]] || token="$(_influx_read_token)"

	log "writing ${measurement} to ${INFLUX_DATABASE} (influx writer, ${INFLUX_INTERFACE} interface) on ${INFLUX_HOST}"
	_influx_write_meta "$writer" "$measurement"
	run_logged DATASET="${DATASET}" -- \
		"${PYTHON}" "${BENCH_DIR}/core/ingest.py" \
		--backend influxdb --adapter influx --writer "$writer" \
		--measurement "$measurement" --node-index "$index" \
		"${args[@]}" --batch "${INFLUX_WRITE_BATCH}" \
		--interface "${INFLUX_INTERFACE}" \
		--host "${INFLUX_HOST}" \
		--database "${INFLUX_DATABASE}" \
		--precision "${INFLUX_PRECISION}" \
		--token "$token"
}

# Write the ingest-handoff meta file read by the query side.
# See EXTENDING.md, "Plugin meta files".
_influx_write_meta() { # WRITER MEASUREMENT
	local writer="$1" measurement="$2"
	local meta_dir="${OUTPUT_DIR}/meta"
	mkdir -p "$meta_dir"
	cat >"${meta_dir}/ingest-handoff-${writer}.json" <<-EOF
	{
	  "target": "${measurement}",
	  "backend": "influxdb",
	  "measurement": "${measurement}",
	  "database": "${INFLUX_DATABASE}",
	  "host": "${INFLUX_HOST}"
	}
	EOF
}

# ----------------------------------------------------------------- helpers

# Extract the token string from the JSON token file written by serve-start.
_influx_read_token() {
	[[ -f "${INFLUX_TOKEN_FILE}" ]] \
		|| die "token file not found: ${INFLUX_TOKEN_FILE}; was serve-write run?"
	"${PYTHON}" -c "import json,sys; print(json.load(open(sys.argv[1]))['token'])" \
		"${INFLUX_TOKEN_FILE}"
}

# Remove the data and token of any earlier run and generate an admin token
# offline. The server must not be running. The CLI prints the token to stdout
# even with --output-file; the redirect sends that to the influxdb3 log.
_influx_fresh_start() {
	if [[ -n "$(ls -A "${INFLUX_DATA_DIR}" 2>/dev/null)" ]]; then
		log "removing data from an earlier run: ${INFLUX_DATA_DIR}"
		rm -rf "${INFLUX_DATA_DIR:?}"
		mkdir -p "${INFLUX_DATA_DIR}"
	fi
	rm -f "${INFLUX_TOKEN_FILE}"
	log "generating admin token"
	influxdb3 create token --admin \
		--offline \
		--name "monbench-admin" \
		--output-file "${INFLUX_TOKEN_FILE}" \
		>>"${_INFLUX_LOG}" 2>&1 \
		|| die "token generation failed; see ${_INFLUX_LOG}"
}

# ----------------------------------------------------------------- serve

plugin_serve_start() { # SERVER_ID
	if ! _influx_is_server_host; then
		log "not the designated InfluxDB host (${_INFLUX_SERVER_HOST}); skipping serve-start"
		return 0
	fi

	mkdir -p "${INFLUX_DATA_DIR}" "${OUTPUT_DIR}/nodes/${HOST}/run" "${OUTPUT_DIR}/logs"

	log "+ influxdb3 serve --node-id ${HOST} --object-store file --data-dir ${INFLUX_DATA_DIR} --admin-token-file ${INFLUX_TOKEN_FILE}"
	if [[ -n "${DRY_RUN:-}" ]]; then
		log "dry run: token generation and influxdb3 startup skipped"
		return 0
	fi

	pkill -f 'influxdb3 serve' 2>/dev/null || true
	sleep 1

	_influx_fresh_start

	log "starting influxdb3 (data: ${INFLUX_DATA_DIR})"
	influxdb3 serve \
		--node-id "${HOST}" \
		--object-store file \
		--data-dir "${INFLUX_DATA_DIR}" \
		--admin-token-file "${INFLUX_TOKEN_FILE}" \
		>>"${_INFLUX_LOG}" 2>&1 &
	echo $! >"${_INFLUX_PID}"

	# Give the process a moment to bind its port; confirm it is still alive.
	sleep 2
	kill -0 "$(cat "${_INFLUX_PID}")" 2>/dev/null \
		|| die "influxdb3 exited immediately; see ${_INFLUX_LOG}"
	log "influxdb3 started"

	local token out
	token="$(_influx_read_token)"
	if out="$(influxdb3 create database "${INFLUX_DATABASE}" \
		--host "${INFLUX_HOST}" --token "$token" 2>&1)"; then
		log "database '${INFLUX_DATABASE}' ready"
	else
		log "WARNING: could not create database '${INFLUX_DATABASE}': ${out}"
	fi
}

plugin_serve_run() { # SERVER_ID  (foreground; used by the slurm launcher)
	if ! _influx_is_server_host; then
		log "not the designated InfluxDB host (${_INFLUX_SERVER_HOST}); skipping serve-run"
		# In the Slurm launcher this step stays alive as a background srun;
		# sleep so the step does not exit (and get cleaned up) immediately.
		# It will be killed by serve-stop's srun pid cleanup.
		if [[ -z "${DRY_RUN:-}" ]]; then
			log "sleeping until stopped (non-server placeholder)"
			sleep infinity || true
		fi
		return 0
	fi

	# The token file must already exist (serve_start ran first in the slurm
	# flow before serve_run is called for background operation).
	mkdir -p "${INFLUX_DATA_DIR}" "${OUTPUT_DIR}/nodes/${HOST}/run" "${OUTPUT_DIR}/logs"
	log "+ influxdb3 serve --node-id ${HOST} --object-store file --data-dir ${INFLUX_DATA_DIR} --admin-token-file ${INFLUX_TOKEN_FILE}"
	if [[ -n "${DRY_RUN:-}" ]]; then
		log "dry run: not exec'ing influxdb3"
		return 0
	fi
	exec influxdb3 serve \
		--node-id "${HOST}" \
		--object-store file \
		--data-dir "${INFLUX_DATA_DIR}" \
		--admin-token-file "${INFLUX_TOKEN_FILE}"
}

plugin_serve_stop() {
	if ! _influx_is_server_host; then
		log "not the designated InfluxDB host (${_INFLUX_SERVER_HOST}); nothing to stop"
		return 0
	fi

	if [[ -n "${DRY_RUN:-}" ]]; then
		log "+ (dry run) would stop influxdb3 using ${_INFLUX_PID}"
		return 0
	fi
	if [[ -f "${_INFLUX_PID}" ]]; then
		local pid
		pid="$(cat "${_INFLUX_PID}")"
		kill "${pid}" 2>/dev/null || true
		# Wait briefly so the data directory is not held open during clean.
		local i
		for i in $(seq 1 10); do
			kill -0 "${pid}" 2>/dev/null || break
			sleep 1
		done
		rm -f "${_INFLUX_PID}"
	fi
	log "influxdb3 stopped"
}

# ----------------------------------------------------------------- clean

plugin_clean() { # SERVER_ID
	if ! _influx_is_server_host; then
		log "not the designated InfluxDB host (${_INFLUX_SERVER_HOST}); nothing to clean"
		return 0
	fi

	# The data cannot be removed from under a running influxdb3, and one this
	# harness did not start is not ours to kill: report it and remove nothing.
	if [[ -z "${DRY_RUN:-}" ]] && pgrep -f 'influxdb3 serve' >/dev/null; then
		die "influxdb3 is still running and was not stopped by this harness. Stop it, then rerun clean."
	fi
	# These default to paths under nodes/<hostname>/, which the harness
	# removes; this covers locations overridden to live elsewhere.
	run_logged rm -rf "${INFLUX_DATA_DIR:?}"
	run_logged rm -f "${INFLUX_TOKEN_FILE:?}"
}
