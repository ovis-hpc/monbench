#!/usr/bin/env bash
# node.sh -- actions executed on one node. bin/run.sh calls this script
# through ssh, srun, or by printing the command for you to run by hand.
# Backend-specific work is delegated to backends/<BACKEND>/<BACKEND>_plugin.sh.
#
#   node.sh build                                  build the backend's writers
#   node.sh write WRITER full|live INDEX ID        write data-set partition INDEX
#                                                  (server node); the plugin
#                                                  determines the storage name
#   node.sh meta WRITER full|live                  record what was written
#   node.sh write-summary WRITER                   print ingest summary (query node)
#   node.sh serve-start ID | serve-run ID          start the backend service
#   node.sh serve-stop | serve-status              stop / check it
#   node.sh check WRITER [live]                    verify query results (query node)
#   node.sh query WRITER                           time the query set (query node)
#   node.sh clean ID                               delete the backend data this
#                                                  server keeps outside nodes/
#                                                  (services must be stopped)
#   node.sh wipe                                   delete this node's nodes/<host>/
#                                                  and the meta/ directory
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF="${MONBENCH_CONF:-${HERE}/../bench.conf}"
set -a
# shellcheck source=../bench.conf
source "$CONF"
# The backend to use for this call is passed by bin/run.sh as BE: the write
# backend for build/write/clean, the query backend for serve/query. It selects
# the plugin to load. Falls back to WRITE_BACKEND when not set.
BACKEND="${BE:-${BACKEND:-${WRITE_BACKEND}}}"
PLUGIN_DIR="${BENCH_DIR}/backends/${BACKEND}"
[[ -f "${PLUGIN_DIR}/${BACKEND}_plugin.sh" ]] || { echo "error: unknown backend '${BACKEND}' (no ${PLUGIN_DIR}/${BACKEND}_plugin.sh)" >&2; exit 1; }
source "${PLUGIN_DIR}/${BACKEND}_backend.conf"
WRITERS="${WRITERS:-${DEFAULT_WRITERS:-}}"
set +a
# Site profile scripts are written for interactive shells and often reference
# unset variables or run commands that can fail, so they are evaluated without
# this script's strict error settings.
if [[ -n "${ENV_SETUP:-}" ]]; then
	set +eu
	eval "${ENV_SETUP}"
	set -eu
fi

HOST="$(hostname -s)"
die() { echo "[${HOST}] error: $*" >&2; exit 1; }
log() { echo "[${HOST}] $*"; }

# Logs a command's argv, then runs it -- unless DRY_RUN is set (bin/run.sh's
# -d flag forwards it here), in which case it only logs and returns success.
# Plugins route their actual external commands (writer invocations, build,
# clean) through this so a dry run and a real run produce the same "+ ..."
# log line, and only a real run executes it. (A command that must be
# backgrounded or exec'd, such as a service start, can't go through this
# directly -- those log their own "+ ..." line and gate on DRY_RUN
# themselves; see e.g. backends/dsos/dsos_plugin.sh.)
#
# A command that reads a setting from its own environment rather than from
# an argv flag -- every writer does this for DATASET -- needs that setting
# named explicitly, or the "+ ..." line can't be copied out and run
# standalone (it would already be set in node.sh's own shell, which is not
# where you'd paste the line). Give those as leading "VAR=val" arguments
# followed by "--", and run_logged both logs and (for a real run) exports
# exactly those vars for CMD: "run_logged DATASET=\"$DATASET\" -- \"$PYTHON\"
# writer.py ...". With no "--" anywhere in the arguments, every argument is
# CMD/ARGS as before, so existing calls are unaffected.
run_logged() { # [VAR=val ...] -- CMD ARGS...
	local envs=()
	while [[ $# -gt 0 && "$1" != "--" ]]; do
		envs+=("$1")
		shift
	done
	if [[ "${1:-}" == "--" ]]; then
		shift
	else
		# No "--" found: there were no env vars, so what was collected into
		# envs[] above is actually CMD/ARGS -- restore it.
		set -- "${envs[@]}"
		envs=()
	fi
	local assigns=""
	[[ ${#envs[@]} -eq 0 ]] || assigns="$(printf '%q ' "${envs[@]}")"
	log "+ ${assigns}$(printf '%q ' "$@")"
	[[ -n "${DRY_RUN:-}" ]] && return 0
	if [[ ${#envs[@]} -gt 0 ]]; then
		env "${envs[@]}" "$@"
	else
		"$@"
	fi
}

# Read the dataset definition's "name" field. Used by plugins to construct
# storage names (e.g. container paths, measurement names).
ds_name() {
	"${PYTHON}" -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "${DATASET}"
}

# shellcheck source=../backends/sos/sos_plugin.sh
source "${PLUGIN_DIR}/${BACKEND}_plugin.sh"

# Run a plugin hook if the backend defines it.
hook() { # NAME ARGS...
	local fn="plugin_$1"
	shift
	if declare -F "$fn" >/dev/null; then
		"$fn" "$@"
	else
		log "backend ${BACKEND} has no '${fn#plugin_}' step; skipped"
	fi
}

cmd_write() { # WRITER MODE INDEX ID
	local writer="$1" mode="$2" out
	local samples="${SAMPLES:-}" batch="${BATCH:-}" rate=0
	if [[ "$mode" == "live" ]]; then
		samples="${LIVE_SAMPLES:-}" batch="${LIVE_BATCH:-}" rate="${LIVE_RATE:-}"
	fi
	# The plugin constructs the storage name and records it in a meta file.
	# The ingest engine (core/ingest.py, or the C engine linked into a C
	# writer) times the write and saves the raw result to --output under
	# OUTPUT_DIR/results, keyed by the writer name and host; the plugin_write
	# hook is responsible for naming the storage target.
	mkdir -p "${OUTPUT_DIR}/results"
	out="${OUTPUT_DIR}/results/ingest-${writer}-${HOST}-raw.json"
	rm -f "$out"
	# Data-set arguments every writer accepts (see backends/README.md).
	dataset_args=(--components "${COMPONENT_IDS_PER_SERVER:-}" --samples "$samples"
		--t0 "${T0:-}" --interval "${INTERVAL:-}" --job-len "${JOB_LEN:-}"
		--batch "$batch" --rate "$rate" --output "$out")
	hook write "$@"
}

cmd_egress() { # MODE WRITER [live]
	local args=(--writer "$2" --mode "$1")
	[[ "${3:-}" == "live" ]] && args+=(--live)
	"${PYTHON:-}" "${BENCH_DIR}/core/egress.py" "${args[@]}"
}

# Delete this node's scratch tree and the shared metadata directory. Not
# backend-specific: everything a plugin puts under nodes/<hostname>/ or meta/
# goes with them. nodes/ itself goes too once no other node's tree is left in
# it (on a shared filesystem the other nodes remove theirs).
cmd_wipe() {
	run_logged rm -rf "${OUTPUT_DIR:?}/nodes/${HOST}" "${OUTPUT_DIR:?}/meta"
	[[ -n "${DRY_RUN:-}" ]] || rmdir "${OUTPUT_DIR:?}/nodes" 2>/dev/null || true
	[[ -n "${DRY_RUN:-}" ]] || log "removed ${OUTPUT_DIR}/nodes/${HOST} and ${OUTPUT_DIR}/meta"
}

action="${1:-}"
shift || true
case "$action" in
build) hook build ;;
write) cmd_write "$@" ;;
meta) "${PYTHON:-}" "${BENCH_DIR}/core/dataset.py" meta "$@" ;;
write-summary) "${PYTHON:-}" "${BENCH_DIR}/core/ingest.py" write-summary --writer "$1" ;;
serve-start) hook serve_start "$@" ;;
serve-run) hook serve_run "$@" ;;
serve-stop) hook serve_stop ;;
serve-status) hook serve_status ;;
check) cmd_egress check "$@" ;;
query) cmd_egress query "$@" ;;
clean) hook clean "$@" ;;
wipe) cmd_wipe ;;
*) sed -n '2,/^set -euo pipefail$/p' "$0" | sed '$d'; exit 2 ;;
esac
