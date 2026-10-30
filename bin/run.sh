#!/usr/bin/env bash
# run.sh -- drive the benchmark from one place, for the write/query backend
# pair(s) named in bench.conf (plugins under backends/).
#
# usage: bin/run.sh [-c CONF] [-l ssh|slurm|manual] [-d] PHASE [PHASE...]
#
# Phases (run in the order given):
#   deploy       copy this directory to every node (ssh only; skipped where the
#                directory is already shared) -- runs once, before any pair
#   build        build the backend's writers on each server node
#   serve-write  start the write backend's service on the servers, if it has one
#   write        write the data set on the servers, once per writer in WRITERS;
#                each plugin determines its own storage name and records it in
#                a metadata file under OUTPUT_DIR/<pair>/meta/
#   serve-query  start the query backend's service on the servers, if it has one
#                (skipped when it is the same service as serve-write)
#   check        verify query results from the query node, once per writer
#                (no timing); reads the writer's meta file for the storage name
#   query        time the query set from the query node, once per writer
#                (no verification)
#   live         write a new target slowly while the query node polls it
#   stop         stop both backend services
#   clean        stop the services and delete the servers' data and run files
#   all          build serve-write write serve-query check query stop
#   all-nocheck  build serve-write write serve-query query stop
#
# Failed steps and what runs after them:
#   check and query are NOT run for a writer whose write step failed (the data
#   they would read is missing or incomplete), and not at all when the service
#   they need (serve-write / serve-query) failed to start. Each skipped step is
#   reported, and the pair counts as FAILED. This only applies to steps that
#   ran earlier in the same invocation: "run.sh check" on its own always runs.
#   Other phases (stop, clean, ...) always run, so services are not left up.
#
# Configuration errors end the whole suite:
#   Before anything runs (including deploy), every pair in BACKENDS is set up
#   once and validated: unknown backend, a query backend that cannot read what
#   the write backend writes, no writers, missing SERVER_HOSTS/QUERY_HOST. Any
#   error stops run.sh with nothing started.
#
# Launchers:
#   ssh      run each step over ssh (default). The command is run as
#            "bash -l -c '<cmd>'", i.e. in a login shell, so the profile files
#            of the server host (/etc/profile, ~/.bash_profile or ~/.profile)
#            set up PATH/PYTHONPATH/LD_LIBRARY_PATH as they would for a login.
#            Set LOGIN_SHELL=0 in bench.conf to run the bare command instead.
#            (A login shell is not the same as an interactive one: ~/.bashrc
#            is only read if a profile file sources it, and many ~/.bashrc
#            files return early when the shell is not interactive.)
#   slurm    run each step with srun inside an allocation (see slurm/); the
#            steps inherit the environment of the submitting shell. A
#            SERVER_HOSTS / QUERY_HOST left empty is filled from the
#            allocation: the last node is the query host, the others servers
#            (at least 2 nodes). The query host may not also be a server.
#   manual   print each step for you to run on the named node, then wait
#
# Dry run:
#   -d   log the exact command each phase would run on each server/query
#        host, without actually running it: build, write, serve-start/run/
#        stop and clean log the command they would run instead of running
#        it (nothing is built, written, started, stopped, or deleted); check
#        and query resolve and print every query's final text without ever
#        connecting to the backend or sending it (core/bench.py, status
#        DRY in its report). Forwarded to every node as DRY_RUN=1, so
#        ssh/slurm steps honor it too; with -l manual you already see the
#        command without -d, so it has no extra effect there besides also
#        appearing in the printed env assignments.
#
# Reproducing a step by hand: every launcher now logs the exact "env
# VAR=val ... bash bin/node.sh ACTION ARGS..." line a step runs, to that
# step's log file under OUTPUT_DIR/logs (see remote() below) -- self-
# contained, so it can be copied onto the named host and run as-is, with
# or without -d. (Under the ssh launcher the line actually sent is that
# line wrapped in "bash -l -c"; the logged line is the inner command, to
# paste into a shell that already has your login environment.) With -d (or
# -l manual), that line also prints to stdout, since it IS the result
# there; on a real ssh/slurm run it only goes to the log file, so stdout
# stays readable -- check the log file for it.
# bin/node.sh's run_logged() goes further for the actual external command a
# hook runs (the writer invocation, a build, a clean): see its doc comment
# in bin/node.sh for how a command that reads a setting from its own
# environment (e.g. every writer's DATASET) gets that logged too, rather
# than only being visible because it was already set in node.sh's shell.
#
# Backend pairs:
#   bench.conf names either one pair (WRITE_BACKEND/QUERY_BACKEND) or several,
#   via BACKENDS -- a space-separated list of "writer" or "writer:query"
#   tokens (query defaults to writer), e.g.:
#     BACKENDS="sos:dsos influxdb"
#   All pairs share this file's SERVER_HOSTS, QUERY_HOST, DATASET, QUERY_FILE,
#   and every other setting; only WRITE_BACKEND/QUERY_BACKEND change per
#   pair. Every phase given on the command line runs for each pair in turn,
#   in the order listed, before moving to the next pair (deploy excepted --
#   it runs once, since it does not depend on the backend).
#
#   Each pair runs in its own subshell: the settings a backend's
#   <name>_backend.conf defines (WRITERS from DEFAULT_WRITERS, LIVE_WRITER,
#   data roots, ...) belong to that pair and never leak into the next one.
#   A pair that fails does not stop the following pairs.
#
#   OUTPUT_DIR is namespaced per pair, named after it (e.g. "sos-dsos",
#   "influxdb"). That name is fixed once per pair (see setup_pair below) and
#   reused by every phase in it.
#
#   OUTPUT_DIR/<pair>/ layout (see EXTENDING.md for the full spec):
#     meta/                backend-level metadata (shared across nodes)
#       storage-<w>.json     where writer <w> stored data (plugin→query handoff)
#       dataset-<w>.json     dataset params for writer <w> (core-written)
#     nodes/<hostname>/    per-server scratch (wiped on clean)
#       data/                backend storage (containers, databases)
#       run/                 daemon runtime (PID files, tokens)
#       tmp/                 transient node-local files
#     results/             benchmark statistics (user-facing output)
#     logs/                all logs (step, daemon, benchmark)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF="$(cd "${HERE}/.." && pwd)/bench.conf"
LAUNCHER="ssh"

# The header comment above is the usage text: print it up to "set -euo".
usage() { sed -n '2,/^set -euo pipefail$/p' "$0" | sed '$d'; }

while getopts "c:l:dh" opt; do
	case "$opt" in
	c) CONF="$(cd "$(dirname "$OPTARG")" && pwd)/$(basename "$OPTARG")" ;;
	l) LAUNCHER="$OPTARG" ;;
	d) DRY_RUN=1 ;;
	*) usage; exit 2 ;;
	esac
done
shift $((OPTIND - 1))
[[ $# -gt 0 ]] || { usage; exit 2; }
case "$LAUNCHER" in ssh | slurm | manual) ;; *) echo "unknown launcher '$LAUNCHER'" >&2; exit 2 ;; esac

# shellcheck source=../bench.conf
source "$CONF"

# In the Slurm launcher, a host setting still empty after bench.conf and the
# environment have been applied is filled from the allocation: the query host
# is the last node, the servers are all the others. When only one of the two
# is given, the other is taken from the remaining nodes.
if [[ "$LAUNCHER" == "slurm" ]]; then
	[[ -n "${SLURM_JOB_ID:-}" ]] || { echo "the slurm launcher must run inside an allocation (salloc/sbatch)" >&2; exit 1; }
	if [[ -z "${SERVER_HOSTS:-}" || -z "${QUERY_HOST:-}" ]]; then
		mapfile -t ALLOC < <(scontrol show hostnames "${SLURM_JOB_NODELIST}")
		if [[ -z "${SERVER_HOSTS:-}" && -z "${QUERY_HOST:-}" ]]; then
			[[ ${#ALLOC[@]} -ge 2 ]] || { echo "need at least 2 nodes in the allocation (servers plus a separate query host), have ${#ALLOC[@]}" >&2; exit 1; }
			QUERY_HOST="${ALLOC[${#ALLOC[@]}-1]}"
			SERVER_HOSTS="${ALLOC[*]:0:${#ALLOC[@]}-1}"
		elif [[ -z "${QUERY_HOST:-}" ]]; then
			for ((_i = ${#ALLOC[@]} - 1; _i >= 0; _i--)); do
				[[ " ${SERVER_HOSTS} " == *" ${ALLOC[_i]} "* ]] && continue
				QUERY_HOST="${ALLOC[_i]}"
				break
			done
			[[ -n "${QUERY_HOST:-}" ]] || { echo "every node of the allocation is in SERVER_HOSTS; the query host needs a node of its own" >&2; exit 1; }
		else
			SERVER_HOSTS=""
			for _n in "${ALLOC[@]}"; do
				[[ "$_n" == "${QUERY_HOST}" ]] || SERVER_HOSTS+="${SERVER_HOSTS:+ }${_n}"
			done
			[[ -n "${SERVER_HOSTS}" ]] || { echo "the allocation has no node besides QUERY_HOST '${QUERY_HOST}' to act as a server" >&2; exit 1; }
		fi
	fi
fi

# Run ssh steps in a login shell (see "Launchers" above). 1 = yes (default).
LOGIN_SHELL="${LOGIN_SHELL:-1}"

# ------------------------------------------------------------ backend pairs
# BACKENDS, if set, lists every write/query pair to run this invocation, as
# space-separated "writer" or "writer:query" tokens (query defaults to
# writer). Otherwise there is exactly one pair, from WRITE_BACKEND/
# QUERY_BACKEND, so a single-pair bench.conf works unchanged.
PAIRS=()
if [[ -n "${BACKENDS:-}" ]]; then
	read -r -a _tokens <<<"${BACKENDS}"
	for _tok in "${_tokens[@]}"; do
		_w="${_tok%%:*}"
		if [[ "$_tok" == *:* ]]; then _q="${_tok#*:}"; else _q="$_w"; fi
		PAIRS+=("${_w}:${_q}")
	done
else
	PAIRS=("${WRITE_BACKEND:-}:${QUERY_BACKEND:-}")
fi
[[ ${#PAIRS[@]} -gt 0 ]] || { echo "BACKENDS is set but names no pairs (see bench.conf.example)" >&2; exit 1; }
for _chk in "${PAIRS[@]}"; do
	[[ -n "${_chk%%:*}" && -n "${_chk#*:}" ]] ||
		{ echo "no backend configured: set BACKENDS, or WRITE_BACKEND/QUERY_BACKEND, in bench.conf (see bench.conf.example)" >&2; exit 1; }
done

# OUTPUT_DIR is namespaced per pair below (see setup_pair).
BASE_OUTPUT_DIR="${OUTPUT_DIR}"

[[ -n "${SERVER_HOSTS:-}" ]] || { echo "SERVER_HOSTS must be set in bench.conf (see bench.conf.example)" >&2; exit 1; }
[[ -n "${QUERY_HOST:-}" ]] || { echo "QUERY_HOST must be set in bench.conf (see bench.conf.example)" >&2; exit 1; }
read -r -a SERVERS <<<"${SERVER_HOSTS}"
[[ ${#SERVERS[@]} -ge 1 ]] || { echo "SERVER_HOSTS must list at least one host" >&2; exit 1; }
for _h in "${SERVERS[@]}"; do
	[[ "$_h" != "${QUERY_HOST}" ]] ||
		{ echo "QUERY_HOST '${QUERY_HOST}' is also in SERVER_HOSTS; the query host must be a separate node" >&2; exit 1; }
done

check_plugin() { # NAME
	[[ -f "${BENCH_DIR}/backends/$1/${1}_backend.conf" && -f "${BENCH_DIR}/backends/$1/${1}_plugin.sh" ]] ||
		{ echo "unknown backend '$1': no plugin in ${BENCH_DIR}/backends/$1" >&2; exit 1; }
}

STATE_DIR="${TMPDIR:-/tmp}/monbench-driver-${USER:-$(id -un)}"
mkdir -p "$STATE_DIR"
OVERALL_FAILED=0

say() { printf '\n== %s\n' "$*"; }

node_cmd() { # ACTION ARGS... -> command line to run on a node
	local assigns v
	assigns="env MONBENCH_CONF=$(printf '%q' "$CONF") "
	# In manual mode the command is typed by hand, so only the configuration
	# file is named; edit bench.conf rather than overriding from the shell.
	if [[ "$LAUNCHER" != "manual" ]]; then
		for v in "${CONFIG_VARS[@]}"; do
			# Skip variables that are unset; they inherit from bench.conf on the node.
			[[ -v "$v" ]] && assigns+="${v}=$(printf '%q' "${!v}") "
		done
	fi
	printf 'cd %q && %sbash bin/node.sh' "$BENCH_DIR" "$assigns"
	printf ' %q' "$@"
}

# What is handed to ssh for a node_cmd() line: wrapped in a login shell, so
# the server host's profile (PATH, PYTHONPATH, ...) is loaded first. The
# "env VAR=val ..." assignments inside the command are applied after the
# profile ran, so settings forwarded from bench.conf still win over it.
# The command is quoted for the remote side, whatever the remote login
# shell is.
ssh_wrap() { # CMD
	if [[ "${LOGIN_SHELL}" == "1" ]]; then
		printf 'bash -l -c %q' "$1"
	else
		printf '%s' "$1"
	fi
}

wait_for_user() {
	# Without a terminal (e.g. output redirected in a batch job) do not wait.
	{ read -r -p "   Press Enter when the step above has finished... " _ </dev/tty; } 2>/dev/null || true
}

# Run one node action and wait for it. The node_cmd() line itself -- the
# full "env VAR=val ... bash bin/node.sh ACTION ARGS" invocation, self-
# contained with every setting the step needs -- is always written to the
# step's log file under OUTPUT_DIR/logs, so it can be copied onto the host
# and rerun by hand (e.g. under gdb) exactly as the harness ran it. It also
# prints to stdout under DRY_RUN (there is nothing else to show, and it IS
# the result), and always for -l manual (showing it is manual mode's whole
# job); on a real ssh/slurm run it stays out of stdout so the step's own
# output (the part you're actually watching) isn't buried under it -- check
# the log file if you need the exact command afterward.
remote() { # HOST LOGNAME ACTION ARGS...
	local host="$1" logname="$2"
	shift 2
	local cmd
	cmd="$(node_cmd "$@")"
	case "$LAUNCHER" in
	ssh)
		if [[ -n "${DRY_RUN:-}" ]]; then
			printf '   on %s:\n     %s\n' "$host" "$cmd" | tee "${LOG_DIR}/${logname}.log"
		else
			printf '   on %s:\n     %s\n' "$host" "$cmd" >"${LOG_DIR}/${logname}.log"
		fi
		ssh -n ${SSH_OPTS} "$host" "$(ssh_wrap "$cmd")" 2>&1 | tee -a "${LOG_DIR}/${logname}.log"
		return "${PIPESTATUS[0]}"
		;;
	slurm)
		if [[ -n "${DRY_RUN:-}" ]]; then
			printf '   on %s:\n     %s\n' "$host" "$cmd" | tee "${LOG_DIR}/${logname}.log"
		else
			printf '   on %s:\n     %s\n' "$host" "$cmd" >"${LOG_DIR}/${logname}.log"
		fi
		srun ${SRUN_OPTS} -N1 -n1 -w "$host" bash -c "$cmd" 2>&1 | tee -a "${LOG_DIR}/${logname}.log"
		return "${PIPESTATUS[0]}"
		;;
	manual)
		printf '   On %s run:\n     %s\n' "$host" "$cmd"
		wait_for_user
		;;
	esac
}

# Run the same action on every server at once and wait for all of them. The
# server's index and name are appended to the action's arguments.
on_servers() { # LOGPREFIX ACTION ARGS...
	local prefix="$1"
	shift
	local pids=() i rc=0
	if [[ "$LAUNCHER" == "manual" ]]; then
		echo "   Run these on the servers at the same time:"
		for i in "${!SERVERS[@]}"; do
			printf '   On %s:\n     %s\n' "${SERVERS[$i]}" "$(node_cmd "$@" "$i" "${SERVERS[$i]}")"
		done
		wait_for_user
		return 0
	fi
	for i in "${!SERVERS[@]}"; do
		remote "${SERVERS[$i]}" "${prefix}-${SERVERS[$i]}" "$@" "$i" "${SERVERS[$i]}" &
		pids+=($!)
	done
	for i in "${pids[@]}"; do
		wait "$i" || rc=1
	done
	return $rc
}

# ------------------------------------------------- failure bookkeeping
# Per pair (set up in run_pair). BLOCKED names the first service phase
# (serve-write / serve-query) that failed; WRITE_FAILED has an entry for each
# writer whose write step failed; SKIPPED lists the check/query steps that
# were not run because of either.
block() { # PHASE
	FAILED=1
	[[ -n "$BLOCKED" ]] || BLOCKED="$1"
}

# Prints why WRITER's check/query must not run, and returns 0; returns 1 when
# nothing upstream failed.
skip_reason() { # WRITER
	if [[ -n "$BLOCKED" ]]; then
		echo "the ${BLOCKED} phase failed"
		return 0
	fi
	if [[ -n "${WRITE_FAILED[$1]:-}" ]]; then
		echo "the write phase failed for writer '$1'"
		return 0
	fi
	return 1
}

skip_step() { # STEP WRITER REASON
	echo "   not run: $3"
	SKIPPED+="${SKIPPED:+, }$1 $2"
}

# ----------------------------------------------------------------- phases
# Backend-independent except for reading the current WRITE_BACKEND/
# QUERY_BACKEND/PAIR_NAME globals, which setup_pair sets before these run.

phase_deploy() {
	[[ "$LAUNCHER" == "ssh" ]] || { echo "   deploy is only needed with the ssh launcher"; return 0; }
	if [[ -n "${DRY_RUN:-}" ]]; then
		echo "   dry run: would copy ${BENCH_DIR} to ${SERVERS[*]} ${QUERY_HOST} (skipped)"
		return 0
	fi
	local marker host id
	marker="${BENCH_DIR}/.monbench-id"
	[[ -f "$marker" ]] || od -An -N8 -tx8 /dev/urandom | tr -d ' ' >"$marker"
	id="$(cat "$marker")"
	for host in "${SERVERS[@]}" "${QUERY_HOST}"; do
		if [[ "$(ssh -n ${SSH_OPTS} "$host" cat "$(printf '%q' "$marker")" 2>/dev/null || true)" == "$id" ]]; then
			echo "   ${host}: directory is shared, nothing to copy"
			continue
		fi
		echo "   ${host}: copying ${BENCH_DIR}"
		ssh -n ${SSH_OPTS} "$host" mkdir -p "$(printf '%q' "$BENCH_DIR")"
		if command -v rsync >/dev/null; then
			rsync -a --exclude results --exclude .monbench-id -e "ssh ${SSH_OPTS}" "${BENCH_DIR}/" "${host}:${BENCH_DIR}/"
		else
			scp -rq ${SSH_OPTS} "${BENCH_DIR}/." "${host}:${BENCH_DIR}/"
			# scp has no --exclude; remove the marker after copying so the
			# shared-filesystem check keeps working on the next deploy.
			ssh -n ${SSH_OPTS} "$host" rm -f "$(printf '%q' "$marker")"
		fi
	done
}

phase_build() {
	local host
	# Sequential, so that builds in a shared directory do not collide. A
	# failed build is not a blocker by itself: the writer that needed it
	# fails in the write phase, and only that writer's check/query is skipped.
	for host in "${SERVERS[@]}"; do
		BE="${WRITE_BACKEND}" remote "$host" "build-${host}" build || FAILED=1
	done
}

phase_write() {
	local w ok
	for w in ${WRITERS}; do
		say "write: ${w} writer (${WRITE_BACKEND}) on ${SERVERS[*]}"
		ok=1
		if BE="${WRITE_BACKEND}" on_servers "write-${w}" write "$w" full; then
			# Record what was written; the plugin wrote storage details to
			# OUTPUT_DIR/meta/storage-<writer>.json on each server, and this
			# meta step on the query node writes the dataset/params metadata
			# that the query engine reads. Without it the query side cannot
			# find the data, so a failed meta step counts as a failed write.
			BE="${WRITE_BACKEND}" remote "${QUERY_HOST}" "meta-${w}" meta "$w" full || ok=0
			# Print the ingest performance summary.
			remote "${QUERY_HOST}" "write-summary-${w}" write-summary "$w" || FAILED=1
		else
			ok=0
		fi
		if [[ $ok -eq 0 ]]; then
			FAILED=1
			WRITE_FAILED[$w]=1
		fi
	done
}

# Start one backend's service on all server nodes, identified by BACKEND_NAME
# and its SERVICE flag. The log/pid prefix distinguishes write from query.
_phase_serve_backend() { # BACKEND_NAME SERVICE_FLAG LOG_PREFIX
	local backend="$1" service="$2" prefix="$3"
	local host i
	if [[ "${service}" != "1" ]]; then
		echo "   ${backend} backend runs no service on the servers; nothing to start"
		return 0
	fi
	case "$LAUNCHER" in
	ssh | manual)
		for host in "${SERVERS[@]}"; do
			BE="${backend}" remote "$host" "${prefix}-${host}" serve-start "$host" || block "$prefix"
		done
		;;
	slurm)
		# A background srun step keeps the service alive; its pid is kept here
		# so that 'stop' can end the step. Namespaced by pair so two pairs
		# using the same host/prefix in one invocation don't clash. The
		# node_cmd() line is logged first (to the log file only, same as
		# remote() does for a real ssh/slurm run), so this backgrounded
		# invocation is reproducible by hand too, not just the foreground ones.
		local _cmd
		for host in "${SERVERS[@]}"; do
			_cmd="$(BE="${backend}" node_cmd serve-run "$host")"
			printf '   on %s:\n     %s\n' "$host" "$_cmd" >"${LOG_DIR}/${prefix}-${host}.log"
			srun ${SRUN_OPTS} -N1 -n1 -w "$host" bash -c "$_cmd" \
				>>"${LOG_DIR}/${prefix}-${host}.log" 2>&1 &
			echo $! >"${STATE_DIR}/srun-${PAIR_NAME}-${prefix}-${host}.pid"
		done
		for host in "${SERVERS[@]}"; do
			for i in $(seq 1 30); do
				BE="${backend}" remote "$host" "${prefix}-status-${host}" serve-status >/dev/null 2>&1 && break
				sleep 2
			done
			BE="${backend}" remote "$host" "${prefix}-status-${host}" serve-status || block "$prefix"
		done
		;;
	esac
}

# Stop one backend's service on all server nodes.
_phase_stop_backend() { # BACKEND_NAME SERVICE_FLAG LOG_PREFIX
	local backend="$1" service="$2" prefix="$3"
	local host pidfile
	[[ "${service}" == "1" ]] || return 0
	for host in "${SERVERS[@]}"; do
		pidfile="${STATE_DIR}/srun-${PAIR_NAME}-${prefix}-${host}.pid"
		if [[ "$LAUNCHER" == "slurm" && -f "$pidfile" ]]; then
			kill "$(cat "$pidfile")" 2>/dev/null || true
			rm -f "$pidfile"
		fi
		BE="${backend}" remote "$host" "stop-${prefix}-${host}" serve-stop || true
	done
}

phase_write_serve() {
	say "serve-write: starting ${WRITE_BACKEND} service on ${SERVERS[*]}"
	_phase_serve_backend "${WRITE_BACKEND}" "${W_SERVICE}" "serve-write"
}

phase_query_serve() {
	# When write and query backend are the same, their service was already
	# started by serve-write; running serve-start again would be a no-op at
	# best, so skip it.
	if [[ "${WRITE_BACKEND}" == "${QUERY_BACKEND}" ]]; then
		echo "   write and query backend are both '${QUERY_BACKEND}'; service already started by serve-write"
		return 0
	fi
	_phase_serve_backend "${QUERY_BACKEND}" "${Q_SERVICE}" "serve-query"
}

phase_check() {
	local w reason
	for w in ${WRITERS}; do
		say "check: ${w} writer from ${QUERY_HOST}"
		if reason="$(skip_reason "$w")"; then
			skip_step check "$w" "$reason"
			continue
		fi
		BE="${QUERY_BACKEND}" remote "${QUERY_HOST}" "check-${w}" check "$w" || FAILED=1
	done
}

phase_query() {
	local w reason
	for w in ${WRITERS}; do
		say "query: ${w} writer from ${QUERY_HOST}"
		if reason="$(skip_reason "$w")"; then
			skip_step query "$w" "$reason"
			continue
		fi
		BE="${QUERY_BACKEND}" remote "${QUERY_HOST}" "query-${w}" query "$w" || FAILED=1
	done
}

phase_live() {
	say "live: ${LIVE_WRITER} writer at ${LIVE_RATE} records/s per server while ${QUERY_HOST} polls"
	if [[ "$LAUNCHER" == "manual" ]]; then
		on_servers "write-live" write "${LIVE_WRITER}" live
		BE="${QUERY_BACKEND}" remote "${QUERY_HOST}" "check-live" check "${LIVE_WRITER}" live
		return 0
	fi
	BE="${WRITE_BACKEND}" on_servers "write-live" write "${LIVE_WRITER}" live &
	local writers_pid=$!
	sleep 3
	BE="${QUERY_BACKEND}" remote "${QUERY_HOST}" "check-live" check "${LIVE_WRITER}" live || FAILED=1
	wait "$writers_pid" || FAILED=1
}

phase_stop() {
	# When both backends are the same, their service was started by
	# serve-write (prefix "serve-write"), and serve-query was skipped. Stop
	# with the prefix the service was actually started under so that the
	# Slurm launcher finds the right srun pid file.
	if [[ "${WRITE_BACKEND}" == "${QUERY_BACKEND}" ]]; then
		_phase_stop_backend "${WRITE_BACKEND}" "${W_SERVICE}" "serve-write"
	else
		# Stop the query backend's service first (it may depend on the write
		# backend's data), then the write backend's.
		_phase_stop_backend "${QUERY_BACKEND}" "${Q_SERVICE}" "serve-query"
		_phase_stop_backend "${WRITE_BACKEND}" "${W_SERVICE}" "serve-write"
	fi
}

phase_clean() {
	phase_stop
	local host
	for host in "${SERVERS[@]}"; do
		BE="${WRITE_BACKEND}" remote "$host" "clean-${host}" clean "$host" || true
	done
}

# ------------------------------------------------------- pair set-up
# Resolve and validate everything about one pair, setting the per-pair
# globals (PAIR_NAME, OUTPUT_DIR, LOG_DIR, WRITERS, W_SERVICE, Q_SERVICE,
# CONFIG_VARS, ...). Touches nothing on disk, and exits non-zero on any
# configuration error, so it can run for every pair up front (see
# "pre-flight" below) as well as at the start of each pair's run. Always call
# it in a subshell: it sources backend confs, which would otherwise leak into
# the next pair.
setup_pair() { # WRITE_BACKEND QUERY_BACKEND
	WRITE_BACKEND="$1" QUERY_BACKEND="$2"
	PAIR_NAME="${WRITE_BACKEND}"
	[[ "${WRITE_BACKEND}" != "${QUERY_BACKEND}" ]] && PAIR_NAME="${WRITE_BACKEND}-${QUERY_BACKEND}"
	# OUTPUT_DIR: namespaced per pair, fixed once here for the whole pair's
	# run and reused by every phase (build, write, check, query, ...)
	# regardless of which single backend -- write or query -- is actually
	# doing the work for a given phase. That is what keeps a pair's write
	# and query sides agreeing on where the data is: both read the one
	# fixed PAIR_NAME-derived OUTPUT_DIR, rather than a path keyed by
	# whichever single backend happens to be running that particular phase.
	OUTPUT_DIR="${BASE_OUTPUT_DIR}/${PAIR_NAME}"
	LOG_DIR="${OUTPUT_DIR}/logs"

	check_plugin "${WRITE_BACKEND}"
	check_plugin "${QUERY_BACKEND}"

	# The write backend supplies the writers and their default settings.
	# shellcheck source=../backends/sos/sos_backend.conf
	source "${BENCH_DIR}/backends/${WRITE_BACKEND}/${WRITE_BACKEND}_backend.conf"
	WRITERS="${WRITERS:-${DEFAULT_WRITERS:-}}"
	LIVE_WRITER="${LIVE_WRITER:-${WRITERS%% *}}"
	# Does the write backend require a service on the servers?
	W_SERVICE=0
	{
		PLUGIN_SERVICE=0
		source "${BENCH_DIR}/backends/${WRITE_BACKEND}/${WRITE_BACKEND}_backend.conf"
		W_SERVICE="${PLUGIN_SERVICE:-0}"
	} 2>/dev/null

	# Does the query backend require a service, and what data can it read?
	Q_SERVICE=0
	Q_READS=""
	{
		PLUGIN_SERVICE=0 PLUGIN_READS=""
		source "${BENCH_DIR}/backends/${QUERY_BACKEND}/${QUERY_BACKEND}_backend.conf"
		Q_SERVICE="${PLUGIN_SERVICE:-0}"
		Q_READS="${PLUGIN_READS:-}"
	} 2>/dev/null

	# Re-source the write backend so its settings (not the query backend's)
	# are the ones forwarded to the write nodes.
	source "${BENCH_DIR}/backends/${WRITE_BACKEND}/${WRITE_BACKEND}_backend.conf"

	# The query backend must be able to read what the write backend produced.
	if [[ -n "${Q_READS}" && " ${Q_READS} " != *" ${WRITE_BACKEND} "* ]]; then
		echo "backend '${QUERY_BACKEND}' cannot query data written by '${WRITE_BACKEND}' (it reads: ${Q_READS})" >&2
		exit 1
	fi

	[[ -n "${WRITERS}" ]] || { echo "no WRITERS set and backend ${WRITE_BACKEND} has no DEFAULT_WRITERS" >&2; exit 1; }

	# Settings forwarded to every remote step so that environment overrides
	# given to run.sh apply on all nodes, not only here.
	CONFIG_VARS=(BE WRITE_BACKEND QUERY_BACKEND SERVER_HOSTS QUERY_HOST BENCH_DIR OUTPUT_DIR
		ENV_SETUP PYTHON DATASET WRITERS COMPONENT_IDS_PER_SERVER SAMPLES T0
		INTERVAL JOB_LEN BATCH QUERY_ADAPTER QUERY_FILE REPEAT LIMIT_N
		LIVE_WRITER LIVE_RATE LIVE_BATCH LIVE_SAMPLES LIVE_POLL DRY_RUN)
	# Forward each plugin's own settings too, from both backends.
	local _bc _pv _v
	for _bc in "${WRITE_BACKEND}" "${QUERY_BACKEND}"; do
		_pv="$(PLUGIN_VARS=""; source "${BENCH_DIR}/backends/${_bc}/${_bc}_backend.conf" 2>/dev/null; echo "${PLUGIN_VARS:-}")"
		for _v in ${_pv}; do
			[[ " ${CONFIG_VARS[*]} " == *" ${_v} "* ]] || CONFIG_VARS+=("${_v}")
		done
	done
}

# ------------------------------------------------------------- phase list
# Expanded once; shared by every pair. 'deploy' is pulled out and run once,
# up front, since it copies the whole directory and does not depend on which
# backend pair is running.
PHASES=()
for phase in "$@"; do
	case "$phase" in
	all)         PHASES+=(build serve-write write serve-query check query stop) ;;
	all-nocheck) PHASES+=(build serve-write write serve-query query stop) ;;
	*)           PHASES+=("$phase") ;;
	esac
done

# ------------------------------------------------------------- pre-flight
# A configuration error (unknown backend, unreadable data, no writers, ...)
# means the suite cannot run as asked, so find every one before anything is
# deployed, built or started, and end the whole run.
for _pair in "${PAIRS[@]}"; do
	set +e
	( set -e; setup_pair "${_pair%%:*}" "${_pair#*:}" )
	_rc=$?
	set -e
	if [[ $_rc -ne 0 ]]; then
		echo "monbench: configuration error in backend pair '${_pair}'; nothing was run" >&2
		exit 1
	fi
done

RUN_PHASES=()
DID_DEPLOY=0
for phase in "${PHASES[@]}"; do
	if [[ "$phase" == "deploy" ]]; then
		if [[ $DID_DEPLOY -eq 0 ]]; then
			say "phase: deploy"
			phase_deploy
			DID_DEPLOY=1
		fi
	else
		RUN_PHASES+=("$phase")
	fi
done

# ------------------------------------------------------- per-pair driver
# Runs in a subshell (see the loop at the bottom), so everything it sets is
# private to the pair; its exit status is 0 when every step succeeded.
run_pair() { # WRITE_BACKEND QUERY_BACKEND
	setup_pair "$1" "$2"

	# Create the well-defined OUTPUT_DIR subdirectory layout (see
	# EXTENDING.md for the full spec):
	#   meta/     backend-level metadata (shared across nodes; write→query
	#             handoff and dataset params)
	#   results/  benchmark statistics (user-facing output)
	#   logs/     all logs (step, daemon, benchmark)
	# nodes/<hostname>/ subdirectories (data/, run/, tmp/) are created by
	# each plugin on its own server host.
	mkdir -p "${OUTPUT_DIR}/meta" "${OUTPUT_DIR}/results" "${LOG_DIR}"

	FAILED=0
	BLOCKED=""
	SKIPPED=""
	declare -A WRITE_FAILED=()

	echo "monbench: pair=${PAIR_NAME} write=${WRITE_BACKEND} query=${QUERY_BACKEND} launcher=${LAUNCHER} servers=${SERVERS[*]} query-host=${QUERY_HOST}"
	echo "          ${COMPONENT_IDS_PER_SERVER} component IDs x ${SAMPLES} samples per server, writers: ${WRITERS}"
	echo "          output: ${OUTPUT_DIR}"

	for phase in "${RUN_PHASES[@]}"; do
		say "phase: ${phase} (${PAIR_NAME})"
		case "$phase" in
		build) phase_build ;;
		serve-write) phase_write_serve ;;
		write) phase_write ;;
		serve-query) phase_query_serve ;;
		check) phase_check ;;
		query) phase_query ;;
		live) phase_live ;;
		stop) phase_stop ;;
		clean) phase_clean ;;
		*) echo "unknown phase '$phase'" >&2; exit 2 ;;
		esac
	done

	say "pair ${PAIR_NAME} done: $([[ $FAILED -eq 0 ]] && echo 'all steps succeeded' || echo 'one or more steps FAILED')"
	[[ -z "$SKIPPED" ]] || echo "   not run because an earlier step failed: ${SKIPPED}"
	exit "$FAILED"
}

for _pair in "${PAIRS[@]}"; do
	# A subshell per pair: settings from one pair's backend confs must not
	# leak into the next. errexit is switched off around the call so a
	# failing pair is recorded, not fatal, and back on inside it.
	set +e
	( set -e; run_pair "${_pair%%:*}" "${_pair#*:}" )
	_rc=$?
	set -e
	[[ $_rc -eq 0 ]] || OVERALL_FAILED=1
done

say "done: $([[ $OVERALL_FAILED -eq 0 ]] && echo 'all pairs succeeded' || echo 'one or more pairs FAILED')"
exit $OVERALL_FAILED
