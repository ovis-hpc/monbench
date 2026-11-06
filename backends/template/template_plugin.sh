# <name>_plugin.sh -- backend hooks, sourced by bin/node.sh. Copy this
# directory to backends/<name>/ and rename each file with your backend's
# name as a prefix: <name>_plugin.sh, <name>_backend.conf,
# query/<name>_query.py. See backends/README.md for what each hook must do.
# Hooks you do not define are skipped; plugin_write is required.
#
# Available from node.sh: HOST, PLUGIN_DIR, log, die, run_logged, ds_name,
# dataset_args (array, in plugin_write), and every setting from bench.conf
# and <name>_backend.conf.
#
# OUTPUT_DIR layout (see EXTENDING.md, "OUTPUT_DIR layout"):
#   meta/                 backend-level metadata (shared across nodes)
#   nodes/<hostname>/     per-server scratch
#     data/               backend storage
#     run/                daemon runtime (PID, tokens)
#     tmp/                transient node-local files
#   results/              benchmark statistics
#   logs/                 all logs
#
# Set DRY_RUN=1 (bin/run.sh's -d flag) to log the command a hook would run
# instead of running it. Route any plain external command a hook runs
# (your writer, a compiler, rm/cleanup) through run_logged() -- see
# plugin_build/plugin_write below -- so a dry run reports it too, the same
# as the other backends. If your command reads a setting from its own
# environment rather than from an argv flag (e.g. most writers here read
# DATASET this way -- see EXTENDING.md's "The value rules"), name it
# explicitly as "run_logged VAR=val -- CMD ARGS..." (note the literal --)
# instead of "run_logged CMD ARGS..."; otherwise the logged line only works
# from inside this script, not copied out and run standalone. See
# EXTENDING.md, "Supporting bin/run.sh -d (dry run)", for the full rule and
# for a command that must be backgrounded or exec'd (plugin_serve_start/
# plugin_serve_run), which can't go through run_logged directly and must
# self-gate on DRY_RUN -- backends/dsos/dsos_plugin.sh's plugin_serve_start
# is a worked example.

# plugin_build() {
# 	run_logged make -C "${PLUGIN_DIR}/writer"   # compile writers, if any
# }

plugin_write() { # WRITER MODE INDEX SERVER_ID
	local writer="$1" mode="$2" index="$3"
	local target
	target="$(ds_name)_${writer}"
	# Replace any existing data for this partition, then write partition
	# INDEX of the data set. Print one JSON summary line; see
	# backends/README.md.
	#
	# The plugin constructs its own target name using ds_name() and records
	# it in an ingest-handoff meta file for the query engine. See EXTENDING.md,
	# "Plugin meta files".
	#
	# Route the actual writer invocation through run_logged so -d reports
	# it; if your writer reads DATASET (or anything else) from its own
	# environment rather than a flag, name it explicitly, e.g.:
	#   run_logged DATASET="${DATASET}" -- "${PYTHON}" "${PLUGIN_DIR}/writer/<name>_write.py" \
	#       --target "$target" --node-index "$index" "${dataset_args[@]}"
	#
	# After writing, record the ingest-handoff meta file:
	#   cat > "${OUTPUT_DIR}/meta/ingest-handoff-${writer}.json" <<-EOF
	#   {"target": "${target}", "backend": "<name>", "data_root": "${OUTPUT_DIR}/nodes/${HOST}/data"}
	#   EOF
	die "template backend: plugin_write not implemented (writer ${writer}, target ${target}, partition ${index})"
}

# plugin_serve_start() { :; }   # SERVER_ID; start the service in the background
# plugin_serve_run() { :; }     # SERVER_ID; run it in the foreground (Slurm)
# plugin_serve_stop() { :; }
# plugin_serve_status() { :; }  # return 0 when the service is ready

# plugin_clean() {              # SERVER_ID; delete this server's data and run files
# 	# Clean node-specific scratch: data, daemon files, transient files
# 	run_logged rm -rf "${OUTPUT_DIR}/nodes/${HOST}/data/<name>"
# 	run_logged rm -f "${OUTPUT_DIR}/nodes/${HOST}/run/<name>.pid"
# }
