# monbench

A pluggable benchmark for storing and querying monitoring data, designed for
distributed HPC monitoring deployments.

monbench writes a deterministic monitoring-style data set into a database
backend, then runs a set of queries against it from a separate query node,
verifying every answer and timing every query. The core of the suite knows
nothing about any particular database: each backend is a plugin under
`backends/`. The first plugins are SOS, DSOS and InfluxDB; `backends/template/`
is the starting point for others.

```
  server node 1           server node 2           ...      query node
  ─────────────           ─────────────                    ──────────
  writer ─> partition 0   writer ─> partition 1            query engine
  backend service  <────────────── backend protocol ─────  + backend adapter
```

## Layout

| Path | Purpose |
|---|---|
| `bench.conf.example` | Template: copy to `bench.conf` and edit for your site (backend(s), hosts, paths, data-set size, query settings). `bench.conf` itself is not part of the repository |
| `bin/run.sh` | Driver: runs each phase on the right nodes via ssh, Slurm or by hand |
| `bin/node.sh` | Actions executed on a single node; calls the backend's hooks |
| `core/dataset.py` | The data set: value rules, parameters, metadata files |
| `core/bench.py` | Query engine: runs a query set, verifies and times each query |
| `core/adapter.py` | The interface query adapters implement |
| `dataset/default.json` | Data set definition: attributes and keys |
| `queries/default.json` | Default query set, used when `QUERY_FILE` is empty |
| `EXTENDING.md` | How to add a backend, define a dataset, or write a query file |
| `backends/README.md` | Summary of backends provided and their capabilities |
| `backends/sos/` | SOS backend: local containers, C and Python writers, direct query adapter |
| `backends/dsos/` | DSOS backend: distributed query through `dsosd`, DSOS writer, `dsosql` adapter |
| `backends/influxdb/` | InfluxDB v3 backend: writer, Python and HTTP query adapters |
| `backends/template/` | Skeleton for a new backend |
| `slurm/monbench.sbatch` | Batch script for a Slurm job |
| `ROADMAP.md` | Current limitations, and what is planned or deferred, with the reasons |
| `OUTPUT.md` | Result file structure, JSON schema, and timing measurement boundaries |

## Write and query backends

The suite separates writing from querying. `WRITE_BACKEND` names the backend
that writes the data; `QUERY_BACKEND` names the one that queries it. They can
be the same backend or different ones, depending on what you want to measure.
(`BACKENDS` is the more general form of this — see "Running multiple backend
pairs" below — and these two are only read when `BACKENDS` is empty.)

Set these in `bench.conf`. The run stops with an error if a backend does not
exist, if its data was written for a different backend, or if the query
adapter cannot reach it.

### Example scenarios

| `WRITE_BACKEND` | `QUERY_BACKEND` | What it measures |
|---|---|---|
| `sos` | `dsos` | The primary HPC deployment: writers store data locally on each server for speed; queries go through `dsosd`, which fans out to every server and merges the results |
| `sos` | `sos` | Query performance without RPC overhead — the query node opens each server's containers directly on disk. Useful as a single-node baseline or on a shared filesystem |
| `dsos` | `dsos` | Both writes and queries go through `dsosd`, so the benchmark includes the distributed-write path as well |
| `influxdb` | `influxdb` | InfluxDB v3 end-to-end, for comparison against the SOS/DSOS results on the same data set and queries |

## Requirements

For every backend:

- Python 3 on every node, and on the query node the Python client of the
  backend being queried.
- The suite directory at the same path on all nodes. On a shared filesystem
  nothing needs copying; otherwise use `bin/run.sh -l ssh deploy`.

Each backend has additional requirements described in its own README under
`backends/<n>/README.md`.

## The data set

The data set is split into one partition per server. Each server writes
`COMPONENT_IDS_PER_SERVER × SAMPLES` records (80,000 by default). Every record
is a function of its `component_id` and `seq` (the sample number, 0 through
`SAMPLES − 1`), so any row can be verified on its own. `seq` is stored as
an attribute alongside the computed values; it has no physical meaning
outside the benchmark but is what makes every row uniquely identifiable for
verification.

The attribute names and units follow the flat (non-record) metrics of the
`curated_metrics` LDMS sampler plugin: `/proc/meminfo` percentages and
`MemTotal`, `/proc/loadavg`, and the scalar `/proc/stat` counters. The exact
value rules are defined in `core/dataset.py`, which the query engine also
verifies every answer against:

| Attribute | Value |
|---|---|
| `component_id` | `(partition + 1) × 1000 + c`, for c in `[0, COMPONENT_IDS_PER_SERVER)` |
| `timestamp` | `T0 + seq × INTERVAL` |
| `job_id` | `1 + seq ÷ JOB_LEN` (integer division; each job spans all servers) |
| `MemTotal`, `free`, `buff`, `cached`, `active`, `inactive`, `dirty`, `writeback` | `/proc/meminfo`-style total (kB) and percentages, each a different deterministic function of `component_id` and `seq` |
| `load1min`, `load5min`, `load15min` | `/proc/loadavg`-style load averages |
| `runnable`, `scheduling_entities`, `newest_pid` | the runnable/total scheduling-entity counts and newest PID that accompany `/proc/loadavg` |
| `interrupts`, `context_sw`, `fork`, `procs_running`, `procs_blocked` | `/proc/stat`-style scalar counters |

Per-CPU-core and per-network-interface metrics, which `curated_metrics`
exposes as nested LDMS records, are not part of the default data set; see
`ROADMAP.md`, "Per-CPU and per-interface records".

`dataset/default.json` lists the attributes and the **keys** the data set is
meant to be queried by: `timestamp`, `comp_time` (component_id, timestamp)
and `job_comp_time` (job_id, component_id, timestamp). Each backend maps the
keys onto its own indexing; the SOS plugin creates an index for each.

### Naming and storage

The dataset definition's `"name"` field (`DS.name`) is the single source of
truth for the logical data name (e.g. `"monitoring_nested"`). The core
never constructs storage target names — that is the plugin's job. Each
backend plugin constructs its own storage name (a container path, a
measurement, a database) during the write phase and records it in a **plugin
meta file** at `OUTPUT_DIR/meta/storage-<writer>.json`. The query engine
reads that meta file to find where the data was stored.

After each writer finishes, the harness also records dataset parameters in a
separate **core meta file** at `OUTPUT_DIR/meta/dataset-<writer>.json`. The
query engine verifies against those parameters, so a later change to `SAMPLES`
does not invalidate a check of data written earlier.

The default data set is a fixed monitoring shape. To define a different
shape, see the "Defining a custom dataset" section in `EXTENDING.md`.

### OUTPUT_DIR layout

Each backend pair gets its own `OUTPUT_DIR/<pair>/` with a well-defined
subdirectory layout:

| Subdirectory | Purpose |
|---|---|
| `meta/` | Writer→query handoff: each plugin writes `storage-<writer>.json` here during the write phase; the query engine reads it to find where the data was stored |
| `nodes/<hostname>/data/` | Backend storage (containers, databases) |
| `nodes/<hostname>/run/` | Runtime files: daemon PIDs, tokens |
| `nodes/<hostname>/tmp/` | Transient files: schema templates, session files |
| `results/` | Benchmark statistics (user-facing output) |
| `logs/` | All logs (step, daemon, benchmark) |

The `clean` phase removes the per-node trees under `OUTPUT_DIR/<pair>/nodes/`.

## How monbench works

A run is a sequence of **phases**. Each phase does one thing — build
writers, start a service, write data, verify queries, time queries, or clean
up. `bin/run.sh` drives the phases, running each step on the right node
through one of three **launchers**: `ssh`, `slurm`, or `manual`.

### Phases

| Phase | What it does | Where it runs |
|---|---|---|
| `deploy` | Copy the suite directory to every node (ssh only; unnecessary on a shared filesystem) | from the driver node |
| `build` | Compile the backend's writers, if any need compiling | each server node |
| `serve-write` | Start the write backend's service on the servers, if it has one (`PLUGIN_SERVICE=1`). Skipped when the backend needs no service | each server node |
| `write` | Write the data set: each server writes its own partition, once per writer in `WRITERS`. After all servers finish, the harness records a metadata file | each server node (parallel) |
| `serve-query` | Start the query backend's service, if it differs from the write backend. Skipped when they are the same or the query backend needs no service | each server node |
| `check` | Run each query once, untimed, and verify the full result against the data set. No timing is recorded | query node |
| `query` | Run each query `REPEAT` times and time each run. No verification is performed. Reports median, p95 and minimum | query node |
| `live` | Write at a controlled rate while the query node polls it, checking that every answer is valid and the row count never decreases | servers + query node |
| `stop` | Stop both backend services | each server node |
| `clean` | Stop both services and delete the servers' data and run files | each server node |

Two shorthand phases expand into a sequence:

| Shorthand | Expands to |
|---|---|
| `all` | `build serve-write write serve-query check query stop` |
| `all-nocheck` | `build serve-write write serve-query query stop` |

Phases can be given in any combination on the command line. This is useful
for iterating: start the service once, then run `check` or `query`
repeatedly while adjusting queries or settings, and `stop` when done:

```bash
bin/run.sh -l ssh serve-write         # start the write backend's service
bin/run.sh -l ssh write               # write the data set
bin/run.sh -l ssh serve-query         # start the query backend's service

bin/run.sh -l ssh check               # verify — run as many times as needed
bin/run.sh -l ssh query               # time — run as many times as needed

bin/run.sh -l ssh stop                # stop both services when done
```

When `BACKENDS` names more than one write/query pair (see "Running multiple
backend pairs" below), every phase given on the command line runs once per
pair, in order, before moving to the next pair; `deploy` is the exception and
only runs once regardless of how many pairs are configured.

### Launchers

| Launcher | How it runs steps | When to use |
|---|---|---|
| `ssh` | ssh to each node (default) | Any environment with passwordless ssh between nodes |
| `slurm` | `srun` inside a Slurm allocation (`salloc` or `sbatch`) | HPC clusters with Slurm |
| `manual` | Prints each command with the node to run it on, then waits for you to press Enter | Debugging, or environments without ssh or Slurm |

Select with `-l`: `bin/run.sh -l slurm all`.

When the Slurm launcher is used, whichever of `SERVER_HOSTS` and `QUERY_HOST`
is still empty after `bench.conf` and the environment have been applied is
filled from the allocation: all nodes except the last become servers; the
last becomes the query host. This needs at least 2 nodes. If only one of the
two is given, the other is taken from the remaining nodes. Set both to
control the split; the allocation size is then not checked.

The query host must be a separate node: the run stops with an error if
`QUERY_HOST` is also listed in `SERVER_HOSTS`, with any launcher.

In manual mode, change settings in the configuration files rather than in
the shell environment, because the commands you type on the other nodes read
only the files.

Every launcher, not only `manual`, now prints the exact node-side command a
step is about to run — `env VAR=val ... bash bin/node.sh ACTION ARGS...`,
with every setting (`CONFIG_VARS`/`PLUGIN_VARS`) it needs already in that
`env` prefix — before running it, and writes that same line to the step's
log file under `OUTPUT_DIR/<pair>/logs/`. It is self-contained: copy it onto
the named host and it reproduces exactly what the harness ran, with or
without `-d` (e.g. to attach a debugger or rerun one step by hand).

### Dry run

`-d` logs the exact command each phase would run on each server/query host,
without actually running it:

- `build`, `write`, `serve-start`/`serve-run`/`serve-stop` and `clean` log
  the command they would run instead of running it — nothing is built,
  written, started, stopped, or deleted.
- `check` and `query` resolve and print every query's final, placeholder-
  filled text exactly as it would be sent, but never connect to the backend
  or actually send it (`core/bench.py`, which reports each query with status
  `DRY` instead of checking or timing it — see `OUTPUT.md`).

```bash
bin/run.sh -d build write
bin/run.sh -c bench-sos-dsos.conf -d check query
```

It works with the `ssh` and `slurm` launchers (`-d` is forwarded to every
node as `DRY_RUN=1`); with `-l manual` you already see each command printed
without running it, so `-d` adds little there besides also showing up in the
printed environment assignments.

A writer invocation's own logged line (from `run_logged()` inside
`bin/node.sh`) includes any setting the writer reads from its own
environment rather than from a flag — e.g. `DATASET`, which every writer
needs — so that line too is standalone-runnable, not just a record of what
ran. See `EXTENDING.md`, "Supporting `bin/run.sh -d` (dry run)", if you are
writing a new backend's plugin and want your own commands to behave the
same way.

### Verification

The `check` phase runs each query through the backend's query adapter and
compares the result against the records the question should return, computed
from the data-set value rules. A query passes when:

- the row count is right (with `limit`, both whole-result and per-server
  counts are accepted, and the report says which);
- every returned row is a real record that satisfies the question, with no
  duplicates and every selected value correct (this needs `component_id` and
  `seq` in the result; otherwise only the count is checked);
- rows are in the stated order, when the ordering columns are in the result.

Queries without a `match` description, or marked `"verify": false`, are run
and reported as INFO.

### Timing

The `query` phase runs each query `REPEAT` times (default 5) and records
the duration of each run, from the start of `adapter.query()` until the
last row has been fetched. It reports the median, p95 and minimum. The
connection to the backend is made once, before any query, so timings do not
include connecting.

`QUERY_ADAPTER` selects the adapter within the backend; empty selects the
backend's default. Each backend's README lists its available adapters.

See `OUTPUT.md` for the exact timing boundaries and the full JSON result
structure.

### The live test

`live` writes at `LIVE_RATE` records per second per server while the query
node polls it with the query set's first query of type `all`. It passes
when every answer contains only valid, non-duplicated records, the row count
never decreases, and the final count is reached.

## Quick start

1. Copy `bench.conf.example` to `bench.conf` and edit the "edit these" block
   near the top: `BACKENDS` (or `WRITE_BACKEND`/`QUERY_BACKEND`),
   `SERVER_HOSTS`, `QUERY_HOST` and, if the backend's software is not on the
   default paths, `ENV_SETUP`. `bin/run.sh` refuses to run until these are
   set — `bench.conf` names your real hosts and paths, so keep it local
   rather than committing it.
2. Edit `backends/<BACKEND>/<BACKEND>_backend.conf` for each backend you
   are using (see `backends/<n>/README.md` for what each setting does).
3. Run the full sequence:

```bash
bin/run.sh -l ssh all
```

This runs the `all` phase sequence described above. Use `-l slurm` inside
a Slurm allocation, or `-l manual` to run each step by hand. For Slurm
batch jobs, `slurm/monbench.sbatch` is a starting point:

```bash
sbatch slurm/monbench.sbatch
```

Settings can be overridden per run (ssh and Slurm launchers only):

```bash
SAMPLES=500 WRITERS=c bin/run.sh -l ssh all
```

## Running multiple backend pairs

To benchmark several write→query pairs (e.g. `sos→dsos` and
`influxdb→influxdb`) against the same `SERVER_HOSTS`/`QUERY_HOST` and the
same data set and query file, set `BACKENDS` in `bench.conf` instead of
`WRITE_BACKEND`/`QUERY_BACKEND`: a space-separated list of `writer` or
`writer:query` tokens (`query` defaults to `writer` when omitted):

```bash
BACKENDS="sos:dsos influxdb"
```

`bin/run.sh` runs every phase you give on the command line once per pair, in
order, before moving to the next pair:

```bash
bin/run.sh -l ssh all
```

Every other setting in `bench.conf` — `DATASET`, `QUERY_FILE`, `SAMPLES`,
`REPEAT`, and so on — is shared by every pair, so all backends are measured
against exactly the same data and the same questions.

`OUTPUT_DIR` gains a subdirectory named after the pair (`sos-dsos`,
`influxdb`, derived from the backend names themselves). That name is fixed
once per pair (before any of its phases run) and reused by every phase in
it — build, write, check, query, and so on — regardless of which single
backend, write or query, is actually doing the work for a given phase. That
is what keeps a pair's write and query sides agreeing on where the data is:
for `sos:dsos`, the write phase (handled by `sos`) and the check/query
phases (handled by `dsos`, reading the very same on-disk containers `sos`
wrote) both resolve `OUTPUT_DIR` from the one fixed `sos-dsos` pair name,
rather than from whichever single backend happens to be running that
particular phase — keying it by whichever backend is active for a given
phase (`sos` while writing, `dsos` while querying) is what causes them to
disagree. The same per-pair namespacing on `OUTPUT_DIR` (and its `logs/`
subdirectory) is also what keeps e.g. `sos→sos` and `sos→dsos` results
apart, since those legitimately differ even when the data underneath is
identical.

Because `OUTPUT_DIR` is namespaced per pair, each pair writes its own copy
of the data, even when two pairs share a writer (e.g. comparing `sos→sos`
against `sos→dsos`): give each pair its own `write` (or `all`) to populate
its own `OUTPUT_DIR` — the second pair does not inherit the first's data.

This only works when every pair can share one `SERVER_HOSTS`/`QUERY_HOST`/
`ENV_SETUP`. If you need different hosts, or environment setup that
conflicts between backends, keep a separate `bench.conf`-style file per pair
instead (each your own, outside the repository, same as `bench.conf`
itself) and select each with `-c`:

```bash
bin/run.sh -c bench-sos-dsos.conf -l ssh all
bin/run.sh -c bench-influxdb.conf -l ssh all
```

Each file sets its own `BACKENDS` (or `WRITE_BACKEND`/`QUERY_BACKEND`),
`SERVER_HOSTS`, `QUERY_HOST`, `OUTPUT_DIR` and any backend-specific
settings.

## Query sets

A query set is a JSON file. When `QUERY_FILE` is empty, the default set
`queries/default.json` is used. Each query describes the **question** once,
independently of any backend, and gives the **query string** for each
backend:

```json
{
  "name": "one job, one component, part of the job's time",
  "type": "job+component+time",
  "match": { "where": "job_id == {job_mid} and component_id == {comp_slast} and timestamp >= {t_mid}",
             "order_by": ["job_id", "component_id", "timestamp"] },
  "query": { "dsos": "select * from {schema} where job_id == {job_mid} and component_id == {comp_slast} and timestamp >= {t_mid} order_by job_comp_time" }
}
```

| Field | Meaning |
|---|---|
| `name`, `type` | Labels for the report. The default set names types by the conditions the question uses, so the labels apply to every backend |
| `match` | The question, used for verification: `where` conditions, `order_by` (the attributes the result is ordered by) and optionally `limit` |
| `query` | Query strings keyed by adapter name or dialect (`dsos` covers the `dsos`, `dsosql` and `sos` adapters). A plain string is used for every adapter. An adapter with no string for a question reports it as SKIP |
| `verify` | `false` to run and time the query without verifying it. Default `true` |

`match.where` is conditions of the form `<attribute> <op> <number>` joined by
`and`, with `op` one of `==`, `!=`, `<`, `<=`, `>`, `>=`. Backend-specific
choices, such as which DSOS index to use, belong in the query string only.

The default set covers:

| Type | Questions |
|---|---|
| `all`, `time` | Every record; all components over 25% and 1% of the time span |
| `component`, `component+time` | One component, alone and within a time window. The time-window question is asked three times, through the `comp_time`, `timestamp` and `job_comp_time` indices, to show the effect of the index choice |
| `job`, `job+component`, `job+component+time` | One, two and all three attributes of the `job_comp_time` key |
| `job+time` | Job and time without component: a gap in the `job_comp_time` key |
| `metric-value` | A condition on `free`, which has no index |
| `projection`, `limit` | Selected columns only; the first rows with `limit` |
| `aggregate` | A `group_by` query; timed, not verified |

Query strings and `match` may use placeholders filled from the data set so
that one file works at any size. See `EXTENDING.md`, "Writing a query file",
for the full list of placeholders, the keying rules, and how to define your
own query file.

## Output

- `results/write-<writer>-<host>.json`: one JSON object per writer run,
  including records written, time to write, time for the final sync, and
  records per second.
- `results/meta/dataset-<writer>.json`: what was written by each writer (the
  data set parameters and writer identity).
- `results/benchmark-<writer>-<adapter>.json`: each query with its
  expanded query string, status, row count, every timed run, median, p95,
  minimum and rows per second.
- `results/benchmark-<writer>-live.json`: live-poll result.
- `logs/`: the full output of every remote step.

See `OUTPUT.md` for the full JSON structure, what each field means, and
exactly what the write and query timing measurements cover.

## Assumptions and limits

Points to be aware of when running the harness or interpreting its results.
Design limitations and plans for later versions are tracked in `ROADMAP.md`.
Backend-specific notes are in each backend's README under `backends/<n>/`.

- **Timings use the Python API.** All current adapters use Python client
  libraries, so timings include Python-level overhead (e.g. building
  DataFrames). They reflect what an analyst's code would experience, not the
  fastest a native client could achieve.
- **Data written by earlier versions has no metadata file.** The engine then
  verifies against the current settings and says so; rewrite the data to get
  a metadata file.
