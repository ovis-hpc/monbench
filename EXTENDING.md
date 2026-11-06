# monbench extension guide

How to add a new database backend, define a custom dataset, or write a query file for the monbench benchmark suite.

## Overview

The benchmark suite has three extension points, each defined by one or more files in the repository:

| Extension point | What it controls | Where it lives |
| --- | --- | --- |
| Backend | How data is written, stored, queried, and how the database service is managed | `backends/<n>/` (at least three files: a config, a plugin script, a query adapter; plus writers and any tools the backend needs) |
| Dataset | The schema — attribute names, types, and access keys — and the deterministic value rules every writer must follow | `dataset/<n>.json` (definition) + `core/dataset.py` (value rules) |
| Query file | The benchmark questions: what to query, in each backend's dialect, and what to verify the answer against | `queries/<n>.json` |

The three are independent. A new backend works with the existing default dataset and query file. A new dataset can be benchmarked by any backend that writes it correctly. A new query file can target any backend that has query strings for its questions.

The connection between them is the dataset definition: it names the attributes that query files reference in their `{placeholders}` and `match` specs, and it lists the attribute types that a backend's `type_overrides` map to its native conventions.

## Writing a new backend

Copy `backends/template/` to `backends/<n>/` and rename each file with your backend's name as a prefix. A backend has three required files.

### Lifecycle

The harness runs phases in a fixed order. Each phase calls specific plugin hooks:

1. **`clean`** — the harness stops the services, calls `plugin_clean` on each server, then removes `nodes/<hostname>/` and `meta/` (optional, only when requested; `purge` also removes `results/` and `logs/`)
2. **`build`** — `plugin_build`: compile writers if needed
3. **`serve-write`** — `plugin_serve_start`: start the database service (only if `PLUGIN_SERVICE=1`)
4. **`write`** — `plugin_write`: write each partition of the dataset, one call per server. The plugin also writes an ingest-handoff meta file recording where it stored the data (see "Plugin meta files" below)
5. **`meta`** — the harness records dataset parameters to `meta/dataset-<writer>.json`; no plugin hook
6. **`serve-query`** — `plugin_serve_start` on the query backend (skipped if it is the same as the write backend)
7. **`query`** — the engine reads the plugin's ingest-handoff meta file from `meta/ingest-handoff-<writer>.json` to find the storage target, connects the adapter (`connect()`), loads the query set, and runs each query through `query()` / `count()` / `records()`
8. **`stop`** — `plugin_serve_stop`: stop the service

A backend that manages its own service externally (`PLUGIN_SERVICE=0`) simply skips the serve and stop phases — the harness calls `plugin_write` and the query adapter directly.

### The config: `<n>_backend.conf`

Sourced after `bench.conf`, so it can use core settings such as `OUTPUT_DIR`. Use the `VAR="${VAR:-default}"` form so every value can be overridden from the environment. These declarations are read by the core:

| Declaration | Purpose |
| --- | --- |
| `DEFAULT_WRITERS` | Writer names used when `WRITERS` is empty in `bench.conf`, e.g. `"py"` |
| `PLUGIN_SERVICE` | `1` if the harness must start and stop a service (the `serve-write`/`serve-query` and `stop` phases); `0` if the service is managed externally |
| `PLUGIN_READS` | Space-separated list of backends whose written data this backend can query (e.g. `"sos dsos"`). The harness checks this against the writer's storage meta file and rejects incompatible write→query pairs before any query runs. Omit or leave empty if not applicable |
| `PLUGIN_DESC` | Short description of what this backend does, shown in the benchmark output |
| `PLUGIN_VARS` | Space-separated list of this backend's own settings, so they are forwarded to remote nodes |

All other settings (connection URLs, database names, tokens, batch sizes) are your own and follow the same `"${VAR:-default}"` pattern.

### The plugin script: `<n>_plugin.sh`

Sourced by `bin/node.sh` on the node where a step runs. It defines shell functions (hooks). `plugin_write` is required; the others are optional.

| Hook | Signature | Purpose |
| --- | --- | --- |
| `plugin_build` | (none) | Compile writers if needed |
| `plugin_write` | `WRITER MODE INDEX SERVER_ID` | Write partition `INDEX` of the dataset, print one JSON summary line. The plugin constructs its own storage target name (see "Plugin meta files" below) |
| `plugin_serve_start` | `SERVER_ID` | Start the database service in the background |
| `plugin_serve_run` | `SERVER_ID` | Run the service in the foreground (Slurm launcher) |
| `plugin_serve_stop` | (none) | Stop the service |
| `plugin_serve_status` | (none) | Return 0 when the service is ready |
| `plugin_clean` | `SERVER_ID` | Delete backend data this server keeps outside `nodes/<hostname>/` (the services are already stopped). `die` instead of deleting if a daemon you did not start still holds the data |

The `plugin_write` hook receives `dataset_args`, an array with the flags every writer should accept: `--components N --samples N --t0 SECS --interval SECS --job-len N --batch N --rate RECS_PER_SEC`. `--batch` is the number of node records the writer commits or sends together; the core times each batch, so it is the unit of ingest latency. Your writer must produce records that match the value rules in `core/dataset.py` exactly — the query engine verifies every answer against them.

The `plugin_write` arguments in order: `$1` = WRITER (the writer name), `$2` = MODE (`full` or `live`), `$3` = INDEX (the partition index, 0-based), `$4` = SERVER\_ID. The plugin constructs its own storage target name using `ds_name()` (see "Plugin meta files" below) — the core does not pass a target. MODE is only relevant if the backend distinguishes between a full write and a live (streaming) write. In `live` mode, `dataset_args` includes a positive `--rate` so the writer throttles its output; in `full` mode, `--rate 0` means unthrottled.

The writer's summary line is a single JSON object on stdout. It carries **raw inputs only**: counts, phase timers and progress checkpoints. A writer computes no rate, no percentile and no total time; `core/stats.py` derives all of them, the same way for every backend. For example:

```json
{"writer": "influx", "measurement": "monitoring", "backend": "influxdb/python", "node_index": 0, "records": 80000, "list_entries": 0, "errors": 0, "batch": 500, "rate": 0, "start_epoch_s": 1767225600.123456, "write_s": 7.73, "close_sync_s": 0.0, "checkpoints": [[0, 0], [0.048, 500], [0.097, 1000], ..., [7.73, 80000]]}
```

| Field | Meaning |
| --- | --- |
| `records` | Node records written (list-attribute entries are counted separately) |
| `list_entries` | List-attribute entries written; 0 for a dataset without list attributes |
| `errors` | Writes that failed |
| `batch`, `rate` | The `--batch` and `--rate` the writer ran with, echoed |
| `start_epoch_s` | Wall-clock time (Unix seconds) at which the write began; lets the core place the hosts' writes on one time line |
| `open_s` | Time to open the container or connection, if the backend has such a step (optional) |
| `write_s` | First record inserted until the last record committed |
| `close_sync_s` | Close and flush to stable storage; `0` if the backend has no such step |
| `checkpoints` | `[[seconds since the write began, records written so far], ...]`, starting with `[0, 0]`. Add one entry after every batch has been committed or its request acknowledged, and a final one when the write finishes |

The core turns each interval between two checkpoints into one sample (its duration is the batch's latency; records divided by duration is its throughput) and reports the same summary for every backend: `n`, `min`, `p50`, `p90`, `p95`, `p99`, `max`, `mean`, `stddev`. Nothing assumes a data set size, a batch size or a host count. A trailing batch smaller than `--batch` is left out of the distributions but kept in the totals, and a percentile is reported only when there are enough batches for it (20 for p95, 100 for p99). See `OUTPUT.md`, "Statistics".

The harness saves the line to `results/ingest-<writer>-<host>-raw.json`.

### Plugin meta files

Each backend plugin is responsible for constructing its own storage target
name and recording where and how it stored the data. The plugin writes a
JSON ingest-handoff meta file to `meta/ingest-handoff-<writer>.json` during
the write phase. The query engine reads this file to find the storage
target, rather than reconstructing paths by convention.

The helper function `ds_name()` (defined in `bin/node.sh`) reads the dataset
definition's `"name"` field from the `DATASET` JSON file. Use it to construct
your target name. The convention is `"$(ds_name)_${writer}"` for full mode
and `"$(ds_name)_live"` for live mode, but the plugin is free to choose.

The meta file must be valid JSON. Its contents are backend-specific, but it
should include at least the storage target name. Examples:

**SOS/DSOS** (`meta/ingest-handoff-c.json`):

```json
{
  "target": "monitoring_nested_c",
  "backend": "sos",
  "data_root": "/data/monbench/output/sos-dsos/nodes/server-01/data",
  "container_path": "/data/monbench/output/sos-dsos/nodes/server-01/data/monitoring_nested_c"
}
```

**InfluxDB** (`meta/ingest-handoff-influx.json`):

```json
{
  "measurement": "monitoring_nested",
  "database": "monbench",
  "host": "http://influx-host:8181"
}
```

The query engine reads the storage meta file and populates `ctx["target"]` from:
- `"target"` key (SOS/DSOS)
- `"measurement"` key (InfluxDB)
- Fallback convention: `"<ds_name>_<writer>"` (normal) or `"<ds_name>_live"` (live mode)

**Available helpers from `node.sh`:** `HOST`, `PLUGIN_DIR`, `log`, `die`,
`run_logged`, `ds_name`, `dataset_args` (array, in `plugin_write`), and
every setting from `bench.conf` and `<name>_backend.conf`.

### OUTPUT_DIR layout

Each backend pair runs under `OUTPUT_DIR/<pair>/`. The harness creates the
top-level subdirectories before any phase runs. When `BACKENDS` names more
than one pair, each pair gets its own isolated tree.

```
OUTPUT_DIR/<pair>/
  meta/                                 # backend-level metadata (shared across nodes)
  nodes/                                # per-server scratch (one subdir per hostname)
    <hostname>/
      data/                             #   backend storage
      run/                              #   daemon runtime
      tmp/                              #   transient node-local files
  results/                              # benchmark statistics (user-facing output)
  logs/                                 # all logs
```

#### `meta/` — backend-level operational metadata

Files that are the same across all server nodes or that coordinate between
phases. Written once, readable from any node (must be on a shared
filesystem or copied). Both plugin-written and core-written meta files live
here.

| File | Written by | Purpose |
| --- | --- | --- |
| `ingest-handoff-<writer>.json` | plugin (`plugin_write`) | Where writer `<writer>` stored data — the writer→query handoff. Core contract: the query engine reads this to find the storage target (see "Plugin meta files" above) |
| `dataset-<writer>.json` | core (`dataset.py`) | Dataset parameters for writer `<writer>` — used by the query engine for verification |
| Backend-specific shared config | plugin | Any file that is identical across servers and needed by multiple nodes or phases |

Backend-specific shared config examples:

| Backend | File | Purpose |
| --- | --- | --- |
| DSOS | `dsosd-directory.json` | Routing config listing all server endpoints (same on every server) |
| SOS | `sos-schema.json` | SOS schema template derived from the dataset definition |
| DSOS | `dsos-write-session.txt` | Session file listing all participating servers |

**Naming rule**: plugin-written files should use a descriptive prefix.
`ingest-handoff-<writer>.json` is the required name for plugin meta. Other
files use the backend name or a short description as prefix (e.g.
`dsosd-directory.json`, not `directory.json`).

#### `nodes/<hostname>/` — per-server scratch

Everything specific to one server node. Each node creates its own
subdirectory using its hostname. All files here are considered scratch —
wiped on clean.

| Subdirectory | What goes here | Examples |
| --- | --- | --- |
| `data/` | Backend storage — the actual database files the backend manages | SOS containers (`monitoring_nested_c/`), InfluxDB data directory |
| `run/` | Daemon runtime files needed to manage a running service | PID files (`dsosd.pid`), auth tokens (`influxdb3-token.json`) |
| `tmp/` | Transient files that can be deleted after the run completes | (empty in most cases; available for future use) |

**Naming rule**: files do not need a hostname suffix — the parent directory
already identifies the node. Use `dsosd.pid`, not `dsosd-server-01.pid`.

**`data/` override**: For HPC sites where database storage must reside on
fast local disk (NVMe, local SSD) rather than the shared filesystem where
`OUTPUT_DIR` may live, backends provide an independently overridable path
for their storage. For example, `SOS_DATA_ROOT` overrides where SOS
containers go (default: `nodes/<hostname>/data`), and `INFLUX_DATA_DIR`
overrides InfluxDB's data directory. The override only affects `data/`;
the rest of the node's `run/` and `tmp/` stay under `nodes/<hostname>/`.
`clean` removes `nodes/<hostname>/` as a whole, so `plugin_clean` is what
deletes an overridden `data/` path.

#### `results/` — benchmark statistics

Only benchmark output that the user reads. No operational or intermediate
files. See `OUTPUT.md` for the full specification of each file's structure.

| File pattern | Written by | Contents |
| --- | --- | --- |
| `ingest-<writer>-<host>-raw.json` | each writer | The writer's raw output for that host (counts, timers, checkpoints) |
| `benchmark-<writer>-<adapter>.json` | `core/bench.py` | Combined benchmark result (write + query stats) |
| `benchmark-live-<writer>.json` | `core/bench.py` | Live-poll benchmark result |

#### `logs/` — all logs

Every log file in one directory. Log filenames include enough context to
identify the source (phase, writer, hostname, adapter).

| File pattern | Source | Contents |
| --- | --- | --- |
| `<phase>-<host>.log` | `run.sh` | Step log (stdout/stderr of a remote step) |
| `<daemon>-<host>.log` | plugin | Daemon log (output from a background service) |
| `benchmark-<writer>-<adapter>.log` | `core/bench.py` | Everything printed to stdout during the benchmark |

Step log examples: `build-server-01.log`, `write-c-server-01.log`,
`serve-write-server-01.log`. Daemon log examples: `dsosd-server-01.log`,
`influxdb3-server-01.log`.

#### Clean and purge semantics

`bin/run.sh clean` resets a pair to a clean slate but keeps its `results/` and
`logs/`. It does this in order:

1. It runs the `stop` phase, so no service is running and no PID file is lost.
2. It calls `plugin_clean SERVER_ID` on each server for the write backend,
   and for the query backend too when it differs.
3. Once every `plugin_clean` has succeeded, it runs `node.sh wipe` on every
   server and on the query node. That removes
   `OUTPUT_DIR/<pair>/nodes/<hostname>/` and `OUTPUT_DIR/<pair>/meta/`, and
   `nodes/` as well once it is empty.

If any `plugin_clean` fails, nothing is wiped and the pair is reported as
FAILED.

`bin/run.sh purge` runs `clean`, then deletes `OUTPUT_DIR/<pair>/` from the
driver node. That takes `results/` and `logs/` with it, so nothing is left of
the pair. If `clean` failed, `purge` deletes nothing.

Neither command recreates any directory. Every other phase creates the layout
it needs when it starts.

By hand, the two come to:

```bash
# clean: per-node scratch and metadata go, results and logs stay
rm -rf OUTPUT_DIR/<pair>/nodes OUTPUT_DIR/<pair>/meta

# purge: everything for this pair
rm -rf OUTPUT_DIR/<pair>
```

Because the harness removes `nodes/<hostname>/` and `meta/`, `plugin_clean`
only deletes backend data that lives outside them, such as an overridden
`SOS_DATA_ROOT` or `INFLUX_DATA_DIR`. It does not touch `logs/` or `results/`.
Data cannot be removed from under a live process, and a process the harness
did not start is not the harness's to kill, so `plugin_clean` calls `die`
when one still holds the data (`dsosd` and `influxdb3` do this).

#### Supporting `bin/run.sh -d` (dry run)

`node.sh` defines `run_logged CMD ARGS...` (next to `log`/`die`, before your plugin is sourced): it logs a command's argv as a `"+ ..."` line and then runs it, unless `DRY_RUN` is set, in which case it only logs. Route any plain external command your hooks run through it — the writer invocation in `plugin_write`, a compiler in `plugin_build`, `rm`/cleanup in `plugin_clean` — so `bin/run.sh -d` reports your backend's real commands too, consistently with the others.

If your command reads a setting from its own environment rather than from an argv flag — most writers do this for `DATASET` — name it explicitly as `run_logged VAR=val -- CMD ARGS...` (note the literal `--`) instead of just `run_logged CMD ARGS...`. `run_logged` logs those `VAR=val` assignments as part of the `"+ ..."` line and, for a real run, exports exactly those vars for `CMD` (equivalent to writing `VAR=val CMD ARGS...` by hand). Without this, the logged line still runs correctly from inside `node.sh` (the var is already in its environment), but is not something you can copy out and run standalone on the node — see `backends/sos/sos_plugin.sh`'s and `backends/dsos/dsos_plugin.sh`'s `plugin_write` (both pass `DATASET` this way) for examples, and `bin/node.sh`'s `run_logged` doc comment for the exact rule (only args before a literal `--` are treated as env assignments; with no `--` anywhere, everything is CMD/ARGS as before, so existing calls with no env vars need no change).

A command that must be backgrounded or `exec`'d (typically in `plugin_serve_start`/`plugin_serve_run`) can't go through `run_logged` directly: log your own `"+ ..."` line (including any `VAR=val` prefix the command needs, the same way) and check `[[ -n "${DRY_RUN:-}" ]]` yourself before actually starting or stopping anything. See `backends/sos/sos_plugin.sh` (`plugin_build`/`plugin_write`/`plugin_clean`) and `backends/dsos/dsos_plugin.sh` (`plugin_serve_start`/`plugin_serve_run`/`plugin_serve_stop`) for examples of both. If a hook reads a file that only a prior real (non-dry) step would have created (e.g. a token or session file), skip that read under `DRY_RUN` too — see `backends/influxdb/influxdb_plugin.sh`'s `plugin_write`.

Separately, `bin/run.sh` itself now always prints the outer `env VAR=val ... bash bin/node.sh ACTION ARGS` line a step runs (for every launcher, not just `-l manual`) before running it — see `README.md`'s "Dry run" section and `bin/run.sh`'s "Reproducing a step by hand" comment. That line already carries every setting `CONFIG_VARS`/`PLUGIN_VARS` forward, so it needs nothing from you as a plugin author; `run_logged`'s `VAR=val --` convention above is only for a setting an external command you invoke reads from its own environment, which that outer line does not help with on its own once you're inside `node.sh`.

### The query adapter: `query/<n>_query.py`

A Python module that defines one or more subclasses of `Adapter` from `core/adapter.py`, plus two module-level attributes:

```python
ADAPTERS = {"<adapter-name>": AdapterClass, ...}
DEFAULT_ADAPTER = "<adapter-name>"
```

The engine loads this file by path, so nothing needs installing.

Every adapter instance receives `self.ctx`, a dict with these keys:

| Key | Contains | Example |
| --- | --- | --- |
| `target` | The storage target name, read from the plugin's ingest-handoff meta file at `meta/ingest-handoff-<writer>.json`. For SOS/DSOS this is the container name; for InfluxDB it is the measurement name. The query adapter does not need to know how the name was chosen | `monitoring_nested_c` |
| `schema` | The dataset name from the JSON definition (`DS.name`) — the table name used in query strings and the name verification matches against | `monitoring_nested` |
| `servers` | List of server hostnames from `SERVER_HOSTS` | `["node-1", "node-2"]` |
| `output_dir` | This pair's output directory (`OUTPUT_DIR/<pair>/`), with `meta/`, `nodes/`, `results/`, `logs/` subdirectories | `/data/monbench/output/sos-dsos` |
| `env` | The full environment (`os.environ`), including all settings from `bench.conf` and the backend's own config |  |

The distinction between `target` and `schema` matters: `target` is the backend-specific storage name (used for connecting and probing), while `schema` is the dataset's logical name (used in query text via `{schema}` and in the verification engine). For backends where these happen to be the same (like InfluxDB, where the measurement name is the dataset name), `schema` is the correct one for query-related work.

#### Adapter methods

Each adapter subclass must implement these methods:

| Method | Timed? | Purpose |
| --- | --- | --- |
| `connect()` | No | Connect and confirm the target holds data. Raise `BackendError` to stop the run |
| `query(text)` | Yes | Run one query and yield its result in batches |
| `count(batch)` | Yes | Return the number of rows in a batch. Default: `len(batch)` |
| `records(batch)` | No | Return `(columns, rows)` where rows are dicts keyed by attribute name. Used for verification only |
| `postprocess(text, columns, rows)` | No | Optional. Backend-specific shaping of a query's full, assembled result (after `records()` and column-map reversal), for verification only. Default: no-op (`return rows`). See `core/adapter.py`'s docstring and `backends/sos/query/sos_query.py` for a concrete use: collapsing a list-attribute query's possible multiple rows per node sample into one, since the SOS/DSOS query language has no JOIN/DISTINCT to do it in the query text itself |
| `close()` | No | Release the connection |

The engine times each run from the call to `query()` until the last batch has been counted. Do only what a normal client of the backend would do inside `query()`, so timings stay comparable.

#### Class attributes

Every adapter has two identity attributes and one optional translation attribute:

| Attribute | Purpose |
| --- | --- |
| `name` | The adapter's name, used as the `QUERY_ADAPTER` setting and as a key in query files |
| `dialect` | The query language (e.g. `"sql"`, `"influxql"`, `"dsos"`). Query files can key on this instead of the adapter name, so adapters that share a language share query strings |
| `type_overrides` | A dict mapping dataset attribute types to backend-specific conventions (see below) |

`dialect` can be set on the class for adapters that always use the same query
language, or set in `__init__` for backends that support more than one
language at runtime. For example, the InfluxDB adapter sets `dialect = None`
on the class and then sets `self.dialect = self.lang` in `__init__` based on
`INFLUX_QUERY_LANG` (which can be `"sql"` or `"influxql"`), so query files
can key strings on either `"sql"` or `"influxql"` and the right one is
selected at runtime.

#### `type_overrides`

Databases differ in how they name and represent certain data types. Rather than requiring query authors to know each backend's conventions, the adapter declares its differences by attribute type, and the engine handles translation automatically.

`type_overrides` is a dict keyed by dataset attribute type (e.g. `"timestamp"`, `"uint64"`, `"string"`). Each value is a dict with two optional keys:

| Key | What it does | Example |
| --- | --- | --- |
| `"column"` | Renames every dataset attribute of this type to the backend's column name in query text, and reverses the rename in result columns | InfluxDB stores the timestamp as `time` regardless of what the dataset calls it |
| `"literal"` | Wraps placeholder values of this type with a format string so the generated SQL uses the backend's native literal syntax | InfluxDB needs `to_timestamp_seconds(1700000000)` instead of a bare integer |

The engine resolves which attributes are affected using the dataset definition's `attributes` list. Translation happens outside the timed region — before query text is finalized and after result columns are returned — so it never contaminates benchmark measurements.

Example from the InfluxDB adapter:

```python
class InfluxPythonAdapter(Adapter):
    name = "influxdb"
    dialect = None
    type_overrides = {
        "timestamp": {"column": "time", "literal": "to_timestamp_seconds({})"},
    }
```

With this declaration and a dataset attribute `{"name": "sample_time", "type": "timestamp"}`, the engine would rename `sample_time` to `time` in queries and wrap its placeholder values with `to_timestamp_seconds()` — with no change to any query string. Backends with no type differences (like SOS/DSOS) leave `type_overrides = None`.

### Testing a new backend

Test incrementally by running phases individually with the SSH launcher. The `bin/run.sh` command accepts a phase name as its last argument:

```bash
# 1. Start the service (if PLUGIN_SERVICE=1)
bin/run.sh -c bench-mydb.conf -l ssh serve-write

# 2. Write the data and confirm the writer's JSON summary
bin/run.sh -c bench-mydb.conf -l ssh write

# 3. Run queries without verification to confirm connectivity
bin/run.sh -c bench-mydb.conf -l ssh query

# 4. Run queries with verification to confirm correctness
bin/run.sh -c bench-mydb.conf -l ssh check

# 5. Run the full sequence including timing
bin/run.sh -c bench-mydb.conf -l ssh all

# 6. Clean up (stops the service first)
bin/run.sh -c bench-mydb.conf -l ssh clean
```

Start with a small dataset (`SAMPLES=100 COMPONENT_IDS_PER_SERVER=4`) to iterate quickly. Increase once the queries pass verification.

`bin/run.sh -d` (see `README.md`, "Dry run") is also useful while wiring up a new plugin: it shows you the exact command each hook would run on each node without needing the backend's service, or even the writer binary, to actually work yet — see "Supporting `bin/run.sh -d` (dry run)" above for what your plugin needs for this to work, and for how to make the logged line copy-pasteable as a standalone command, not just a record of what ran.

## Defining a custom dataset

The default dataset (`dataset/default.json` + the value rules in
`core/dataset.py`) is a monitoring shape: one record per component per
sample, with attribute names and units following the flat (non-record)
metrics of the `curated_metrics` LDMS sampler plugin. To benchmark a
different data shape, you define a new dataset.

A dataset has two parts:

### The definition file: `dataset/<n>.json`

A JSON file that lists the attributes, their types, and the access keys. The
default dataset's own `dataset/default.json` looks like this (trimmed to a
few attributes for illustration — the real file has 19 metric attributes;
see `dataset/default.json` for the full list):

```json
{
  "name": "monitoring",
  "description": "...",
  "attributes": [
    { "name": "timestamp",    "type": "timestamp" },
    { "name": "component_id", "type": "uint64" },
    { "name": "job_id",       "type": "uint64" },
    { "name": "seq",          "type": "uint64" },
    { "name": "MemTotal",     "type": "uint64" },
    { "name": "free",         "type": "double" },
    { "name": "load1min",     "type": "double" }
  ],
  "keys": [
    { "name": "timestamp",     "attrs": ["timestamp"] },
    { "name": "comp_time",     "attrs": ["component_id", "timestamp"] },
    { "name": "job_comp_time", "attrs": ["job_id", "component_id", "timestamp"] }
  ]
}
```

| Field | Purpose |
| --- | --- |
| `name` | The schema name; used in query strings as `{schema}` and as the record type the verifier matches against. This is also the single source of truth for the logical data name — the core never constructs storage names from it; that is the plugin's job |
| `attributes` | The columns. `name` is how queries and the verifier refer to each one. `type` is used by `type_overrides` to apply per-backend column renaming and literal formatting |
| `keys` | The access paths the data is meant to be queried by; each backend maps them onto its own indexing (e.g. SOS creates an index for each, InfluxDB maps tags) |

Any entry in `attributes` may instead be a **list attribute**: a per-sample
nested list, identified by its own name like any other attribute, with
`"type": "list"`, a per-sample length (`min_list_length`/`max_list_length`,
or the `list_length` fixed-length shorthand, plus an optional
`length_stride`), and an `entries` spec that is either
`{"type": "record", "tuples": [...]}` (a flat record of named scalar
fields) or a bare scalar type spec (just `{"type": ..., ...}`, with no
`name` of its own) — see `dataset/curated_metrics_like.json` for a worked
example (`cpu`, `netdev`) and "List attributes" below for the full
declaration format and generation algorithm. This is generated and verified
generically by `core/dataset.py`/`core/valuegen.py` and `core/bench.py`
with no code change needed per dataset; whether a *backend* can write and
natively query a given dataset's list attributes is a separate, per-backend
question (see "Writing a new backend" above and `STATUS.md` for which
backends currently do).

Set `DATASET` in `bench.conf` to point to your definition file:

```bash
DATASET="${BENCH_DIR}/dataset/my-shape.json"
```

### The value rules: `core/dataset.py` / `core/valuegen.py`

Every attribute's value is a deterministic, hash-based function of its
identifying coordinates (dataset name, attribute name, list name/entry
index, node id, job id, sample sequence). There is no per-attribute
hand-tuned formula and no AST/expression language — a dataset is defined
as pure JSON data, and `core/valuegen.py` is the one evaluator every writer
and the verifier both call.

This module has a bit-identical C port, `core/valuegen.c` /
`core/valuegen.h`, for writers that cannot import Python (see
`backends/sos/writer/sos_write.c`). Any change to the hash, key
construction, or decode rules must be mirrored there —
`core/valuegen_cross_check.py` exercises both implementations over the same
inputs and fails loudly on any divergence.

No per-dataset code is needed: any dataset JSON under `dataset/` is
generated and verified by the same `core/valuegen.py` and
`core/dataset.py`/`core/dataset.c`. Every Python writer and `core/bench.py`
itself call `Dataset.expected()`/`Dataset.expected_lists()` directly, so
they pick up a new dataset automatically; `backends/sos/writer/sos_write.c`
likewise calls into `core/dataset.c`/`core/valuegen.c` rather than
reimplementing anything.

#### Vocabulary

| Term | Meaning |
|---|---|
| **node** | A compute node — what `component_id` means in code, data and every other doc. Reserved as `component_id` rather than renamed, because that name is fixed by LDMS convention |
| **list attribute** | An attribute whose value is a variable-length, per-sample list (see "List attributes" below). Identified by its own `name`, exactly like any other attribute — there is no separate "kind" concept |
| **job** | A job running on the system |
| **job rank** | A rank (e.g. an MPI rank) within a job. Name reserved, support deferred — no current metric needs it |

Hierarchy: **server** (writer) → **node**, with **job** as an orthogonal
dimension that cuts across nodes. A node's sample may include list
attributes — e.g. one entry per CPU or network interface — but these are
not a separate tier; they are ordinary attributes whose `type` happens to
be `"list"`.

This vocabulary is consistent with the shell/config layer:
`SERVER_HOSTS` (physical writer machines), `COMPONENT_IDS_PER_SERVER`
(monitored entities per writer), `QUERY_HOST` (query target).

#### Motivation

The previous design hardcoded a fixed `ATTRS` tuple and a literal formula
per attribute in `Dataset.expected()`, plus a hand-translated C
re-implementation in `sos_write.c`. Defining a new dataset meant editing
`core/dataset.py` *and* hand-updating every non-Python writer. The hash-
based scheme removes both problems: the value rules are driven entirely by
a dataset's own JSON declaration, and `valuegen.py`/`valuegen.c` is the one
evaluator. A second motivation: removing a redundant "component kind" concept
for nested lists that collided with the LDMS-reserved `component_id` — a
list attribute is now just an attribute whose `type` is `"list"`, identified
by its own `name`.

#### Key construction

For a given attribute, build a byte string from its identifying
coordinates. Unused coordinates (e.g. list name/entry index for a
node-level attribute, or the tuple-field name for a bare-scalar list
entry) are fixed to an empty-string / `0` sentinel:

```
K = dataset_name || 0x00 || metric_name || 0x00 || list_name || 0x00
  || le64(node_id) || le64(entry_index)
  || le64(job_id)  || le64(floor(seq / stride))
```

- `le64(x)`: 8-byte little-endian encoding of a 64-bit unsigned integer.
- `list_name`: the declaring list attribute's own `name` (e.g. `"cpu"`,
  `"netdev"`), empty string for a node-level (non-list) attribute.
  Included so that two list attributes can reuse the same tuple-field
  name (e.g. both `"cpu"` and `"gpu"` having a `util` field) without
  colliding.
- `metric_name`: the attribute's own name for a flat or record-tuple
  attribute; the empty-string sentinel for a list whose entries are a
  bare scalar (there is no sub-field name in that case — the entry *is*
  the value). This is what makes different attributes of the same record
  statistically independent of each other (see "Distinguishability" in
  the design rationale below).
- `entry_index`: 0-based position within the list; `0` when not applicable.
- `stride` (default 1): lets an attribute be "sticky." `stride = s` means
  the value only changes every `s` samples. A very large stride (or an
  explicit `"constant": true` flag) makes the value depend only on
  identity, never on `seq`.

This key shape is generic: it has exactly one list-entry slot, and a
dataset may declare any number of sibling list attributes, each using
this same `K` with its own `list_name`.

#### Hash function: FNV-1a, 64-bit

Chosen for being simple, fast, and trivially implementable identically in
Python and C (plain integer ops, no library dependency). Not a
cryptographic hash — none of this needs cryptographic properties, only
good mixing and speed, since the suite hashes on the write-performance
critical path.

```
FNV_OFFSET = 0xcbf29ce484222325
FNV_PRIME  = 0x100000001b3
MASK64     = 2^64 - 1

fnv1a64(b):                      # b: byte string
    h = FNV_OFFSET
    for byte in b:
        h = h XOR byte
        h = (h * FNV_PRIME) AND MASK64
    return h
```

For attributes needing more than 64 bits of output (e.g. long strings),
chain blocks with a counter:

```
digest(K, i) = fnv1a64(K || le64(i)),   i = 0, 1, 2, ...
```

#### Decoding a digest into a typed value

Let `D0 = digest(K, 0)`.

```
uintN:  value = lo + (D0 mod (hi - lo + 1))
intN:   value = lo + (D0 mod (hi - lo + 1))     # same form, signed default range
```

**`double` — scaled integer.** `double` is not a separate primitive; it is
a display transform over an integer decode:

```
double: base_value = uintN_decode(D0, attr.base or uint64, attr.range or default_range(base))
        value = base_value / attr.scale + attr.offset
                 # scale default 100.0, offset default 0.0
```

Default range table (admin may override any field per attribute):

| Type | Default range / shape |
|---|---|
| `uint8` | `[0, 255]` |
| `uint16` | `[0, 65535]` |
| `uint32` | `[0, 2^32 - 1]` |
| `uint64` | `[0, 2^64 - 1]` |
| `int8` | `[-128, 127]` |
| `int16` | `[-32768, 32767]` |
| `int32` | `[-2^31, 2^31 - 1]` |
| `int64` | `[-2^63, 2^63 - 1]` |
| `string` | length 8, alphabet `a-z0-9` |
| `double` | base `uint64` range `[0, 9999]`, `scale = 100.0`, `offset = 0.0` → value in `[0.00, 99.99]` |

**String decode** (length `L`, alphabet `Σ`, `|Σ|` symbols):

```
string(K, L, Σ):
    chars = []
    i = 0
    while len(chars) < L:
        bytes = le64(digest(K, i))        # 8 bytes
        for byte in bytes:
            if len(chars) == L: break
            chars.append(Σ[byte mod |Σ|])
        i = i + 1
    return join(chars)
```

A `string`-typed list entry uses exactly this decode — nothing
list-specific about it; `list_name`/`entry_index` just feed into `K` the
same way they do for any other list-entry attribute.

#### Full evaluator

```
evaluate(attr, dataset_name, node_id, list_name, entry_index,
         job_id, seq):
    K = key_bytes(dataset_name, attr.name, list_name,
                   node_id, entry_index, job_id, seq, attr.stride or 1)
    return decode(attr, K)
```

This same function is called for every flat attribute, every record-tuple
field, and every bare scalar list entry a dataset declares — nothing
about it is specific to a particular list.

#### Design rationale

The hash-based scheme preserves three properties the original hand-written
formulas had, without hand-tuning:

**Selectivity.** `D0` is effectively uniform over its declared range for a
given `(metric_name, ...)`, so a numeric condition like `free < 50` matches
a realistic, non-contiguous fraction of rows — exercising a backend's
filter rather than letting it exploit a trivial pattern (e.g. one contiguous
run).

**Distinguishability.** Because `metric_name` (and, for list-entry
attributes, `list_name`) is part of `K`, two attributes evaluated at the
same `(node_id, seq, ...)` get statistically unrelated digests — there is
no algebraic relationship between them. A column-swap, truncation, or
type-coercion bug is still caught by verification, because nothing about
one attribute's value predicts another's.

**Compression resistance.** Several backends (InfluxDB especially, SOS's
on-disk structures to a lesser extent) compress columns at write time —
delta encoding, run-length encoding, dictionary encoding. Trivially regular
synthetic values (monotonic counters, repeated values, values strongly
correlated with row position) let a compressor collapse them almost for
free: writes become artificially cheap, storage artificially small, and
scans artificially fast, none of which reflects how the backend performs
on real `curated_metrics` data, which varies unpredictably. Every timing the
suite reports (`write_s`, `records_per_s`, query latencies) is downstream
of how many real bytes had to move through I/O, so this bias affects every
measurement even though the suite doesn't report a compression ratio
directly. A hash-based value has no structure for a compressor to exploit,
so cross-backend comparisons stay about the engines, not about how
compressible the test data happens to be.

### List attributes

A dataset's list attributes are declared and generated generically: the
schema, key construction, and algorithm below support *any* set of list
attributes with *any* entry shape — a dataset with no lists, one list, or
ten; entries with 2 fields or 20, or no fields at all (a bare scalar).
`curated_metrics` is one concrete instance plugged into this generic
machinery, used because it is real evidence of what a monitoring dataset's
nested structure and data types look like — it is not what the schema or
algorithm are tailored to.

#### Declaration

There is a single `attributes` array; a list attribute is simply one
whose `type` is `"list"`. Its entries are either a flat record (scalar
tuple fields) or a bare scalar — including `string`. Length is variable per
sample, bounded by a declared min/max (or fixed via a `list_length`
shorthand), and these length properties — `min_list_length`,
`max_list_length`, `list_length`, `length_stride` — are properties of the
*list*, siblings of `entries`, not of any one entry field:

```json
{
  "name": "monitoring",
  "attributes": [
    { "name": "timestamp",    "type": "timestamp" },
    { "name": "component_id", "type": "uint64" },
    { "name": "job_id",       "type": "uint64" },
    { "name": "seq",          "type": "uint64" },

    { "name": "cpu",
      "type": "list",
      "min_list_length": 2,
      "max_list_length": 2,
      "length_stride": 1000000000,
      "entries": {
        "type": "record",
        "tuples": [
          { "name": "cpu_name", "type": "string" },
          { "name": "idle",     "type": "double" },
          { "name": "sys",      "type": "double" },
          { "name": "user",     "type": "double" },
          { "name": "iowait",   "type": "double" }
        ]
      }
    },

    { "name": "netdev",
      "type": "list",
      "min_list_length": 1,
      "max_list_length": 2,
      "entries": {
        "type": "record",
        "tuples": [
          { "name": "iface_name", "type": "string" },
          { "name": "rx_bytes",   "type": "uint64" },
          { "name": "tx_bytes",   "type": "uint64" }
        ]
      }
    },

    { "name": "recent_latencies_us",
      "type": "list",
      "list_length": 5,
      "entries": { "type": "uint32", "range": [0, 5000] }
    }
  ]
}
```

- `min_list_length` / `max_list_length`: the length for a given
  `(node_id, job_id, seq-bucket)` is hash-derived (see "Generation
  algorithm" below) and falls in `[min_list_length, max_list_length]`,
  inclusive.
- `list_length`: shorthand for `min_list_length == max_list_length ==
  list_length` — how an admin gets a fixed length, if that is all they
  need. Omitting all three defaults to a fixed length of 1.
- `length_stride` (default: a very large constant, i.e. effectively fixed
  for the run): like an attribute's own `stride` (see "Key construction"),
  controls how often the *length* is allowed to change — independent of any
  entry field's own `stride`, which controls how often that field's *value*
  changes. A real interface list, for instance, rarely changes length
  within one run, while an interface's own counters still change every
  sample.
- `entries`: either `{"type": "record", "tuples": [...]}` (each tuple
  exactly like a flat attribute, minus list-level fields — a tuple's own
  `type` cannot be `"list"`; see "Scope limitation" below) or a bare
  scalar type spec (`{"type": "uint32", "range": [...]}`,
  `{"type": "string", ...}`, etc.).

The third example above (`recent_latencies_us`) is a list of scalars: no
record wrapper, no sub-field name — the list's own `name` and each
entry's index are all the identity a value needs.

See `dataset/curated_metrics_like.json` for the real worked example this
schema produces (also declaring `rx_packets`/`tx_packets` on `netdev`).

#### Identifiers

`list_name` is the declaring attribute's own `name`, carried into `K` as
in the key construction above. `entry_index` is a plain 0-based position
within that list, up to (length − 1) for the current sample — not hashed
itself, just the loop variable in the generation algorithm.

#### Generation algorithm

```
list_length(dataset_name, list_attr, node_id, job_id, seq):
    K_n = dataset_name || 0x00 || "__length__" || 0x00 || list_attr.name || 0x00
        || le64(node_id) || le64(job_id)
        || le64(floor(seq / list_attr.length_stride))
    span = list_attr.max_list_length - list_attr.min_list_length + 1
    return list_attr.min_list_length + (fnv1a64(K_n) mod span)

generate_node_sample(dataset, node_id, job_id, seq):
    sample = {}
    for attr in dataset.attributes:
        if attr.type != "list":
            sample[attr.name] = evaluate(attr, dataset.name, node_id,
                                           "", 0, job_id, seq)
            continue
        n = list_length(dataset.name, attr, node_id, job_id, seq)
        sample[attr.name] = []
        for i in range(n):
            if attr.entries.type == "record":
                entry = {}
                for field in attr.entries.tuples:
                    entry[field.name] = evaluate(field, dataset.name, node_id,
                                                  attr.name, i, job_id, seq)
            else:
                entry = evaluate(attr.entries, dataset.name, node_id,
                                  attr.name, i, job_id, seq)
            sample[attr.name].append(entry)
    return sample
```

Deterministic and order-independent: every `(node_id, job_id, seq)`
produces the same length and the same attribute values regardless of
write order, exactly like the flat case — including the length itself,
which a verifier can recompute rather than having to read back and count
entries.

Nothing in this algorithm names `cpu` or `netdev`, or treats list
attributes as a separate category from flat ones beyond the single
`attr.type == "list"` branch; a dataset with one list, five lists, or
none runs through the same loop.

#### Resulting shape (curated_metrics as a worked instance)

Plugging the declaration above (modeled on `curated_metrics`'s `cpu` and
`netdev` record types) into the generation algorithm, one sample looks
like:

```json
{
  "timestamp": ..., "job_id": ..., "seq": ...,
  "cpu": [
    { "cpu_name": "cpu0", "idle": 71.22, "sys": 8.91, "user": 12.08, "iowait": 1.03 },
    { "cpu_name": "cpu1", "idle": 65.90, "sys": 7.44, "user": 18.41, "iowait": 2.10 }
  ],
  "netdev": [
    { "iface_name": "eth0", "rx_bytes": ..., "tx_bytes": ... }
  ]
}
```

This is one instance of the generic shape — the node has two list
attributes (`cpu`, `netdev`), each a list of records, each with its own
independently variable length. A different dataset declaration (say, a
GPU list with power/temperature tuples, or a bare scalar list, or no
lists at all) produces the same shape of result through the same
algorithm.

#### Verification model and backend mapping

**Verification is recomputation, not storage.** For any attribute
occurrence identified by its coordinates

```
C = (dataset_name, node_id, list_name, entry_index, job_id, seq)
```

define

```
expected(attr, C)                          = evaluate(attr, C)
expected_length(list_attr, node_id, job_id, seq) = list_length(list_attr, ...)
```

No expected-value table is ever stored or shipped alongside a dataset —
the verifier holds only the dataset declaration (the same JSON used to
generate) and recomputes both functions directly.

**Condition grammar**, generic over any declared list attribute:

```
condition    := node_cond | list_cond | length_cond
node_cond    := attr cmp value
list_cond    := name '[' idx ']' ('.' attr)? cmp value
idx          := INT | '*'
length_cond  := 'count(' name ')' cmp INT
cmp          := '==' | '!=' | '<' | '<=' | '>' | '>='
value        := NUMBER | STRING_LITERAL
```

`.attr` is required when `name`'s `entries.type == "record"`, and
forbidden when `entries` is a bare scalar — the entry *is* the value in
that case. Semantics:

```
match(node_cond, C)                 = (evaluate(attr, C) cmp value)
match(name[i].attr cmp v, C)        = i < expected_length(name, C) AND evaluate(attr, C_i) cmp v
match(name[i] cmp v, C)             = i < expected_length(name, C) AND evaluate(entries, C_i) cmp v
match(name[*].attr cmp v, C)        = ∃ i ∈ [0, expected_length(name, C)) : evaluate(attr, C_i) cmp v
match(name[*] cmp v, C)             = ∃ i ∈ [0, expected_length(name, C)) : evaluate(entries, C_i) cmp v
match(count(name) cmp n, C)         = expected_length(name, C) cmp n
```

`STRING_LITERAL` needs no new machinery beyond `==`/`!=`: `evaluate`
already produces a string for `string`-typed attributes, so string
equality is just that decoded value compared to a literal — the same
comparison form as a number, and this applies identically whether the
string is a flat attribute, a record-tuple field, or a bare scalar list
entry.

**Backend schema mapping**, generic per declared list attribute:

| Level | SOS/DSOS | InfluxDB |
|---|---|---|
| node | existing node schema, keyed `(component_id, job_id, seq)` | existing measurement, tags `component_id, job_id` |
| each list attribute | one schema per list, columns = the list's entry fields (or one value column for a bare-scalar list) + `component_id, job_id, seq, entry_index`; join index on `(component_id, job_id, seq)` | one measurement per list, tags `component_id, job_id, entry_index`, fields = the list's entry fields (or one value field) |

This is the same shape the node table already uses — one schema/
measurement per declared list attribute rather than hardcoded ones for
`cpu`/`netdev`. Translating the grammar into native queries:

- `name[i].attr cmp v` / `name[i] cmp v` → `WHERE entry_index = i AND
  attr cmp v` (or `WHERE entry_index = i AND value cmp v` for a
  bare-scalar list) on that list's table/measurement, joined to the node
  row by `(component_id, job_id, seq)`.
- `name[*]...` → `WHERE attr cmp v` (or `WHERE value cmp v`) on that
  list's table — no `EXISTS` subquery needed; a matching row's existence
  *is* the existential, since row cardinality alone encodes it.
- `count(name) cmp n` → either `SELECT COUNT(*) ... WHERE component_id=?
  AND job_id=? AND seq=?` against the backend, or, since
  `expected_length` is a pure recomputation, the verifier can check this
  one without touching the backend at all.

**Type coverage check against curated_metrics.** Every field type
`curated_metrics.c` actually uses is already covered by the type table
above: flat scalar counters (`free`, `MemTotal`, ...) → `uint64`/`int64`;
`cpu`/`netdev` name fields (`cpu_name`, `iface_name`) → `string`;
percentage-style fields (`idle`, `sys`, `user`, `iowait`) → `double`
(scaled-integer decode); byte/packet counters (`rx_bytes`, `tx_packets`,
...) → `uint64`. Nothing new was needed in the type system to cover a
real sampler's output.

**Implementation status, per backend** (see `STATUS.md` for the full
record and `backends/<name>/README.md` for each backend's own mapping
details): InfluxDB, SOS and DSOS all write list-attribute entries and
answer `name[i]...`/`name[*]...` as native queries (SOS/DSOS via a plain
select on the per-list schema plus `Adapter.postprocess()`-based dedup for
the existential form, since the SOS query language has no
JOIN/DISTINCT). `count(name)` is native only on InfluxDB; SOS/DSOS have
no `GROUP BY`/`HAVING`-style count filter, so that condition form reports
SKIP under those backends.

#### Scope limitation: one level of list nesting only

This design supports exactly **one level** of list nesting below a node:
`node → list[]`, where each entry is a flat record of scalar attributes
or a bare scalar. It does **not** support an entry itself containing a
nested list.

This is enforced structurally, not just documented: a record entry's
tuple fields are declared with the same attribute grammar as a flat
attribute, and that grammar's `type` excludes `"list"` for a tuple field.
A dataset JSON that tried to declare `cpu`'s `idle` tuple as `"type":
"list"` is rejected by the loader, not merely discouraged by this
section.

This is a deliberate boundary, not an oversight:

- It matches how LDMS itself represents this data. Reading
  `curated_metrics.c`'s `create_metric_set()` confirms each record type
  (`cpu`, `netdev`) is built from scalar fields only
  (`ldms_record_create` + `ldms_record_metric_add`, never a nested
  record), and is attached to the schema as one variable-length list
  (`ldms_schema_metric_list_add`). Going to a list-of-lists would need a
  shape that is not something `curated_metrics` — or, as far as this
  design has checked, LDMS's record/list API — produces.
- It keeps the key construction, the generation algorithm, the verifier
  grammar, and the backend schema mapping all one level deep, with no
  third hash-key dimension and no third level of indexing to design,
  implement, and keep consistent across backends.

**Consequence, accepted as a limitation:** the suite cannot benchmark a
dataset whose true shape is two-level-nested. If a future dataset
genuinely needs that, it is out of scope for this design and would need a
separate extension — one that would have to introduce a new grammar
construct, since today's grammar makes the shape inexpressible rather
than merely unimplemented.

#### Resolved design questions

1. **Default string length/alphabet** — 8 characters, alphabet `a-z0-9`
   (`valuegen.py`'s `DEFAULT_STRING_LENGTH`/`DEFAULT_STRING_ALPHABET`,
   mirrored in `valuegen.h` as `VG_DEFAULT_STRING_LENGTH`/
   `VG_DEFAULT_STRING_ALPHABET`).
2. **Default `min_list_length`/`max_list_length`/`length_stride`** when a
   dataset's JSON omits them — `list_length()` falls back to a fixed
   length (`list_length`, default 1, i.e. `min == max`) and a very large
   `length_stride` (`DEFAULT_LENGTH_STRIDE = 10**15` in Python,
   `VG_DEFAULT_LENGTH_STRIDE` in C), so a dataset that doesn't care about
   variable lengths gets a fixed-length list with no extra configuration.
   `list_length` is the one documented spelling.
3. **Job rank** — name reserved in the vocabulary; no implementation
   planned. A list attribute whose entries themselves need to be lists
   remains out of scope per the scope limitation above.
4. **Vocabulary rename.** `component_kind`/`component_index`/
   `component_count()` → `list_name`/`entry_index`/`list_length()`,
   `Dataset.components()` → `Dataset.component_ids()`, etc. The
   shell/config layer's `SERVER_NODES`/`COMPONENTS_PER_NODE`/`QUERY_NODE`
   → `SERVER_HOSTS`/`COMPONENT_IDS_PER_SERVER`/`QUERY_HOST`.

## Writing a query file

A query file is a JSON file that the benchmark engine runs against any
backend that has query strings for its questions. Copy `queries/default.json`
as a starting point.

### Structure

```json
{
  "description": "...",
  "queries": [
    {
      "name": "one component, 25% of the time span",
      "type": "component+time",
      "match": {
        "where": "component_id == {comp_s0} and timestamp >= {t_q1} and timestamp < {t_mid}",
        "order_by": ["component_id", "timestamp"]
      },
      "query": {
        "dsos": "select * from {schema} where component_id == {comp_s0} and timestamp >= {t_q1} and timestamp < {t_mid} order_by comp_time",
        "influxdb": "SELECT * FROM {schema} WHERE component_id = {comp_s0} AND timestamp >= {t_q1} AND timestamp < {t_mid} ORDER BY component_id, timestamp"
      }
    }
  ]
}
```

### Per-query fields

| Field | Required | Purpose |
| --- | --- | --- |
| `name` | Yes | Label for the report |
| `type` | No | Category label (e.g. `"component+time"`, `"job"`); appears in the report alongside the name |
| `match` | No | The question in backend-neutral terms, used for verification (see below). Without it, the query is run, timed, and reported as INFO |
| `query` | Yes | Query strings keyed by adapter name or dialect. A plain string is used for every adapter. An adapter with no string for a question reports it as SKIP |
| `verify` | No | `false` to run and time the query without verifying it. Default `true` |

### The `match` description

`match` describes what the question should return, independently of any
backend's query language:

| Key | Purpose |
| --- | --- |
| `where` | Conditions joined by `and`, with op one of `==`, `!=`, `<`, `<=`, `>`, `>=`. No `or`, no aggregates. Three condition forms, generic over any attribute or declared list attribute the dataset defines (see "Verification model and backend mapping" above): a flat condition `<attribute> <op> <value>`; a list condition `<name>[<i>].<attribute> <op> <value>` (a specific entry instance) or `<name>[*].<attribute> <op> <value>` (true if any entry of that list matches) — `.{attribute}` is omitted for a list whose entries are a bare scalar (the entry's own value is compared directly); and a cardinality condition `count(<name>) <op> <n>`. `<value>` is a number, or a quoted string (`'...'` or `"..."`) for a `string`-typed attribute |
| `order_by` | List of attribute names the result must be ordered by |
| `limit` | Maximum number of rows (the verifier accepts both whole-result and per-server semantics) |

List conditions describe the question in backend-neutral terms for
verification; a backend also needs its own native query against however it
stores that list attribute to actually answer the question (see the backend
schema mapping in "Verification model and backend mapping" above).
**InfluxDB, SOS and DSOS all have this now**: a list attribute is written to
its own measurement or schema (`backends/influxdb/README.md`'s and
`backends/sos/README.md`'s "List attribute data model mapping" sections),
and a native query answers `name[i].attr`/`name[*].attr` against it —
InfluxDB with a SQL join back to the node measurement on
`(component_id, timestamp)`; SOS/DSOS (whose query language has no JOIN) by
selecting straight from the per-list-attribute schema, with the existential
`name[*].attr` form's possible multiple-rows-per-sample collapsed by the
query engine's `Adapter.postprocess()` hook (`core/adapter.py`) rather than
in the query text — see `queries/curated_metrics.json` for worked examples
against `dataset/curated_metrics_like.json`, each with `"influxdb"` and
`"dsos"` query strings. `count(name)` is native only on InfluxDB: SOS's
query language has no `GROUP BY`/`HAVING`-style count filter, so that one
condition form still has no native query string for the `sos`/`dsos`
adapters and reports SKIP (see `STATUS.md`). Every list attribute's entry
attribute type maps onto a real SOS schema type (`backends/sos/
make_schema.py`'s `sos_attr()`): `string` becomes a fixed-length
CHAR_ARRAY and `uint8`/`int8` widen to the closest wider SOS integer
type SOS has (`UINT16`/`INT16`) — nothing is dropped from the schema.

Backend-specific choices — which index to use, SQL syntax differences,
function calls — belong in the query string only, not in `match`.

### Keying query strings

The `query` dict can be keyed by adapter name (e.g. `"dsos"`, `"influxdb"`,
`"influxdb-http"`) or by dialect (e.g. `"sql"`, `"dsos"`). The engine looks
up the adapter's name first, then its dialect. This means:

- Adapters that share a dialect share query strings. The `dsos`, `dsosql` and
  `sos` adapters all use dialect `"dsos"`, so one `"dsos"` string covers all
  three.
- An adapter that sets its dialect at runtime (like InfluxDB, where the
  dialect is `"sql"` or `"influxql"` depending on `INFLUX_QUERY_LANG`) picks
  up strings keyed to either language.
- If both the adapter name and dialect have a string, the adapter name wins.

### Placeholders

Query strings and `match` use `{placeholder}` syntax. Placeholders are
filled from the dataset so that one file works at any data-set size:

| Placeholder | Value |
| --- | --- |
| `{schema}` | Schema name from the dataset definition |
| `{limit}` | `LIMIT_N` from `bench.conf` |
| `{t_start}`, `{t_end}` | Timestamps of the first and last sample |
| `{t_q1}`, `{t_mid}`, `{t_q3}` | Timestamps at 25%, 50% and 75% of the time span |
| `{t_mid_1pct}` | 1% of the time span after `{t_mid}` |
| `{comp_s0}`, `{comp_s0_last}` | First and last component on the first server |
| `{comp_slast}`, `{comp_last}` | First component on the last server; last component overall |
| `{job_first}`, `{job_mid}`, `{job_last}` | First job, the job at mid-span, last job |

A list attribute's measurement name is not a separate placeholder: write it
as `{schema}_<name>` directly in the query string (e.g. `{schema}_cpu`) —
the `{schema}` placeholder still expands normally, leaving the list
attribute's name suffix as literal text, exactly like
`queries/curated_metrics.json`'s queries do.

Placeholder values for types that have a `"literal"` entry in the adapter's
`type_overrides` are automatically wrapped with the backend's native syntax
(e.g. InfluxDB timestamps become `to_timestamp_seconds(...)`) — you write
bare `{t_mid}` in the query string, and the engine handles the wrapping.
Column renaming from `type_overrides` is applied to the query template before
placeholders are expanded, so that literal format strings that contain
dataset attribute names (e.g. `to_timestamp_seconds`) are not mangled. This
also means a dataset attribute name that is a plain-text substring of a
list attribute's measurement name (none of the current datasets have one)
would be rewritten too — not a concern today, but worth knowing if you name
attributes and measurements close together.

### Tips

- Start with a copy of `queries/default.json` and modify the questions.
- Add query strings for your backend under its adapter name or dialect.
- Test with `bin/run.sh check` (verification, no timing) before running
  `bin/run.sh query` (timing).
- Use `"verify": false` for queries the verifier cannot check (aggregates,
  grouping, queries with `or` conditions).
- The `type` field groups queries in the report; choose types that describe
  the conditions, not the backend.
