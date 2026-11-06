# Output files and timing boundaries

This document describes the files monbench produces, the structure of the
JSON result files, and exactly what each timing measurement covers.

For the full `OUTPUT_DIR` directory layout — where each file goes, what
each subdirectory is for, and the naming conventions plugins must follow —
see `EXTENDING.md`, "OUTPUT_DIR layout".


## Files

After a run, `OUTPUT_DIR/<pair>/` contains:

| Directory | Path pattern | Written by | Contents |
|---|---|---|---|
| `meta/` | `ingest-handoff-<writer>.json` | plugin (`plugin_write`) | Where the writer stored data (writer→query handoff) |
| `meta/` | `dataset-<writer>.json` | `core/dataset.py` | Dataset parameters and writer identity for verification |
| `results/` | `ingest-<writer>-<host>-raw.json` | each writer | One JSON object per writer run on that host |
| `results/` | `benchmark-<writer>-<adapter>.json` | `core/egress.py` | Combined benchmark result (see structure below) |
| `results/` | `benchmark-live-<writer>.json` | `core/egress.py` | Live-poll result (see structure below) |
| `results/` | `egress-<adapter>-raw.json` | `core/egress.py` | Per-run raw query timings for downstream analysis (see below) |
| `logs/` | `<phase>-<host>.log` | the harness | Full stdout/stderr of every remote step |
| `logs/` | `benchmark-<writer>-<adapter>.log` | `core/egress.py` | Everything printed to stdout during the benchmark |


## Benchmark result JSON

The main result file, `results/benchmark-<writer>-<adapter>.json`, has this
structure:

```
{
  "writer":    "<writer name>",
  "target":    "<storage target from plugin meta>",
  "dataset":   { ... },           # data set parameters (see below)
  "servers":   ["host1", ...],    # server hosts used
  "dry_run":   true|false,        # true if DRY_RUN was set for this run (see below)
  "ingest":    { ... },           # write-side results (see below)
  "query":     { ... },           # query-side results (see below)
  "all_passed": true|false        # false if any check failed
}
```

The `writer` field is the writer name (e.g. `"c"`, `"py"`, `"influx"`).
The `target` field is the backend-specific storage target read from the
plugin's ingest-handoff meta file at `meta/ingest-handoff-<writer>.json` — for SOS/DSOS
this is the container name (e.g. `"monitoring_nested_c"`), for InfluxDB the
measurement name (e.g. `"monitoring_nested"`).

### `dataset`

The data set parameters used for this run.

| Field | Description |
|---|---|
| `partitions` | number of server partitions (one per server host) |
| `component_ids_per_partition` | component IDs (time series) per partition |
| `samples` | data points per component ID |
| `t0` | timestamp of the first sample (Unix seconds) |
| `interval` | seconds between consecutive samples |
| `job_len` | samples per job |

### `dry_run`

`true` when `DRY_RUN` was set for this `check`/`query` run (`bin/run.sh -d`,
forwarded as `DRY_RUN=1` — see `README.md`, "Dry run"). When `true`, every
entry in `query.results` has status `DRY`: each query's final text was
resolved and is reported, but `core/egress.py` never called
`adapter.connect()`, and never called `adapter.query()`/`count()`/`records()`
for any query. `ingest` is unaffected by this field — it reflects whatever
the most recent real `write` produced; a dry `query`/`check` run does not
write a new `ingest` section.

### `ingest`

Write-side results, computed by `core/stats.py` from the per-host writer
JSON files. Every derived number (rates, percentiles, wall time) is
computed in the core so that every backend is summarized the same way.

```
"ingest": {
  "backend": "sos",
  "hosts": [
    {
      "host":                  "server-01",
      "backend":               "sos",
      "writer":                "c",
      "storage":               "mmos",
      "records":               80000,
      "list_entries":          0,
      "errors":                0,
      "batch":                 100,
      "rate":                  0,
      "open_s":                0.01,
      "write_s":               1.23,
      "sync_s":                0.45,
      "close_s":               0.02,
      "durability":            "sos_container_commit(SOS_COMMIT_SYNC)",
      "records_per_s":         65040,
      "durable_records_per_s": 47619,
      "window_records":        100,
      "latency_s":      { "n": 800, "min": ..., "p50": ..., "p90": ..., "p95": ..., "p99": ..., "max": ..., "mean": ..., "stddev": ... },
      "throughput_per_s": { "n": 800, "min": ..., "p50": ..., ... }
    }
  ],
  "summary": {
    "hosts":                  2,
    "total_records":          160000,
    "total_list_entries":     0,
    "errors":                 0,
    "wall_basis":             "wall-clock",
    "write_wall_s":           1.31,
    "durable_wall_s":         1.76,
    "records_per_s":          122137,
    "durable_records_per_s":  90909,
    "host_records_per_s_min": 64000,
    "host_records_per_s_max": 65040,
    "host_imbalance":         1.02,
    "latency_s":              { ... },
    "throughput_per_s":       { ... }
  }
}
```

#### Per-host fields

| Field | Description |
|---|---|
| `records` | node-level records written |
| `list_entries` | list-attribute entries written (0 without list attributes) |
| `errors` | failed writes |
| `batch` | the batch size setting |
| `rate` | the rate setting |
| `open_s` | one-time setup: connect, open the container, schemas |
| `write_s` | first record → last commit returned (generation included) |
| `sync_s` | making the data durable and visible (0 if there is no such step) |
| `close_s` | teardown; part of no rate |
| `durability` | what `sync_s` guarantees for this backend, declared by the adapter |
| `records_per_s` | `records / write_s` |
| `durable_records_per_s` | `records / (write_s + sync_s)` |
| `window_records` | records per batch window (`batch`, or `records` if batch is 0) |
| `latency_s` | nine-number summary of per-window durations (seconds) |
| `throughput_per_s` | nine-number summary of per-window throughput (records/s) |

#### Summary fields

| Field | Description |
|---|---|
| `hosts` | number of writer hosts |
| `total_records` | sum of all hosts' records |
| `total_list_entries` | sum of all hosts' list entries |
| `errors` | sum of all hosts' errors |
| `wall_basis` | `"wall-clock"` (from host start-epoch timestamps) or `"slowest-host"` (fallback when start epochs are missing) |
| `write_wall_s` | wall-clock elapsed time for the write phase |
| `durable_wall_s` | wall-clock elapsed time including sync |
| `records_per_s` | `total_records / write_wall_s` |
| `durable_records_per_s` | `total_records / durable_wall_s` |
| `host_records_per_s_min` | slowest host's `records_per_s` |
| `host_records_per_s_max` | fastest host's `records_per_s` |
| `host_imbalance` | `max / min` host rate (null when no host reported a rate) |
| `latency_s` | nine-number summary of all hosts' windows pooled |
| `throughput_per_s` | nine-number summary of all hosts' windows pooled |

The pooled latency/throughput series hold every host's windows together:
they describe the rate each server achieved over a window, not a sum
across servers.

#### Nine-number summary

Every `latency_s` and `throughput_per_s` object, ingest and query alike,
has this shape (computed by `core/stats.py`):

```
{ "n", "min", "p50", "p90", "p95", "p99", "max", "mean", "stddev" }
```

A percentile is reported only when the series has enough samples for it
to be more than the maximum (see `stats.min_samples()`); otherwise it is
`null`. That threshold is 2 for p50, 10 for p90, 20 for p95 and 100 for
p99.

### `query`

Query-side results from the benchmark engine.

```
"query": {
  "backend":    "dsos",
  "adapter":    "dsos",
  "query_file": "queries/default.json",
  "mode":       "check",
  "repeat":     20,
  "results": [
    {
      "name":           "[all] all records",
      "status":         "PASS",
      "rows":           160000,
      "verified":       true,
      "query":          "SELECT ... WHERE ...",
      "times_s":        [0.123, 0.131, ...],
      "latency_s":      { "n": 20, "min": ..., "p50": ..., ... },
      "throughput_per_s": { "n": 20, "min": ..., "p50": ..., ... },
      "first_batch_s":  { "n": 20, "min": ..., "p50": ..., ... }
    }
  ]
}
```

| Field | Description |
|---|---|
| `mode` | `check` (verification only), `query` (timing only), or `both` |
| `repeat` | number of timed runs per query (from `REPEAT` in bench.conf) |
| **Per-query fields** | |
| `name` | query label from the query set file |
| `status` | `PASS`, `FAIL`, `INFO`, `SKIP`, or `DRY` |
| `rows` | number of rows returned |
| `verified` | `true` if the result was verified against the data set |
| `query` | the expanded query string that was executed |
| `times_s` | array of individual run durations in seconds (raw samples) |
| `latency_s` | nine-number summary of `times_s` (seconds) |
| `throughput_per_s` | nine-number summary of `rows / duration` for each run (rows/s) |
| `first_batch_s` | nine-number summary of time-to-first-batch for each run (seconds) |
| `detail` | present on `FAIL` entries; describes what went wrong |

When `mode` is `check`, the timing fields (`times_s`, `latency_s`,
`throughput_per_s`, `first_batch_s`) are absent. When `mode` is `query`,
`verified` is always `false`.

A `DRY` entry (top-level `dry_run` is `true` — see above) has `query`
present but no timing fields, `rows` is `null`, and `verified` is `false`:
the query's final text was resolved and reported, but never sent.


## Raw query output

The file `results/egress-<adapter>-raw.json` contains the
per-run raw measurements for every timed query, intended for downstream
analysis and recomputation of statistics outside the suite.

```
{
  "writer":     "c",
  "target":     "monitoring_nested_c",
  "backend":    "dsos",
  "adapter":    "dsos",
  "query_file": "queries/default.json",
  "repeat":     20,
  "servers":    ["server-01", "server-02"],
  "queries": [
    {
      "name":  "[all] all records",
      "type":  "all",
      "query": "SELECT ... WHERE ...",
      "rows":  160000,
      "runs": [
        { "run": 1,  "duration_s": 0.123456, "rows": 160000, "first_batch_s": 0.045678 },
        { "run": 2,  "duration_s": 0.131234, "rows": 160000, "first_batch_s": 0.042345 },
        ...
      ]
    },
    ...
  ]
}
```

| Field | Description |
|---|---|
| `writer` | the writer whose data was queried |
| `target` | backend-specific storage target |
| `backend` | query backend name |
| `adapter` | query adapter name |
| `query_file` | path to the query set file (`null` for live-poll) |
| `repeat` | number of timed runs per query |
| `servers` | server hosts used for the write phase |
| **Per-query fields** | |
| `name` | query label from the query set file |
| `type` | query type (e.g. `all`, `timeseries`, `job`, `latest`) |
| `query` | the expanded query string |
| `rows` | number of rows returned (consistent across runs) |
| `runs` | array of per-run measurements |
| **Per-run fields** | |
| `run` | 1-based run number |
| `duration_s` | total duration: query dispatch → last row fetched (seconds) |
| `rows` | rows returned in this run |
| `first_batch_s` | time until the first batch had been fetched (seconds) |

This file is written only when at least one timed query ran successfully.
It is not written for check-only mode or dry runs.


## Live result JSON

The live result file, `results/benchmark-live-<writer>.json`, uses the same
top-level structure as the standard benchmark result, but its query results
contain no timing fields. Instead, each entry is a pass/fail check on the
live-poll behaviour:

```
"query": {
  "backend":    "dsos",
  "adapter":    "dsos",
  "query_file": null,
  "mode":       "both",
  "repeat":     5,
  "results": [
    { "name": "rows never decrease",       "status": "PASS", "rows": null, "verified": true },
    { "name": "every answer valid",        "status": "PASS", "rows": null, "verified": true },
    { "name": "final count reached",       "status": "PASS", "rows": null, "verified": true },
    { "name": "query errors while writing", "status": "INFO", "rows": null, "verified": false }
  ]
}
```

| Check | Passes when |
|---|---|
| `rows never decrease` | the row count returned by each poll is >= the previous poll's count |
| `every answer valid` | every polled answer contains only valid, non-duplicated records (each row checked against the data set) |
| `final count reached` | the last poll returned the full data set size (all partitions × component IDs × samples) |
| `query errors while writing` | always INFO; reports how many polls returned an error (e.g. the target did not exist yet when the first poll ran) |

The `detail` field on each entry carries the specifics: poll counts for
`rows never decrease`, the number of invalid answers for `every answer
valid`, and the final vs expected count for `final count reached`.

Under `DRY_RUN`, `live` reports a single `"live poll"` entry with status
`DRY` instead of running any poll, and `dry_run` is `true` at the top level.


## What is timed

### Write side — core-timed on the server

The ingest engine (`core/ingest.py`, or `core/ingest.c` linked into a C
adapter) runs on each server host via ssh or srun. It owns the clock, the
write loop, batching and throttling, and writes the raw result file
`results/ingest-<writer>-<host>-raw.json` itself; the backend's ingest
adapter only calls its storage API. Every backend is therefore timed by the
same code, the way queries are timed by `core/egress.py`.

The engine times four phases, in this order:

| Field | What it covers |
|---|---|
| `open_s` | one-time setup: connect, open or create the container, add schemas |
| `write_s` | first record → the last `commit()` returns (record generation is included) |
| `sync_s` | `adapter.sync()`: making the data durable and visible to readers (`0` if the backend has no such step) |
| `close_s` | teardown only; reported, but part of no rate |

An adapter's `commit()` returns only after the backend has acknowledged the
batch, and nothing needed for completeness is left to `close()`. The raw file
also holds a `durability` string, declared by the adapter, saying what
`sync_s` guarantees for that backend (for example `os.sync()` for the SOS
Python adapter), so the durable figures can be read for what they are.

The summary derives `records_per_s = records / write_s` and
`durable_records_per_s = records / (write_s + sync_s)`.

Additionally, the raw file holds a list of checkpoints: `[[seconds, samples], ...]`,
one entry at the end of every batch (after its commit completed) and a
final entry when the write finished. `core/stats.py` uses these to compute
per-batch-window latency and throughput distributions (see the ingest
section above).

#### Measuring the harness floor

A backend's `write_s` includes work that is not the backend's: the engine
generates every record inside the write phase and runs the loop. The null
adapter stores nothing, so its `write_s` is that work alone, the floor under
every backend's `write_s`. There is one null adapter per engine, and a
backend is compared with the engine it runs on, because the two engines
generate at very different speeds (Python about 130-200 µs per record, C a
few µs).

Run the null adapter on the same host, with the same data set, `--components`,
`--samples` and `--batch` as the run being explained, and unthrottled
(`--rate 0`):

```bash
# Python engine (the floor for sos_write.py, dsos_write.py, influx_write.py)
DATASET=dataset/default.json python3 core/ingest.py --backend null --adapter null \
    --writer null --output /tmp/floor-py.json --node-index 0 \
    --components 16 --samples 5000 --batch 500

# C engine (the floor for sos_write)
make -C core ingest_null
DATASET=dataset/default.json core/ingest_null \
    --writer null --output /tmp/floor-c.json --node-index 0 \
    --components 16 --samples 5000 --batch 500
```

Then, from each raw file:

| Quantity | Calculation |
|---|---|
| Floor rate | `records / write_s` of the null run |
| Storage time of a backend run | `write_s` of the backend run − `write_s` of the null run on the same engine |
| Share of the backend's `write_s` that is harness | `write_s` (null) / `write_s` (backend) |
| Storage-only rate | `records / (write_s (backend) − write_s (null))` |

Notes on reading the result:
- Generation is deterministic, so the null run costs the same every time;
  repeat it and take the median, since it still varies with the host's load.
- The subtraction is an estimate. It leaves in what the adapter itself does to
  turn a record into the backend's format (for example building line
  protocol), which is the adapter's cost and belongs to the backend.
- Never compare a Python adapter's `write_s` with the C null run, or the
  reverse.
- This is a stopgap for measuring generation: a later change will time
  generation separately inside the engine and report it directly.

`QUERY_ADAPTER=null` does the same for egress in `query` mode: it returns no
rows, so it measures only the timing loop (`check` mode fails with it, as
there is no data).

### Query side — core-timed on the client

Query timing is done by the benchmark engine (`core/egress.py`), not by
the adapter. The function `timed_run()` brackets the measurement:

```python
def timed_run(adapter, text):
    begin = time.perf_counter()
    n = 0
    first = None
    for batch in adapter.query(text):
        n += adapter.count(batch)
        if first is None:
            first = time.perf_counter() - begin
    total = time.perf_counter() - begin
    return total, n, total if first is None else first
```

The timed region is: **query dispatch → last row fetched**, including all
network round trips, server-side processing, result deserialization, and
the `adapter.count(batch)` call inside the loop (deliberately included;
it is meant to be a trivial operation like `len(batch)`).

The third return value, **time to first batch**, is the duration from query
dispatch to the moment the first batch of results has been counted. A query
that returns no results at all reports its total duration as the first-batch
time.

The adapter's `connect()` and `close()` calls are outside the timed
region — connection setup is measured once, not per query.

Under `DRY_RUN` (see `dry_run` above), `timed_run()` is never called at
all: each query is reported with status `DRY` as soon as its text is
resolved, and `adapter.connect()` is never called either.

### Why they differ

Write timing is server-local because the writer runs on the server. Query
timing is client-perceived because the benchmark engine runs on the query
host and measures what a real client would experience. These are different
vantage points by design:

- **Writer self-timing** allows each backend to report its own internal
  phases (open, write, sync, close) with granularity that the harness
  cannot observe remotely.
- **Core-timed queries** ensure every adapter is measured the same way,
  from the same vantage point, regardless of backend internals. The
  timing reflects end-to-end latency including network and deserialization,
  which is what matters for query performance.
