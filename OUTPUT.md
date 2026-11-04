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
| `meta/` | `storage-<writer>.json` | plugin (`plugin_write`) | Where the writer stored data (writer→query handoff) |
| `meta/` | `dataset-<writer>.json` | `core/dataset.py` | Dataset parameters and writer identity for verification |
| `results/` | `write-<writer>-<host>.json` | each writer | The writer's raw output for that host: counts, phase timers and checkpoints (no derived numbers) |
| `results/` | `benchmark-<writer>-<adapter>.json` | `core/bench.py` | Combined benchmark result (see structure below) |
| `results/` | `benchmark-<writer>-live.json` | `core/bench.py` | Live-poll result (see structure below) |
| `logs/` | `<phase>-<host>.log` | the harness | Full stdout/stderr of every remote step |
| `logs/` | `benchmark-<writer>-<adapter>.log` | `core/bench.py` | Everything printed to stdout during the benchmark |


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
plugin's storage meta file at `meta/storage-<writer>.json` — for SOS/DSOS
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
resolved and is reported, but `core/bench.py` never called
`adapter.connect()`, and never called `adapter.query()`/`count()`/`records()`
for any query. `ingest` is unaffected by this field — it reflects whatever
the most recent real `write` produced; a dry `query`/`check` run does not
write a new `ingest` section.

### `ingest`

Write-side results. Writers report raw inputs only; `core/stats.py`
computes everything below from them, identically for every backend (see
"Statistics" at the end of this file).

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
      "batch":                 500,
      "rate":                  0,
      "open_s":                0.02,
      "write_s":               1.23,
      "sync_s":                0.45,
      "records_per_s":         65040,
      "durable_records_per_s": 47619,
      "window_records":        500,
      "latency_s":        { "n": 160, "min": ..., "p50": ..., "p90": ..., "p95": ..., "p99": ..., "max": ..., "mean": ..., "stddev": ... },
      "throughput_per_s": { "n": 160, "min": ..., "p50": ..., ... }
    }
  ],
  "summary": {
    "hosts":                    2,
    "total_records":            160000,
    "total_list_entries":       0,
    "errors":                   0,
    "wall_basis":               "wall-clock",
    "write_wall_s":             1.31,
    "durable_wall_s":           1.78,
    "records_per_s":            122137,
    "durable_records_per_s":    89888,
    "host_records_per_s_min":   61000,
    "host_records_per_s_max":   65040,
    "host_imbalance":           1.07,
    "latency_s":        { "n": 320, ... },
    "throughput_per_s": { "n": 320, ... }
  }
}
```

| Field | Description |
|---|---|
| `records_per_s` | records divided by `write_s` (per host), or by the wall time of the whole write phase (summary) |
| `durable_records_per_s` | the same, with the close/sync time included: the rate at which data reached stable storage |
| `write_wall_s`, `durable_wall_s` | earliest write start to latest write end across hosts, without and with the sync. `wall_basis` is `wall-clock` when every host reported `start_epoch_s` (the hosts' clocks are then assumed to be synchronized) and `slowest-host` otherwise |
| `host_imbalance` | fastest host's `records_per_s` divided by the slowest host's |
| `window_records` | records per window: the writer's `batch` |
| `latency_s` | summary of how long each window (one batch) took, in seconds |
| `throughput_per_s` | summary of the records per second achieved in each window |

The summary's `latency_s` and `throughput_per_s` pool every host's windows.
They describe what one server achieved over one batch, not a sum across
servers. A trailing batch smaller than `batch` is left out of the
distributions but counts in all totals.

Every summary (`latency_s`, `throughput_per_s`) has the same shape,
shared with the query statistics: `n`, `min`, `p50`, `p90`, `p95`, `p99`,
`max`, `mean`, `stddev`. A percentile is `null` when `n` is too small for
it to be more than the maximum: p50 needs 2 samples, p90 10, p95 20,
p99 100.

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
      "name":       "[all] all records",
      "status":     "PASS",
      "rows":       160000,
      "verified":   true,
      "query":      "SELECT ... WHERE ..."
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
| `latency_s` | summary of `times_s` (`n`, `min`, `p50`, `p90`, `p95`, `p99`, `max`, `mean`, `stddev`) |
| `throughput_per_s` | the same summary of `rows / time` for each run |
| `first_batch_s` | the same summary of the time until the first batch of each run had been fetched |
| `detail` | present on `FAIL` entries; describes what went wrong |

When `mode` is `check`, the timing fields (`times_s`, `latency_s`,
`throughput_per_s`, `first_batch_s`) are absent. When `mode` is `query`, `verified` is
always `false`.

A `DRY` entry (top-level `dry_run` is `true` — see above) has `query`
present but no timing fields, `rows` is `null`, and `verified` is `false`:
the query's final text was resolved and reported, but never sent.

## Live result JSON

The live result file, `results/benchmark-<writer>-live.json`, uses the same
top-level structure as the standard benchmark result, but its query results
contain no timing fields. Instead, each entry is a pass/fail check on the
live-poll behaviour:

```
"query": {
  "backend":    "dsos",
  "adapter":    "dsos",
  "query_file": null,
  "mode":       "both",
  "repeat":     20,
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

### Write side — raw timers on the server, statistics in the core

Each writer is a standalone program (C binary or Python script) that runs
on a server host via ssh or srun. The core harness cannot directly time a
remote process, so each writer records raw measurements and prints them as
one JSON line. It computes no rate and no percentile; `core/stats.py` does,
the same way for every backend.

The writer's JSON output includes:

| Field | What it covers |
|---|---|
| `records`, `list_entries`, `errors` | node records and list entries written; failed writes |
| `batch`, `rate` | the writer's own settings, echoed |
| `start_epoch_s` | wall-clock time at which the write began |
| `open_s` | container or connection open (if the writer has one) |
| `write_s` | first record inserted → last record committed (the data ingestion itself) |
| `close_sync_s` | container close and flush to stable storage (0 if there is no such step) |
| `checkpoints` | `[[seconds since write start, records written so far], ...]`, starting with `[0, 0]`, one entry after each batch has been committed or its request completed, and a last one when the write finished |

The interval between two checkpoints is one batch. Its duration is the
batch's latency, and its records divided by its duration is the batch's
throughput. The exact boundaries differ slightly between writers (the SOS C
writer includes `sos_container_commit()` in the batch; the InfluxDB writer
has no sync step), so a batch means "`batch` node records handed to the
backend and acknowledged" everywhere.

### Query side — core-timed on the client

Query timing is done by the benchmark engine (`core/bench.py`), not by
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

- **Writer timers** let each backend report its own internal phases
  (open, write, sync, close) and per-batch progress with granularity that
  the harness cannot observe remotely. Only the raw numbers come from the
  writer; the statistics are computed by the core.
- **Core-timed queries** ensure every adapter is measured the same way,
  from the same vantage point, regardless of backend internals. The
  timing reflects end-to-end latency including network and deserialization,
  which is what matters for query performance.


## Statistics

`core/stats.py` computes every reported statistic, for ingest and query
alike. Each measured series is summarized the same way: `n`, `min`, `p50`,
`p90`, `p95`, `p99`, `max`, `mean`, `stddev`. Percentiles are linearly
interpolated between the closest ranks. A percentile is reported only when
the series has enough samples for it to be more than the maximum, and is
`null` (shown as `-`) otherwise:

| Percentile | Samples needed |
|---|---|
| p50 | 2 |
| p90 | 10 |
| p95 | 20 |
| p99 | 100 |

Nothing assumes a data set size, a batch size or a host count. The number
of ingest samples is `records / batch` per host (160 per host with the
defaults, `BATCH=500` and 80,000 records), and the number of query samples
is `REPEAT` (20 by default). Raise `SAMPLES`/`COMPONENT_IDS_PER_SERVER`,
lower `BATCH` or raise `REPEAT` to get more samples; the percentiles
appear as soon as there are enough.
