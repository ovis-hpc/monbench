# influxdb backend

Writes the benchmark data set to, and queries it from, a single **InfluxDB 3**
server. Unlike `sos`/`dsos`, it is not partitioned across the server nodes: each
server node generates its own partition and sends it to the same InfluxDB
server, so together the nodes write the whole data set once.

## What it supports

| | |
|---|---|
| Write | Yes, to one InfluxDB 3 server (`INFLUX_HOST`) |
| Query | Yes, via the Python client (Arrow Flight) or the HTTP query API |
| Reads data written by | `influxdb` |
| Service | Yes (`PLUGIN_SERVICE=1`); the harness starts and stops `influxdb3` and generates its admin token, on the designated server host only (see "Edition & version" and "How writing works with multiple server hosts" below) |
| Partitioning | Data is generated per server but all sent to one server, not sharded |
| List attributes (design/value-generation.md Sec 6) | Yes -- see "List attribute data model mapping" below |

## Edition & version

This backend runs **InfluxDB 3 Core** — the open source, single-node time
series engine (MIT/Apache 2) — via the plain `influxdb3 serve` binary with
`--object-store file`. It does not use, and cannot drive, **InfluxDB 3
Enterprise** or **InfluxDB Clustered**, the commercial products that add
clustering, replication and high availability on top of Core. Core has no
mode where one logical database spans several nodes, so no setting in this
harness can make that happen — `SERVER_HOSTS` with more than one entry does
not give you a multi-node *database*; see "How writing works with multiple
server hosts" below for what it does give you.

The Core version in effect is whatever `influxdb3` resolves to on the
server host's `PATH`; this repo does not pin or install a specific build.
`plugin_serve_start` logs `influxdb3 --version` to the server's
`influxdb3.log` under `OUTPUT_DIR` on every run — check that log (or run
`influxdb3 --version` yourself on the server host) before comparing numbers
across runs or machines.

## How writing works with multiple server hosts

Because Core cannot cluster, there is exactly **one** real `influxdb3`
server for an entire run: the host named by `INFLUX_HOST` (by default, the
*first* entry in `SERVER_HOSTS`). Only that host runs `influxdb3`;
`plugin_serve_start`/`plugin_serve_run` no-op on every other `SERVER_HOSTS`
entry. The plugin determines the designated host by extracting the hostname
from `INFLUX_HOST` and comparing it with the current node's `$HOST`
(`hostname -s`); when they don't match, the serve and clean hooks return
immediately.

Every `SERVER_HOSTS` entry, however, runs its own `core/ingest.py` process (with the `influx` ingest adapter, `writer/influx_write.py`) —
that's the sense in which this backend supports multiple server hosts: each
host generates and sends its own partition of the data set (a disjoint
slice of component IDs) to the one real server, so together they write the
whole data set once, concurrently, as several independent clients. Listing
more hosts in `SERVER_HOSTS` for this backend means more concurrent writer
load against one database, not more storage nodes.

## Auth

`plugin_serve_start` runs on the designated server host only and always
starts from scratch, like the `sos` writer, which removes its container
before writing. It:

1. stops any `influxdb3` instance already running on the host;
2. removes `INFLUX_DATA_DIR` and `INFLUX_TOKEN_FILE` left by an earlier run;
3. generates an admin token offline and writes it to `INFLUX_TOKEN_FILE` (a
   JSON file under `OUTPUT_DIR`, outside the data directory);
4. starts `influxdb3` with `--admin-token-file`, then creates
   `INFLUX_DATABASE`.

Starting from an empty data directory matters for two reasons. Data left
by an earlier run would skew the ingest numbers and break verification. And
`influxdb3` adopts `--admin-token-file` only while its catalog (stored in
the data directory) has no admin token, so a new token would not match an
old catalog and every request would fail with `Unauthenticated`.

Every write and query step — wherever it runs — reads the token back from
`INFLUX_TOKEN_FILE` (`_influx_read_token` in `influxdb_plugin.sh`). No token
needs to be set in `bench.conf` or the environment. Because each
`serve-write` replaces the data, a second `write` without a new
`serve-write` adds records to the database from the previous one; run
`serve-write` again to start over. The `clean` phase also removes the data
directory and the token file.

Because only the designated host generates a token, there is no risk of a
second instance overwriting `INFLUX_TOKEN_FILE` with a different token.
All writers and the query adapter read the same file and therefore use the
same token the single server recognizes.

## Query side

`check`/`query` always run on the single `QUERY_HOST`, never on a
`SERVER_HOSTS` entry, and always against the same one `INFLUX_HOST` server.
The adapter is chosen by `INFLUX_INTERFACE` (`python`: the
`influxdb_client_3` library over Arrow Flight; `http`: the HTTP query API)
and the query language by `INFLUX_QUERY_LANG` (`sql` or `influxql`; see
`query/influxdb_query.py`). Authentication uses the same admin token file
described above — this relies on `QUERY_HOST` and every `SERVER_HOSTS` entry
resolving `OUTPUT_DIR` to the same path (true for this harness's `ssh`/`slurm`
launchers).

## Data model mapping

The data set maps to InfluxDB as:

| Data set | InfluxDB |
|---|---|
| data set name (`{schema}`) | measurement (table) |
| `component_id`, `job_id` | tags |
| every other attribute (`seq`, `MemTotal`, `free`, ..., `procs_blocked`) | fields (`seq` kept so rows can be identified in verification) |
| `timestamp` | point time |

## List attribute data model mapping

A dataset may also declare list attributes (design/value-generation.md Sec
6) -- a per-sample nested list, identified by its own attribute name, with
entries that are either a flat record of named tuple fields or a bare
scalar, and a variable, per-sample length (e.g. `cpu`, `netdev` in
`dataset/curated_metrics_like.json`). Each list attribute gets its own
InfluxDB measurement, generic over however many list attributes a dataset
declares:

| List attribute | InfluxDB |
|---|---|
| attribute name | measurement `{schema}_<name>` (e.g. `monitoring_nested_cpu`) |
| `component_id`, `job_id`, `entry_index` | tags |
| the list's own entry attributes (a record entry's tuple fields, or a single `value` field for a bare-scalar entry), plus `seq` | fields (`seq` kept for the same reason as the flat measurement's) |
| `timestamp` of the node sample this instance belongs to | point time |

A list-attribute measurement joins back to the flat node measurement on
`(component_id, time)` -- that pair already uniquely identifies
`(component_id, seq)`, and `job_id` is a deterministic function of `seq`
(`1 + seq // JOB_LEN`), so it never disagrees with that join. A query
translating the `match.where` grammar's `name[i].attr`/`name[*].attr`/
`count(name)` conditions (design doc Sec 6.5) into native SQL joins the
node measurement to the list attribute's measurement this way, filters on
the list's fields (and `entry_index` for the `name[i]` form, or a
`GROUP BY component_id, time HAVING COUNT(*) ...` subquery for
`count(name)`), and selects the *node* measurement's columns -- so the
result is still node-shaped rows that `core/egress.py`'s existing
verifier (which recomputes the expected node-row set, not list-attribute
entry rows) can check unchanged. See `queries/curated_metrics.json` for
worked examples against `dataset/curated_metrics_like.json`.

A dataset with no list attributes declared (e.g. `dataset/default.json`)
writes and queries exactly as before -- the `influx` adapter writes no
list-attribute measurements and `queries/default.json` has no `match.where`
of this form.

## Write methods

One ingest adapter, `writer/influx_write.py` (run by `core/ingest.py`, which does the timing), with a selectable interface. `write()` buffers a node record's lines and `commit()` sends the batch in one request and returns when the server has accepted it; there is no sync step, so `sync_s` is 0:

| `INFLUX_INTERFACE` | How it writes | Extra dependency |
|---|---|---|
| `python` (default) | the `influxdb_client_3` library, batched | `pip install influxdb3-python` |
| `http` | POST line protocol to the v3 write API | none (standard library only) |

Select the writer with `WRITERS=influx` (it is the default for this backend).

## Query methods

The query adapter supports InfluxDB 3's two query languages, selected with
`INFLUX_QUERY_LANG`:

| `INFLUX_QUERY_LANG` | Language |
|---|---|
| `sql` (default) | SQL |
| `influxql` | InfluxQL |

The interface for querying follows `INFLUX_INTERFACE` (the `python` client uses
Arrow Flight; `http` uses the query API). Query strings for InfluxDB go in a
query set under the `influxdb` adapter name. List-attribute queries (see
above) need the `sql` dialect -- InfluxQL has no JOIN.

## Architectural contrast with SOS/DSOS

SOS and DSOS run a store on every server node; each node writes to its own
local containers, and the query side either reads one node's containers
directly (SOS) or aggregates across nodes through `dsosd` (DSOS). InfluxDB
3 Core cannot do this — it is a single-process, single-node engine. The
harness therefore treats InfluxDB differently:

| Aspect | SOS / DSOS | InfluxDB |
|---|---|---|
| Server process | one per `SERVER_HOSTS` entry | one total (designated host) |
| Data directory | per-host, local | one, on the designated host |
| Writer transport | local library call or local file I/O | HTTP POST to `INFLUX_HOST` |
| Token / auth | N/A (file permissions) | single token generated at each serve-write, shared via `INFLUX_TOKEN_FILE` |

## Configuration

Settings live in `backends/influxdb/influxdb_backend.conf` (override from the
environment):

| Setting | Default | Purpose |
|---|---|---|
| `INFLUX_HOST` | `http://<first SERVER_HOSTS host>:8181` | InfluxDB 3 server URL, reachable from all nodes; also the host where `influxdb3` actually runs (see "How writing works with multiple server hosts") |
| `INFLUX_DATABASE` | `monbench` | Database to write to and query; created automatically by `plugin_serve_start` |
| `INFLUX_TOKEN_FILE` | `${OUTPUT_DIR}/nodes/${HOSTNAME}/run/influxdb3-token.json` | Path to the admin token file maintained by `plugin_serve_start` (see "Auth"); set automatically, override only if managing the token yourself |
| `INFLUX_INTERFACE` | `python` | `python` or `http` (write and query) |
| `INFLUX_QUERY_LANG` | `sql` | `sql` or `influxql` |
| `INFLUX_PRECISION` | `s` | Line-protocol timestamp precision: `ns`/`us`/`ms`/`s` |
| `INFLUX_WRITE_BATCH` | `10000` | Records per write request/batch |
| `INFLUX_DATA_DIR` | `${OUTPUT_DIR}/nodes/${HOSTNAME}/data/influxdb` | `influxdb3`'s data directory on the designated server host; override to place data on a specific mount (e.g. a fast local disk on the InfluxDB host). `HOSTNAME` is expanded on the driver host. `OUTPUT_DIR` is already namespaced per write/query pair by `bin/run.sh`, so this needs no backend-name segment of its own. The directory is removed at each serve-write; the token file is kept outside it |

### Examples

```bash
# Write with the Python client (default)
WRITE_BACKEND=influxdb bin/run.sh -l ssh write

# Write with the HTTP interface (no client library needed)
WRITE_BACKEND=influxdb INFLUX_INTERFACE=http bin/run.sh -l ssh write

# Choose the query language
QUERY_BACKEND=influxdb INFLUX_QUERY_LANG=influxql bin/run.sh -l ssh check
```

## Requirements

- An InfluxDB 3 **Core** server reachable at `INFLUX_HOST`. Auth is handled
  by the harness: `plugin_serve_start` generates an admin token offline and
  creates `INFLUX_DATABASE` before the write or query steps run — no token
  or database needs to be set up in advance.
- For the `python` interface: `pip install influxdb3-python` on the write (and
  query) nodes. The `http` interface needs only the Python standard library.
