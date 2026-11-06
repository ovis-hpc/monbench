# Backend plugins

The core of the suite (`bin/`, `core/`, `dataset/`, `queries/`) knows nothing
about any particular database. Everything backend-specific lives in one
directory per backend:

```
backends/<n>/
  <n>_backend.conf   settings and declarations
  <n>_plugin.sh      lifecycle hooks: build, write, serve, stop, clean
  query/<n>_query.py  query adapters
  writer/                writers (if any)
  ...                    anything else the backend needs (tools, etc.)
```

Selecting a backend is two settings in `bench.conf`: `WRITE_BACKEND` and
`QUERY_BACKEND`, each the name of a directory under `backends/`. If the
directory does not exist, or lacks a required file, the run stops with an
error before doing anything.

To add a backend, copy `backends/template/` to `backends/<n>/` and rename
each file with your backend's name as a prefix: `<n>_plugin.sh`,
`<n>_backend.conf`, `query/<n>_query.py`. See `EXTENDING.md` for the full
contract: config declarations, plugin hooks, writer summary format, plugin
meta files, query adapter interface, `type_overrides`, and a testing
walkthrough.

## Ingest-handoff naming

Each backend plugin constructs its own storage target name during the write
phase, using the `ds_name()` helper to read the dataset definition's `"name"`
field. The plugin records the target name and any other storage-specific
information in an ingest-handoff meta file at `meta/ingest-handoff-<writer>.json`
(under the pair's `OUTPUT_DIR`). The query engine reads this file to find
where the data lives — the core never constructs or passes storage target
names. See `EXTENDING.md`, "Plugin meta files" for the full contract and
"OUTPUT_DIR layout" for the directory structure.

## Backends provided

Each backend has its own README with full details:
[sos](sos/README.md), [dsos](dsos/README.md), [influxdb](influxdb/README.md).

| Backend | Writes | Queries | Notes |
|---|---|---|---|
| `sos` | C and Python writers, one local SOS container per server | `sos` adapter: directly on disk via the `sosdb` Python API | The primary writer; containers also queried by the `dsos` backend |
| `dsos` | `dsos` writer: writes through the `dsosd` API | `dsos` (Python API) and `dsosql` adapters: queries across servers through `dsosd` | Data is the same SOS container format; data written by either `sos` or `dsos` can be queried by either |
| `influxdb` | `influx` writer (`influxdb_client_3` or HTTP line protocol) | `influxdb` (Arrow Flight) and `influxdb-http` (HTTP query API) adapters | Writes to one InfluxDB 3 server; not partitioned across servers |
| `template` | skeleton | skeleton | Starting point for new backends |

### Tested write→query pairs

| `WRITE_BACKEND` | `QUERY_BACKEND` | Description |
|---|---|---|
| `sos` | `dsos` | Write locally, query through dsosd (primary use case) |
| `sos` | `sos` | Write and query locally — single-node or shared-filesystem comparison |
| `dsos` | `dsos` | Write and query through dsosd |
| `influxdb` | `influxdb` | Write and query InfluxDB v3 |

### InfluxDB v3

The `influxdb` backend writes to and queries a single InfluxDB 3 server, so it
is not partitioned the way `sos`/`dsos` are: each server node generates its own
partition of the data set and sends it to the same `INFLUX_HOST`. Together the
nodes write the whole data set once.

Auth is managed by the harness: `plugin_serve_start` generates an admin token
offline before starting `influxdb3`, and creates the database automatically.
No token or database needs to be set up in advance.

Two options in `backends/influxdb/influxdb_backend.conf`:

- `INFLUX_INTERFACE`: `python` (the `influxdb_client_3` library, default) or
  `http` (POST line protocol to the v3 write API with no extra dependency).
- `INFLUX_QUERY_LANG`: `sql` (default) or `influxql`.

The data set maps to InfluxDB as: measurement = the data set name
(`ds_name()`, which reads the dataset definition's `"name"` field); tags =
`component_id`, `job_id`; fields = every other dataset attribute, including
`seq` (kept as a field so verification can identify each row).

Requirements: an InfluxDB 3 server reachable at `INFLUX_HOST`. The `python`
interface also needs `pip install influxdb3-python` on the write and query
nodes; the `http` interface needs only Python's standard library.
