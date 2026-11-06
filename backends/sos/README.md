# sos backend

Writes the benchmark data set into local SOS containers, one partition per
server node, using the SOS storage library directly. It can also query those
containers directly on disk, without going through `dsosd`, providing a
single-node or shared-filesystem comparison point.

## What it supports

| | |
|---|---|
| Write | Yes, one local SOS container per server node |
| Query | Yes, directly on disk (containers must be accessible from the query node) |
| Partitioning | Each server writes partition *i* of the data set to its own container |
| Service | None; SOS is a library, no daemon needed for writing or querying |
| List attributes (design/value-generation.md Sec 6) | Yes -- see "List attribute data model mapping" below |

Common configurations:

| `WRITE_BACKEND` | `QUERY_BACKEND` | Description |
|---|---|---|
| `sos` | `dsos` | Write locally, query through dsosd (primary use case) |
| `sos` | `sos` | Write and query locally — single-node or shared-filesystem comparison |

`sos` has no service (`PLUGIN_SERVICE=0`), so the `serve-write` and
`serve-query` phases are no-ops for this backend.

## Write methods

Two writers produce identical data; choose with `WRITERS` (space-separated;
empty selects both):

| `WRITERS` value | Writer | Interface |
|---|---|---|
| `c` | `writer/sos_write` | SOS **C API**, compiled by the build phase |
| `py` | `writer/sos_write.py` | SOS **Python bindings** (`sosdb`) |
| `c py` (default) | both, into separate targets `monitoring_c` and `monitoring_py` | for comparison |

The C writer is built by `bin/run.sh build`; the Python writer needs the
`sosdb` module importable on the server nodes. Both are ingest adapters: the
Python one is run by `core/ingest.py`, the C one links the C engine
`core/ingest.c`, and the engine does the timing and saves the raw result.
The two end differently: the C adapter's `sync()` is
`sos_container_commit(SOS_COMMIT_SYNC)`, the Python adapter's is `os.sync()`
(the bindings do not export `SOS_COMMIT_SYNC`); each declares this as its
`durability`.

Both writers (and `dsos_write.py`, which shares `sos_write.py`'s approach)
treat the SOS schema template -- the JSON built by `make_schema.py` from
the dataset definition -- as a derived artifact, not their source of
truth. At write time, the attribute list and each attribute's type come
from the dataset definition itself (the `DATASET` env var); the template
only needs a same-named, same-typed SOS attribute for each one. This is
what lets a new or changed dataset need no change to either writer.

## Query methods

| `QUERY_ADAPTER` | Adapter | How it queries | Use |
|---|---|---|---|
| `sos` (default) | `sosdb` Python API (`SqlQuery`), returning DataFrames | opens each server's container directly on disk | **timings** without RPC overhead |

The `sos` adapter speaks the same query language (dialect `dsos`) as the
`dsos` backend's adapters, so they share the query strings in a query set.

## List attribute data model mapping

A dataset may also declare list attributes (design/value-generation.md
Sec 6) -- see `dataset/curated_metrics_like.json`'s `cpu`/`netdev` for a
worked example. Following the design doc's Sec 6.5 SOS mapping table,
each declared list attribute gets its own schema, generic over however
many list attributes a dataset declares:

| List attribute | SOS |
|---|---|
| attribute name | schema `{schema}_<name>` (e.g. `monitoring_nested_cpu`), built by `make_schema.py`'s `convert_list()` |
| `component_id`, `job_id`, `seq`, `entry_index` | plain attributes (`component_id` and `timestamp` indexed) |
| the list's own entry attributes (a record entry's tuple fields, or a single `value` column for a bare-scalar entry) | plain attributes, same type mapping as the node schema (see "Scope" below) |
| `timestamp` of the node sample this instance belongs to | the `timestamp` attribute, indexed; also joined with `component_id` into a `comp_time` JOIN attribute, the same shape and name the node schema's own `comp_time` key already uses |

This is the same shape the node schema already uses -- one schema per
declared list attribute rather than hardcoded ones for `cpu`/`netdev`.
`sos_write.py`/`sos_write.c` write every entry instance
(`Dataset.expected_lists()` / `ds_expected_lists()`) into its list
attribute's schema alongside the node record for the same sample.

Querying: the SOS query language has no JOIN, so a native
`name[i].attr`/`name[*].attr` query (design doc Sec 6.5) selects directly
from the list attribute's own schema rather than joining it back to the
node schema in the query text -- e.g. `select component_id, job_id, seq,
timestamp from {schema}_cpu where idle < 50 order_by timestamp`. That
alone answers the indexed form (`name[i].attr`; one instance per node
sample per fixed index is already unique). The existential form
(`name[*].attr`) can return more than one row per node sample when more
than one instance matches; `sos_query.py`'s `postprocess()` collapses
those into one row, the effect InfluxDB's native `SELECT DISTINCT ...
JOIN ...` gets from its own query language. See
`queries/curated_metrics.json`'s `"dsos"` entries for worked examples.
`count(name)` has **no native query here** -- the SOS query language has
no `GROUP BY`/`HAVING`-style count filter -- so that condition still
reports SKIP under this backend (see `STATUS.md`).

**Scope:** every attribute type maps onto a real SOS schema type
(`make_schema.py`'s `sos_attr()`): `string` becomes a fixed-length
CHAR_ARRAY, and `uint8`/`int8` widen to the closest wider SOS integer
type (`UINT16`/`INT16`) since SOS has no 8-bit integer type. `sos_write.c`
writes every family, signed integers of every width included
(`int8`/`int16`/`int32`/`int64`, via `set_value()`'s `DS_FAMILY_INT`
case), flat and list-attribute entries alike -- nothing is dropped or
unsupported at the writer level any more. The remaining caveat is that
this has not been built or smoke-tested against a real SOS install in
any environment that produced this code (no `libsos`/`libods` available
there); see `STATUS.md` item 8. In `dataset/curated_metrics_like.json`,
`cpu_name` and `iface_name` (both `string`) are written to SOS as
CHAR_ARRAY.

**Requirement:** the containers must be accessible from the query node.
This works when the query node is also a server node, or on a shared
filesystem. `SOS_DATA_ROOT` must resolve to the same container root from
the query node as it does on the server nodes; if the default includes
`${HOSTNAME}`, override it to a shared path for the query side.

## Configuration

Settings live in `backends/sos/sos_backend.conf` (override from the environment):

| Setting | Default | Purpose |
|---|---|---|
| `SOS_PREFIX` | `/opt/ovis` | SOS installation prefix; used to build the C writer |
| `SOS_DATA_ROOT` | `${OUTPUT_DIR}/nodes/${HOSTNAME}/data` | Container root; each server uses `SOS_DATA_ROOT/<server-id>/`. `OUTPUT_DIR` is already namespaced per write/query pair by `bin/run.sh`, so this needs no backend-name segment of its own; it shares the exact same default formula as `dsos_backend.conf`, so within a pair the two backends always agree on where the data is |
| `SOS_STORAGE` | `mmos` | SOS storage type for new containers: `mmos` or `lsos` |
| `SOS_ROW_LIMIT` | `4096` | Rows per batch fetched by the query adapter |

### Choosing the storage type

```bash
SOS_STORAGE=mmos bin/run.sh -l ssh write     # memory-mapped (default)
SOS_STORAGE=lsos bin/run.sh -l ssh write     # log-structured, for Lustre/GPFS/NFS
```

### Choosing the writer

```bash
WRITERS=c   bin/run.sh -l ssh build write    # C writer only
WRITERS=py  bin/run.sh -l ssh write          # Python writer only (no build needed)
WRITERS="c py" bin/run.sh -l ssh build write # both (default)
```

## Requirements

- SOS installed at `SOS_PREFIX` on the server nodes.
- For the C writer: `jansson` and `libuuid` development files, to build.
- For the Python writer: the `sosdb` module importable, built against the same
  Python that runs it.
