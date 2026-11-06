# dsos backend

Writes to and queries SOS containers across all server nodes through the
`dsosd` RPC service. A single query fans out to every server, each scans its
own partition, and the client merges the results in index order.

## What it supports

| | |
|---|---|
| Write | Yes, through the `dsosd` API (`dsos` writer). Each server's writer connects to the local `dsosd` and inserts records through the distributed API |
| Query | Yes, across all servers through `dsosd` |
| Reads data written by | `sos`, `dsos` (declared in `PLUGIN_READS`) |
| Service | `dsosd` on each server node; started in `serve-write` and stopped in `stop` |
| List attributes (design/value-generation.md Sec 6) | Yes -- see "List attribute data model mapping" below |

The DSOS writer stores data in the same SOS container format as the `sos`
backend, so data written by either backend can be queried by either.

Common configurations:

| `WRITE_BACKEND` | `QUERY_BACKEND` | Description |
|---|---|---|
| `sos` | `dsos` | Write locally, query through dsosd |
| `dsos` | `dsos` | Write and query through dsosd |

## Query methods

Two adapters, selected with `QUERY_ADAPTER` (empty selects the default):

| `QUERY_ADAPTER` | Adapter | How it queries | Use |
|---|---|---|---|
| `dsos` (default) | `sosdb` Python API (`SqlQuery`), returning DataFrames | one client, results merged in the client | **timings** |
| `dsosql` | the `dsosql` command, CSV output parsed | starts a `dsosql` process per query | cross-checking results; its timings include process start-up and connecting, so not for performance comparison |

Both speak the same query language (dialect `dsos`), so they share the query
strings in a query set.

## List attribute data model mapping

A dataset may also declare list attributes (design/value-generation.md
Sec 6) -- see `dataset/curated_metrics_like.json`'s `cpu`/`netdev` for a
worked example. DSOS writes and queries them exactly the way the `sos`
backend does, because both store into the same SOS container format and
schema shape (see `backends/sos/README.md`'s "List attribute data model
mapping" for the full mapping table): one SOS schema per declared list
attribute, named `{schema}_<name>`, built by the same
`backends/sos/make_schema.py`.

- **Writing:** the `dsos_write.py` ingest adapter gained a `ListWriter` mirroring
  `sos_write.py`'s -- it writes every entry instance
  (`Dataset.expected_lists()`) into its list attribute's schema through
  the same `dsosd` session the flat node records go through.
- **Querying:** `dsos_query.py`'s `dsos` and `dsosql` adapters answer
  `name[i].attr`/`name[*].attr` (design doc Sec 6.5) by selecting
  directly from the per-list-attribute schema -- the DSOS query language
  has no JOIN, so there is nothing to join against the node schema in
  the query text itself. The existential form (`name[*].attr`) can
  return more than one row per node sample when more than one instance
  matches; both adapters' `postprocess()` collapses those into one row,
  generically (by comparing the query's own `FROM` target against the
  dataset's schema name, so it needs no list of declared list-attribute
  names), the same effect InfluxDB's native `SELECT DISTINCT ... JOIN
  ...` gets. See `queries/curated_metrics.json`'s `"dsos"` entries for
  worked examples.
- **`count(name)` has no native query here** -- the DSOS query language
  has no `GROUP BY`/`HAVING`-style count filter -- so that condition
  reports SKIP under this backend (see `STATUS.md`).
- **Type mapping:** every attribute type maps onto a real SOS schema type
  (`backends/sos/make_schema.py`'s `sos_attr()`): `string` becomes a
  fixed-length CHAR_ARRAY, `uint8`/`int8` widen to `UINT16`/`INT16`.
  Nothing is dropped. `dataset/curated_metrics_like.json`'s `cpu_name`/
  `iface_name` (both `string`) are written as CHAR_ARRAY.

## Configuration

Settings live in `backends/dsos/dsos_backend.conf` (override from the environment):

| Setting | Default | Purpose |
|---|---|---|
| `DSOSD` | `dsosd` | The `dsosd` command. Set to `sudo -E dsosd` if registering with `rpcbind` needs root |
| `DSOSQL` | `dsosql` | The `dsosql` command (used by the `dsosql` adapter) |
| `SOS_ROW_LIMIT` | `4096` | Rows per batch the `dsos` adapter fetches |
| `SOS_DATA_ROOT` | `${OUTPUT_DIR}/nodes/${HOSTNAME}/data` | Where each server's containers are; must match the `sos` backend's value so `dsosd` finds them |

### Choosing the query adapter

```bash
QUERY_ADAPTER=dsos   bin/run.sh -l ssh serve check stop   # Python API (default)
QUERY_ADAPTER=dsosql bin/run.sh -l ssh serve check stop   # dsosql, for cross-checking
```

When `dsos` is also the write backend, use `serve-write` instead of `serve`.

## Requirements

- `dsosd` and `dsosql` installed on the nodes.
- `rpcbind` running on the server nodes, and the query node able to reach it
  (TCP/UDP 111) and the port `dsosd` registers.
- For the `dsos` adapter: the `sosdb` Python module importable on the query
  node, built with the `SqlQuery.next()` batch fix (without it, one row per full
  `SOS_ROW_LIMIT` batch is dropped and verification fails).

## Notes

- The harness removes `dsosd`'s `rpcbind` registration when it stops the
  service, since `dsosd` does not deregister itself.
- `limit` in a query applies per server, so `limit N` can return up to N per
  server; the verifier accepts this and reports it.
