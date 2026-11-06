"""SOS direct query adapter for the benchmark engine.

Queries SOS containers directly on disk, without going through dsosd.
This provides a single-node (or shared-filesystem) comparison point:
the same SqlQuery interface as the dsos adapter, but with no RPC layer.

The adapter opens each server's container from the query node, so the
containers must be accessible from the query node — either on a shared
filesystem, or the query node is also a server node.

The container path for each server is constructed as:
    SOS_DATA_ROOT/<server>/<target>

This must match the path the writer used. When SOS_DATA_ROOT includes
${HOSTNAME} (as it does by default on the write side), the query-side
config must override it to a path that resolves correctly from the query
node — typically the shared-filesystem prefix without ${HOSTNAME}.

Settings (from sos_backend.conf / environment):
    SOS_DATA_ROOT   root of the containers
    SOS_ROW_LIMIT   rows per batch (default 4096)

List-attribute queries (design/value-generation.md Sec 6.5): a list
attribute is written to its own schema named "{schema}_<name>" (see
backends/sos/README.md's "List attribute data model mapping" and
backends/sos/writer/sos_write.py/.c). The SOS query language has no
JOIN/DISTINCT, so a native name[i].attr / name[*].attr query selects
directly from that per-list schema -- e.g.
"select component_id, job_id, seq, timestamp from {schema}_cpu where
idle < 50 order_by timestamp" -- rather than joining it back to the node
schema in the query text itself. That alone answers name[i].attr (each
instance is already unique per node sample). name[*].attr (existential)
can return more than one row per node sample when more than one instance
matches; postprocess() below collapses those into one, the same effect
InfluxDB's native SQL gets from SELECT DISTINCT over its join. See
queries/curated_metrics.json's "dsos" entries for worked examples.
count(name) has no native query here yet -- SOS's language has no
GROUP BY/HAVING-style count filter either, so a count(name) condition
still reports SKIP for this adapter (see STATUS.md).
"""
import os
import re

from egress_adapter import EgressAdapter, BackendError

# Matches the table name in "... from <name> ...", case-insensitively, the
# only part of a SOS query string postprocess() needs to read.
_FROM_RE = re.compile(r"\bfrom\s+(\S+)", re.IGNORECASE)
# Columns identifying one node sample, checked in this order for presence.
_IDENTITY_COLS = ("component_id", "job_id", "seq")


def _dedup_list_rows(ctx, text, columns, rows):
    """Collapse duplicate node-sample rows from a list-attribute query.

    A query against a per-list-attribute schema (see module docstring)
    can return more than one row for the same node sample -- one per
    matching entry instance -- when it has no 'entry_index' filter (the
    name[*] existential form). Collapse rows that agree on every
    identity column present, keeping the first, the same effect
    InfluxDB's native "SELECT DISTINCT ... JOIN ..." gets
    (backends/influxdb/README.md).

    A no-op for an ordinary query against the node schema itself (the
    common case): detected generically by comparing the query's own FROM
    target to ctx['schema'], so this never needs to know which list
    attributes a dataset declares. Also a no-op if no identity column is
    selected, or if the query names no FROM target this regex
    understands (e.g. the exploratory 'group by job' aggregate query) --
    never raises on text it doesn't recognise.
    """
    m = _FROM_RE.search(text)
    if not m or m.group(1) == ctx["schema"]:
        return rows
    keys_present = [c for c in _IDENTITY_COLS if c in columns]
    if not keys_present:
        return rows
    seen = set()
    out = []
    for r in rows:
        key = tuple(r.get(c) for c in keys_present)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _probe(ctx):
    return f"select timestamp from {ctx['schema']} order_by timestamp limit 1"


class SosAdapter(EgressAdapter):
    """Query SOS containers directly on disk (no dsosd)."""

    name = "sos"
    dialect = "dsos"      # same query language as dsos

    def connect(self):
        try:
            from sosdb import Sos
        except ImportError as e:
            raise BackendError(f"the sosdb Python module is not available: {e}")
        self._Sos = Sos

        ctx = self.ctx
        env = ctx["env"]
        self.row_limit = int(env.get("SOS_ROW_LIMIT", "4096"))
        data_root = env.get("SOS_DATA_ROOT", "")
        if not data_root:
            raise BackendError("SOS_DATA_ROOT is not set")

        # Open each server's container directly.
        self._containers = []
        for server in ctx["servers"]:
            path = os.path.join(data_root, server, ctx["target"])
            if not os.path.exists(path):
                raise BackendError(
                    f"container not found: {path}  "
                    f"(SOS_DATA_ROOT may need to be set to a path "
                    f"accessible from the query node)")
            try:
                cont = Sos.Container()
                cont.open(path, o_perm=Sos.PERM_RD)
            except Exception as e:
                raise BackendError(f"cannot open container '{path}': {e}")
            self._containers.append(cont)

        # Probe the first container to verify the schema exists.
        try:
            rows = sum(self.count(b) for b in self._query_one(
                self._containers[0], _probe(ctx)))
        except Exception as e:
            raise BackendError(
                f"schema '{ctx['schema']}' cannot be queried: {e}")
        if rows == 0:
            raise BackendError(
                f"container for server '{ctx['servers'][0]}' holds no "
                f"'{ctx['schema']}' records")

    def _query_one(self, cont, text):
        """Run a query against a single container, yielding DataFrames."""
        q = cont.query(self.row_limit)
        q.select(text)
        for df in q:
            yield df

    def query(self, text):
        """Run the query against every server's container and yield batches."""
        for cont in self._containers:
            yield from self._query_one(cont, text)

    def records(self, df):
        return list(df.columns), df.to_dict("records")

    def postprocess(self, text, columns, rows):
        return _dedup_list_rows(self.ctx, text, columns, rows)

    def close(self):
        for cont in self._containers:
            try:
                cont.close()
            except Exception:
                pass
        self._containers = []


ADAPTERS = {SosAdapter.name: SosAdapter}
DEFAULT_ADAPTER = "sos"
