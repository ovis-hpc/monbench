"""SOS/DSOS query adapters for the benchmark engine.

  dsos    DSOS through the Python API (sosdb.Sos), returning DataFrames.
          Use this one for timings.
  dsosql  DSOS through the dsosql command, parsing its CSV output. Each query
          starts a dsosql process that connects to every server, so timings
          include that start-up; useful as a cross-check only.

Settings (backends/dsos/dsos_backend.conf): SOS_ROW_LIMIT, DSOSQL.

List-attribute queries (design/value-generation.md Sec 6.5): see
backends/sos/query/sos_query.py's module docstring -- both adapters here
share the same "dsos" dialect and SOS query language, so the same native
query text (select straight from the per-list-attribute schema
"{schema}_<name>", no JOIN) and the same postprocess()-based dedup for
the name[*] existential form apply unchanged. count(name) is likewise
not yet natively queryable here (STATUS.md).
"""
import os
import re
import subprocess

from egress_adapter import EgressAdapter, BackendError

# Matches the table name in "... from <name> ...", case-insensitively --
# see backends/sos/query/sos_query.py's _dedup_list_rows() docstring,
# which this mirrors for the dsos/dsosql adapters.
_FROM_RE = re.compile(r"\bfrom\s+(\S+)", re.IGNORECASE)
_IDENTITY_COLS = ("component_id", "job_id", "seq")


def _dedup_list_rows(ctx, text, columns, rows):
    """Identical to sos_query.py's helper of the same name -- duplicated
    rather than shared, consistent with every backend's query adapter
    being a single self-contained file loaded by path (core/egress.py's
    load_plugin()); see that file's docstring for the full rationale."""
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


def _session_file(ctx):
    """DSOS session file: the server hosts, one per line.

    Written to output_dir/tmp/ -- a transient runtime file, not part of
    the writer→query meta handoff.
    """
    tmp_dir = os.path.join(ctx["output_dir"], "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    path = os.path.join(tmp_dir, "dsos-session.txt")
    with open(path, "w") as f:
        f.write("\n".join(ctx["servers"]) + "\n")
    return path


def _probe(ctx):
    return f"select timestamp from {ctx['schema']} order_by timestamp limit 1"


class DsosAdapter(EgressAdapter):
    name = "dsos"
    dialect = "dsos"

    def connect(self):
        try:
            from sosdb import Sos
        except ImportError as e:
            raise BackendError(f"the sosdb Python module is not available: {e}")
        ctx = self.ctx
        self.row_limit = int(ctx["env"].get("SOS_ROW_LIMIT", "4096"))
        try:
            self.session = Sos.Session(_session_file(ctx))
        except Exception as e:
            raise BackendError(f"cannot attach to DSOS servers "
                               f"{', '.join(ctx['servers'])}: {e}")
        try:
            self.cont = self.session.open(ctx["target"], o_perm=Sos.PERM_RD)
        except Exception as e:
            raise BackendError(f"cannot open container '{ctx['target']}': {e}")
        try:
            rows = sum(self.count(b) for b in self.query(_probe(ctx)))
        except Exception as e:
            raise BackendError(f"schema '{ctx['schema']}' cannot be queried: {e}")
        if rows == 0:
            raise BackendError(f"container '{ctx['target']}' holds no "
                               f"'{ctx['schema']}' records")

    def query(self, text):
        q = self.cont.query(self.row_limit)
        q.select(text)
        for df in q:
            yield df

    def records(self, df):
        return list(df.columns), df.to_dict("records")

    def postprocess(self, text, columns, rows):
        return _dedup_list_rows(self.ctx, text, columns, rows)

    def close(self):
        self.cont = None
        self.session = None


class DsosqlAdapter(EgressAdapter):
    name = "dsosql"
    dialect = "dsos"
    PROMPT = "dsosql: "

    def connect(self):
        self.session = _session_file(self.ctx)
        self.dsosql = self.ctx["env"].get("DSOSQL", "dsosql")
        batches = list(self.query(_probe(self.ctx)))
        if not batches or not batches[0]:
            raise BackendError(f"no '{self.ctx['schema']}' records reachable in "
                               f"container '{self.ctx['target']}'")

    def query(self, text):
        script = (f"set format csv\n"
                  f"open path {self.ctx['target']} perm ro\n"
                  f"{text}\n")
        env = dict(os.environ, TERM="dumb", INPUTRC="/dev/null")
        proc = subprocess.run([self.dsosql, "-a", self.session], input=script,
                              text=True, capture_output=True, env=env)
        columns, rows, footer, in_csv = [], [], None, False
        errors = [l.strip() for l in proc.stderr.splitlines() if l.strip()]
        for line in proc.stdout.splitlines():
            while line.startswith(self.PROMPT):
                line = line[len(self.PROMPT):]
            if not in_csv:
                if line.startswith("# ") and not line.startswith("# Records"):
                    columns = [c.strip() for c in line[2:].split(",")]
                    in_csv = True
                elif line.startswith("Error"):
                    errors.append(line.strip())
                continue
            m = re.match(r"# Records (\d+)", line)
            if m:
                footer, in_csv = int(m.group(1)), False
            elif line.strip():
                rows.append(dict(zip(columns, line.split(","))))
        if errors or proc.returncode != 0 or footer is None:
            raise BackendError("; ".join(errors) or
                               f"dsosql exited with status {proc.returncode}, no result")
        if footer != len(rows):
            raise BackendError(f"dsosql reported {footer} records, parsed {len(rows)}")
        self._columns = columns
        yield rows

    def records(self, rows):
        return self._columns, rows

    def postprocess(self, text, columns, rows):
        return _dedup_list_rows(self.ctx, text, columns, rows)


ADAPTERS = {a.name: a for a in (DsosAdapter, DsosqlAdapter)}
DEFAULT_ADAPTER = "dsos"
