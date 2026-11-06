#!/usr/bin/env python3
"""egress.py -- benchmark and verify queries against one backend.

Runs on the query node (normally through 'bin/node.sh check' or
'bin/node.sh query'). Settings come from the environment exported by
node.sh from bench.conf and the backend's <name>_backend.conf.

The engine is backend-neutral. It loads the query adapter from the backend
plugin (backends/<BACKEND>/query/<BACKEND>_query.py), connects once, then for each query in
the query set runs one or both of the following steps (QUERY_ADAPTER=null
uses a built-in adapter that returns no rows, to measure the timing loop):

  check  Run the query once, untimed, and verify the full result against the
         data set (core/dataset.py).
  query  Run the query REPEAT times, timing each run from the start of the
         query until the last row has been fetched.

Every statistic is computed in the core, by core/stats.py: adapters only
call their backend, and the engines do the timing (this one for queries,
core/ingest.py for writes). Ingest and query results are summarized the same
way, so they are directly comparable. The ingest results of the writer are
loaded through core/ingest.py and included in the combined result file.

The data set parameters come from the writer's metadata file, written when
the data was written. If it names a different backend, the run stops.

The storage target (container name, measurement, etc.) is determined by
reading the backend plugin's ingest-handoff meta file at
OUTPUT_DIR/meta/ingest-handoff-<writer>.json, written by the plugin during
the write phase. This is the writer->query handoff: the plugin decides
where to store the data, records it, and the query engine reads it back.

'--live' polls a writer that is still writing and checks that every
answer is internally consistent and that the row count never decreases.

Set DRY_RUN=1 in the environment (bin/run.sh's -d flag forwards it here, the
same as for build/write/serve/clean) to resolve and print every query's
final, placeholder-filled text without ever calling adapter.connect() or
sending a query: each query is reported with status DRY instead of being
checked or timed. No adapter changes are needed for this -- it is handled
entirely here, before any adapter method that would touch the backend runs.
"""
import argparse
import importlib.util
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stats                                          # noqa: E402
import ingest as ingest_engine                        # noqa: E402
from egress_adapter import BackendError, EgressAdapter  # noqa: E402
from dataset import Dataset, IDENTITY_ATTRS, read_meta  # noqa: E402

# ----------------------------------------------------------------- settings


def env(name, cast=str, default=None):
    value = os.environ.get(name, default)
    if value is None:
        sys.exit(f"egress.py: {name} is not set; run through bin/node.sh")
    return cast(value)


BENCH_DIR = env("BENCH_DIR")
BACKEND = env("BACKEND")               # the query backend (set by run.sh)
QUERY_BACKEND = os.environ.get("QUERY_BACKEND", BACKEND)
WRITE_BACKEND = os.environ.get("WRITE_BACKEND", BACKEND)
# Backends whose data this query backend can read (from its <name>_backend.conf).
PLUGIN_READS = os.environ.get("PLUGIN_READS", "").split()
SERVERS = env("SERVER_HOSTS").split()
OUTPUT_DIR = env("OUTPUT_DIR")
LIMIT_N = env("LIMIT_N", int)
LIVE_POLL = env("LIVE_POLL", float)
REPEAT = env("REPEAT", int, "20")
QUERY_ADAPTER = os.environ.get("QUERY_ADAPTER", "")
QUERY_FILE = os.environ.get("QUERY_FILE") or os.path.join(BENCH_DIR, "queries", "default.json")
# See the module docstring: resolves and reports every query's text without
# ever connecting to the backend or sending it.
DRY_RUN = bool(os.environ.get("DRY_RUN"))

DS = None          # the Dataset being checked; set in main()


class NullEgress(EgressAdapter):
    """Runs nothing and returns no rows: with 'query' mode it measures the
    cost of the timing loop itself, the floor under every other backend's
    figures. It has no data, so verification ('check') fails with it."""
    name = "null"

    def connect(self):
        pass

    def query(self, text):
        return iter(())


def load_plugin():
    """Load backends/<BACKEND>/query/<BACKEND>_query.py and return the adapter class."""
    if QUERY_ADAPTER == "null":
        return NullEgress
    path = os.path.join(BENCH_DIR, "backends", BACKEND, "query", f"{BACKEND}_query.py")
    if not os.path.exists(path):
        sys.exit(f"egress.py: backend '{BACKEND}' has no query adapter ({path})")
    spec = importlib.util.spec_from_file_location(f"backend_{BACKEND}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    name = QUERY_ADAPTER or module.DEFAULT_ADAPTER
    if name not in module.ADAPTERS:
        sys.exit(f"egress.py: backend '{BACKEND}' has no adapter '{name}'; "
                 f"available: {', '.join(sorted(module.ADAPTERS))}")
    return module.ADAPTERS[name]


# ----------------------------------------------------------------- plugin meta


def _read_plugin_meta(writer):
    """Read the backend plugin's ingest-handoff meta from OUTPUT_DIR/meta/ingest-handoff-<writer>.json.

    The plugin writes this during the write phase to record where and how
    it stored the data (the target name, paths, database names, etc.).
    Returns None if the file does not exist (e.g. live mode before writing
    finishes).
    """
    path = os.path.join(OUTPUT_DIR, "meta", f"ingest-handoff-{writer}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _resolve_target(writer, plugin_meta, live=False):
    """Determine the storage target name from the plugin meta file.

    The plugin records its chosen target in the meta file during the write
    phase. For live mode (writer still running), the meta may not exist
    yet; fall back to the convention '<ds_name>_live'.
    """
    if plugin_meta and "target" in plugin_meta:
        return plugin_meta["target"]
    # InfluxDB uses "measurement" instead of "target".
    if plugin_meta and "measurement" in plugin_meta:
        return plugin_meta["measurement"]
    # Fallback: derive from convention (needed for live mode).
    if live:
        return f"{DS.name}_live"
    return f"{DS.name}_{writer}"


def make_adapter(cls, writer, live=False):
    plugin_meta = _read_plugin_meta(writer)
    target = _resolve_target(writer, plugin_meta, live=live)
    inst = cls({"target": target, "schema": DS.name, "servers": SERVERS,
                "output_dir": OUTPUT_DIR, "env": os.environ})
    inst._column_map = _build_column_map(
        cls.type_overrides, DS.definition.get("attributes", []))
    return inst, target

# ----------------------------------------------------------------- row checks


def parse_ts(value):
    """Return a timestamp in seconds as a float, or None if not recognised.

    Accepts numbers, 'sec.usec' or '(sec, usec)' text, and datetime-like
    objects such as pandas Timestamp or numpy datetime64 (taken as UTC).
    """
    if hasattr(value, "timestamp"):                      # pandas / datetime
        return value.timestamp()
    if type(value).__name__ == "datetime64":             # numpy
        return value.astype("datetime64[us]").astype("int64") / 1e6
    try:
        return float(value)
    except (TypeError, ValueError):
        m = re.match(r"\(?\s*(\d+)\s*[,.]\s*(\d+)\s*\)?$", str(value).strip())
        if m:
            return int(m.group(1)) + int(m.group(2)) / 1e6
    return None


def validate_rows(rows):
    """Check each row against the data set.

    Only the columns present are checked, so queries that select a subset of
    columns can be verified. Checks every flat attribute the dataset
    declares, generically (float attributes with a small tolerance, others
    for exact equality, strings for exact equality), so a custom or
    expanded data set is validated without code changes here. This checks
    values *within* an already-returned flat row; it does not validate
    list-attribute entry values, since no backend schema maps a list
    attribute onto a returned row yet (design/value-generation.md Sec
    6.5's backend-mapping table). Filtering a query's expected result set
    by a list-attribute condition (name[i].attr / name[*].attr /
    count(name)) is separate and is handled by record_matches()/verify().
    Returns (problems, keys, warnings); keys lists (component_id, seq) for
    each row.
    """
    problems, keys = [], []
    ts_unparsed = 0
    check_names = [n for n in DS.attr_names if n not in IDENTITY_ATTRS]
    for n, r in enumerate(rows):
        try:
            cid, seq = int(r["component_id"]), int(r["seq"])
        except (KeyError, ValueError, TypeError):
            problems.append(f"row {n}: missing component_id/seq: {r}")
            continue
        keys.append((cid, seq))
        exp = DS.expected(cid, seq)
        for name in check_names:
            if name not in r:
                continue
            want = exp[name]
            try:
                attr_type = DS.attr_type(name)
                if attr_type == "double":
                    if abs(float(r[name]) - want) > 1e-6:
                        problems.append(f"row {n}: {name} {r[name]} != {want}")
                elif attr_type == "string":
                    if str(r[name]) != str(want):
                        problems.append(f"row {n}: {name} {r[name]!r} != {want!r}")
                elif int(r[name]) != int(want):
                    problems.append(f"row {n}: {name} {r[name]} != {want}")
            except (ValueError, TypeError) as e:
                problems.append(f"row {n}: cannot parse {name} ({e}): {r}")
        if "timestamp" in r:
            ts = parse_ts(r["timestamp"])
            if ts is None:
                ts_unparsed += 1
            elif abs(ts - exp["timestamp"]) > 1e-3:
                problems.append(f"row {n}: timestamp {r['timestamp']} != {exp['timestamp']}")
    warnings = []
    if ts_unparsed:
        warnings.append(f"{ts_unparsed} timestamps in an unrecognised format (not validated)")
    return problems, keys, warnings


def is_sorted(values):
    return all(a <= b for a, b in zip(values, values[1:]))

# ----------------------------------------------------------------- query sets
# A query set is a JSON file. Each query has a backend-neutral description of
# the question ("match"), used for verification, and one query string per
# backend or dialect ("query"). Strings may use {placeholders}. See
# queries/default.json and README.md.

OPS = {"==": lambda a, b: a == b, "!=": lambda a, b: a != b,
       "<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
       ">": lambda a, b: a > b, ">=": lambda a, b: a >= b}
CMP = r"==|!=|<=|>=|<|>"
# A condition value is a number or a quoted string literal (design doc
# Sec 6.5's STRING_LITERAL -- a string-typed attribute decodes to a Python
# str, so string equality is just that value compared to the literal,
# same comparison form as a number).
VALUE = r"-?[\d.]+|'[^']*'|\"[^\"]*\""
NODE_COND_RE = re.compile(rf"^\(?\s*(\w+)\s*({CMP})\s*({VALUE})\s*\)?$")
# name[i].attr cmp value / name[*].attr cmp value -- Sec 6.5's list_cond.
# .attr is optional (a bare-scalar list's entry is the value itself).
LIST_COND_RE = re.compile(rf"^\(?\s*(\w+)\[(\d+|\*)\](?:\.(\w+))?\s*({CMP})\s*({VALUE})\s*\)?$")
# count(name) cmp n -- Sec 6.5's length_cond. Always an integer comparison.
LENGTH_COND_RE = re.compile(rf"^\(?\s*count\((\w+)\)\s*({CMP})\s*(-?\d+)\s*\)?$")


def parse_cond_value(raw):
    """A condition literal: a quoted string (either quote style) or a number."""
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "'\"":
        return raw[1:-1]
    return float(raw)


def fill(value, values, where):
    """Apply placeholders to a string (or to the strings in a dict or list)."""
    if isinstance(value, str):
        try:
            return value.format(**values)
        except KeyError as e:
            sys.exit(f"egress.py: {where} uses unknown placeholder {e}; "
                     f"available: {', '.join(sorted(values))}")
    if isinstance(value, dict):
        return {k: fill(v, values, where) for k, v in value.items()}
    if isinstance(value, list):
        return [fill(v, values, where) for v in value]
    return value


def parse_match(match):
    """Turn a 'match' description into conditions the verifier can apply.

    Returns (spec, reason); reason is set when the description cannot be used.
    Implements design/value-generation.md Sec 6.5's full condition grammar:
    a node-level 'attr cmp value' (as before, now also accepting a quoted
    string literal for string-typed attributes), a list condition
    'name[i].attr cmp value' or existential 'name[*].attr cmp value'
    (".attr" omitted for a bare-scalar list), and a cardinality condition
    'count(name) cmp n'. Attribute and list names are resolved against the
    current dataset's own declaration (DS), not a fixed suite-wide list,
    so a custom dataset's 'where'/'order_by' just work.

    Each entry in spec["conds"] is one of:
      ("node", attr_index, op, value)
      ("list", name, index_or_None, attr_or_None, op, value)  # index None = 'name[*]'
      ("count", name, op, n)
    """
    spec = {"conds": [], "order_by": match.get("order_by") or [],
            "limit": int(match["limit"]) if match.get("limit") not in (None, "") else None}
    where = (match.get("where") or "").strip()
    if where:
        for raw_term in re.split(r"\s+and\s+", where, flags=re.IGNORECASE):
            term = raw_term.strip()
            c = LENGTH_COND_RE.match(term)
            if c:
                name, op, n = c.groups()
                if DS.list_attr(name) is None:
                    return None, f"match condition '{term}' names unknown list attribute '{name}'"
                spec["conds"].append(("count", name, OPS[op], int(n)))
                continue
            c = LIST_COND_RE.match(term)
            if c:
                name, idx, attr, op, raw_value = c.groups()
                la = DS.list_attr(name)
                if la is None:
                    return None, f"match condition '{term}' not understood"
                is_record = la["entries"].get("type") == "record"
                if is_record:
                    if attr is None or attr not in [a["name"] for a in la["entries"].get("tuples", [])]:
                        return None, f"match condition '{term}' not understood"
                elif attr is not None:
                    return None, f"match condition '{term}' not understood"
                spec["conds"].append(("list", name, None if idx == "*" else int(idx),
                                      attr, OPS[op], parse_cond_value(raw_value)))
                continue
            c = NODE_COND_RE.match(term)
            if not c or c.group(1) not in DS.attr_names:
                return None, f"match condition '{term}' not understood"
            attr, op, raw_value = c.groups()
            spec["conds"].append(("node", DS.attr_names.index(attr), OPS[op],
                                  parse_cond_value(raw_value)))
    unknown = [a for a in spec["order_by"] if a not in DS.attr_names]
    if unknown:
        return None, f"match order_by names unknown attributes {unknown}"
    return spec, None


def cond_matches(cond, cid, seq, node_record):
    """One condition's match(...) from Sec 6.5, evaluated at coordinate (cid, seq).

    *node_record* is this row's flat-attribute tuple in DS.attr_names order
    (what DS.records() yields), used for "node" conditions without a second
    recomputation. "list"/"count" conditions call DS.expected_length() /
    DS.expected_list_value(), which recompute directly rather than
    generating every entry of the list (DS.expected_lists()) -- the narrow
    primitives Sec 6.5 calls expected_length() and evaluate(attr, C_i).
    """
    tag = cond[0]
    if tag == "node":
        _, i, op, v = cond
        return op(node_record[i], v)
    if tag == "count":
        _, name, op, n = cond
        return op(DS.expected_length(name, cid, seq), n)
    # tag == "list"
    _, name, idx, attr, op, v = cond
    n = DS.expected_length(name, cid, seq)
    if idx is not None:
        return idx < n and op(DS.expected_list_value(name, idx, attr, cid, seq), v)
    return any(op(DS.expected_list_value(name, i, attr, cid, seq), v) for i in range(n))


def record_matches(spec, cid, seq, node_record):
    """match(condition, C) for every condition in spec["conds"], ANDed (Sec 6.5)."""
    return all(cond_matches(cond, cid, seq, node_record) for cond in spec["conds"])


def load_query_set(path, adapter):
    with open(path) as f:
        doc = json.load(f)
    queries = doc.get("queries")
    if not isinstance(queries, list) or not queries:
        sys.exit(f"egress.py: {path} has no 'queries' list")
    values = DS.placeholders(LIMIT_N,
                             _build_literal_formats(adapter.type_overrides))
    # A second set of placeholders without literal formats, used to expand
    # the backend-neutral 'match' description. The verifier's condition
    # parser expects bare numeric values, not the backend-specific wrappers.
    match_values = DS.placeholders(LIMIT_N)

    # Build the column map once so we can apply it to every query template
    # *before* placeholder expansion.  This order matters: literal formats
    # may inject function names that contain a dataset attribute name (e.g.
    # to_timestamp_seconds), and a post-expansion string replace would
    # mangle them.  Applying the map to the raw template — where only
    # column references use the dataset name — avoids this entirely.
    col_map = _build_column_map(
        adapter.type_overrides, DS.definition.get("attributes", []))
    out = []
    for n, q in enumerate(queries):
        where = f"query {n + 1} ('{q.get('name', '')}') in {path}"
        text = q.get("query")
        if isinstance(text, dict):                       # per backend or dialect
            text = text.get(adapter.name, text.get(adapter.dialect))
            if text is None and adapter.name == "null":
                text = next(iter(q["query"].values()), None)
        if text is None and "query" not in q:
            sys.exit(f"egress.py: {where} has no 'query'")
        # Rename columns before expanding placeholders.
        if text:
            text = _apply_column_map(text, col_map)
        match = q.get("match")
        spec, reason = (parse_match(fill(match, match_values, where)) if match
                        else (None, "no 'match' description"))
        if not q.get("verify", True):
            spec, reason = None, "verification off for this query"
        out.append({"name": q.get("name", f"query {n + 1}"),
                    "type": q.get("type", "-"),
                    "query": fill(text, values, where) if text else None,
                    "spec": spec, "unverified_reason": reason})
    return out

# ----------------------------------------------------------------- verification


def verify(spec, columns, rows):
    """Compare a full result with the records the question should return."""
    idx_cid = DS.attr_names.index("component_id")
    idx_seq = DS.attr_names.index("seq")
    expected = [r for r in DS.records()
               if record_matches(spec, r[idx_cid], r[idx_seq], r)]
    have_key = "component_id" in columns and "seq" in columns
    problems, notes = [], []
    got = want = None
    if have_key:
        vp, keys, warn = validate_rows(rows)
        problems += vp[:3]
        notes += warn
        want = {(r[idx_cid], r[idx_seq]) for r in expected}
        got = set(keys)
        if len(keys) != len(got):
            problems.append(f"{len(keys) - len(got)} duplicate rows")
        if got - want:
            problems.append(f"{len(got - want)} rows that do not match the question")
    else:
        notes.append("rows not identified (component_id and seq not selected); "
                     "count checked only")
    n = len(rows)
    if spec["limit"] is None:
        if n != len(expected):
            problems.append(f"{n} rows, expected {len(expected)}")
        elif have_key and got != want:
            problems.append("returned rows differ from the expected rows")
    else:
        # How a limit is applied differs between backends: to the whole
        # result, or on each server to its own partition. Both are accepted.
        per_server = sum(min(spec["limit"],
                             sum(1 for r in expected if DS.partition_of(r[idx_cid]) == p))
                         for p in range(DS.partitions))
        overall = min(spec["limit"], len(expected))
        if n == overall:
            notes.append("limit applied to the whole result")
        elif n == per_server:
            notes.append("limit applied per server")
        else:
            problems.append(f"{n} rows; expected {overall} (whole result) "
                            f"or {per_server} (per server)")
    order = spec["order_by"]
    if order and all(a in columns for a in order):
        try:
            keyvals = [tuple(parse_ts(r[a]) if a == "timestamp" else float(r[a])
                             for a in order) for r in rows]
            if not is_sorted(keyvals):
                problems.append(f"rows not ordered by {', '.join(order)}")
        except (TypeError, ValueError):
            notes.append("order not checked (unparsed values)")
    elif order:
        notes.append("order not checked (ordering columns not selected)")
    return ("FAIL" if problems else "PASS"), "; ".join(problems[:4] or notes)

# ----------------------------------------------------------------- engine


class Tee:
    """Write every print() to stdout and to a log file at the same time."""
    def __init__(self, path):
        self._file = open(path, "w", buffering=1)
        self._stdout = sys.stdout
        sys.stdout = self

    def write(self, text):
        self._stdout.write(text)
        self._file.write(text)

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def close(self):
        sys.stdout = self._stdout
        self._file.close()


def _ms(seconds):
    """Seconds as milliseconds for a table cell; '-' when not available."""
    return "-" if seconds is None else f"{seconds * 1e3:.1f}"


def _num(value):
    """A rate for a table cell; '-' when not available."""
    return "-" if value is None else f"{value:,.0f}"


class Report:
    def __init__(self, writer, adapter):
        self.writer = writer
        self.adapter = adapter
        self.items = []
        self.raw_queries = []
        self.width = 40

    def add(self, label, status, detail, **fields):
        item = {"check": label, "status": status, "detail": detail}
        item.update(fields)
        self.items.append(item)
        timing = ""
        if "latency_s" in fields:
            lat, thr = fields["latency_s"], fields["throughput_per_s"]
            timing = (f"{_ms(lat['p50']):>10} {_ms(lat['p95']):>10} "
                      f"{_num(thr['p50']):>12}")
        rows = fields.get("rows", "")
        print(f"  {status:5} {label:{self.width}} {rows:>8} {timing:34}  {detail}")

    def failed(self):
        return any(i["status"] == "FAIL" for i in self.items)


def _build_column_map(type_overrides, attributes):
    """Build {dataset_name: backend_name} from type_overrides and dataset attributes."""
    if not type_overrides:
        return None
    col_map = {}
    for attr in attributes:
        override = type_overrides.get(attr["type"])
        if override and "column" in override:
            col_map[attr["name"]] = override["column"]
    return col_map or None


def _build_literal_formats(type_overrides):
    """Extract {attr_type: format_string} from type_overrides."""
    if not type_overrides:
        return None
    fmts = {}
    for attr_type, override in type_overrides.items():
        if "literal" in override:
            fmts[attr_type] = override["literal"]
    return fmts or None


def _apply_column_map(text, column_map):
    """Rewrite dataset attribute names in *text* to backend column names."""
    if not column_map:
        return text
    for dataset_name, backend_name in column_map.items():
        text = text.replace(dataset_name, backend_name)
    return text


def _reverse_column_map(columns, rows, column_map):
    """Rename backend column names back to dataset attribute names."""
    if not column_map:
        return columns, rows
    rev = {v: k for k, v in column_map.items()}
    columns = [rev.get(c, c) for c in columns]
    rows = [{rev.get(k, k): v for k, v in r.items()} for r in rows]
    return columns, rows


def collect(adapter, text):
    """Run a query and return (columns, rows) for the whole result.

    The caller is responsible for applying the column map to *text*
    before calling this function.  Result columns are renamed back to
    dataset attribute names here so the verifier always sees dataset
    names. adapter.postprocess() then gets a chance to do any further,
    backend-specific shaping of the full result (see EgressAdapter.postprocess's
    docstring in core/egress_adapter.py) -- a no-op for most backends.
    """
    columns, rows = [], []
    for batch in adapter.query(text):
        cols, recs = adapter.records(batch)
        columns = columns or cols
        rows.extend(recs)
    columns, rows = _reverse_column_map(columns, rows, adapter._column_map)
    rows = adapter.postprocess(text, columns, rows)
    return columns, rows


def timed_run(adapter, text):
    """Time one run: from the start of the query until the last row is fetched.

    Returns (seconds, rows, seconds until the first batch had been fetched).
    A query that returns no batch at all has no first batch; its time to
    first batch is its whole duration.
    """
    begin = time.perf_counter()
    n = 0
    first = None
    for batch in adapter.query(text):
        n += adapter.count(batch)
        if first is None:
            first = time.perf_counter() - begin
    total = time.perf_counter() - begin
    return total, n, total if first is None else first


def run_query_set(rep, adapter, queries, mode="both"):
    do_check = mode in ("check", "both")
    do_time  = mode in ("query", "both")
    rep.width = max(len(f"[{q['type']}] {q['name']}") for q in queries)
    if do_time:
        print(f"  {'':5} {'query':{rep.width}} {'rows':>8} {'p50 ms':>10} {'p95 ms':>10} "
              f"{'rows/s p50':>12}  detail")
        needed = stats.min_samples(95)
        if REPEAT < needed:
            print(f"  note: REPEAT={REPEAT}; p95 is reported from {needed} runs "
                  f"({stats.min_samples(99)} for p99), so it shows '-' here")
    else:
        print(f"  {'':5} {'query':{rep.width}} {'rows':>8}  detail")
    for q in queries:
        label = f"[{q['type']}] {q['name']}"
        if not q["query"]:
            rep.add(label, "SKIP", f"no query string for adapter '{adapter.name}'")
            continue

        # Column names and literal formats were already applied in
        # load_query_set, so the query is ready to send as-is.
        native_query = q["query"]

        # DRY_RUN: the query is fully resolved (placeholders filled, columns
        # mapped) above, so it is shown exactly as it would be sent, but
        # never actually is -- adapter.query()/count()/records() are never
        # called, and run_checks() never called adapter.connect() either.
        if DRY_RUN:
            rep.add(label, "DRY", "not sent (dry run)", query=q["query"])
            continue

        status, detail = "INFO", ""
        nrows = 0

        # 1. Untimed verification run (check and both modes).
        if do_check:
            try:
                columns, rows = collect(adapter, native_query)
            except Exception as e:
                rep.add(label, "FAIL", f"{type(e).__name__}: {e}", query=q["query"])
                continue
            nrows = len(rows)
            if q["spec"]:
                status, detail = verify(q["spec"], columns, rows)
            else:
                status, detail = "INFO", q["unverified_reason"]
                if q["type"] == "aggregate" and columns and rows:
                    groups = {}
                    for r in rows:
                        groups[r.get(columns[0])] = groups.get(r.get(columns[0]), 0) + 1
                    detail += f"; {len(groups)} groups, rows per group {sorted(set(groups.values()))}"

        if not do_time:
            rep.add(label, status, detail, query=q["query"], rows=nrows)
            continue

        # 2. Timed runs (query and both modes).
        times, firsts, runs = [], [], []
        try:
            for i in range(REPEAT):
                t, n, first = timed_run(adapter, native_query)
                times.append(t)
                firsts.append(first)
                runs.append({"run": i + 1, "duration_s": round(t, 6),
                             "rows": n, "first_batch_s": round(first, 6)})
                if nrows == 0 and i == 0:
                    nrows = n       # first timed run establishes the baseline in query-only mode
                elif n != nrows:
                    status, detail = "FAIL", f"row count changed between runs ({nrows} then {n})"
        except Exception as e:
            status, detail = "FAIL", f"timed run failed: {type(e).__name__}: {e}"
        fields = {"query": q["query"], "rows": nrows, "times_s": [round(t, 6) for t in times]}
        if times:
            fields.update(stats.egress(times, nrows, firsts))
        rep.add(label, status, detail, **fields)
        if runs:
            rep.raw_queries.append({
                "name": q["name"], "type": q["type"],
                "query": q["query"], "rows": nrows, "runs": runs,
            })


def load_dataset(writer, live):
    """Data set to verify against: from the writer's metadata if present."""
    global DS
    meta = None if live else read_meta(writer)
    if meta:
        wrote = meta["backend"]
        if PLUGIN_READS and wrote not in PLUGIN_READS:
            sys.exit(f"egress.py: writer '{writer}' was written by backend "
                     f"'{wrote}', which query backend '{QUERY_BACKEND}' cannot "
                     f"read (it reads: {', '.join(PLUGIN_READS)})")
        DS = Dataset.from_meta(meta)
        return f"written by {wrote} ({meta['writer']} writer) at {meta['written_at']}"
    DS = Dataset.from_env(live=live)
    return "no metadata file; using the current settings" if not live else "live settings"


def run_checks(writer, query_file, mode="both"):
    cls = load_plugin()
    source = load_dataset(writer, live=False)
    adapter, target = make_adapter(cls, writer)
    print(f"Write backend '{WRITE_BACKEND}' -> query backend '{QUERY_BACKEND}' "
          f"(adapter '{adapter.name}'), writer '{writer}', target '{target}' "
          f"on {', '.join(SERVERS)}")
    print(f"Data set '{DS.name}': {DS.size()} records in {DS.partitions} partitions; {source}")
    ingest = ingest_engine.load_results(OUTPUT_DIR, writer, WRITE_BACKEND)
    if DRY_RUN:
        print("\nDRY RUN: not connecting to the backend; queries below are resolved but not sent.")
    else:
        try:
            adapter.connect()
        except BackendError as e:
            sys.exit(f"egress.py: backend '{BACKEND}' check failed: {e}")
    queries = load_query_set(query_file, adapter)

    mode_desc = {"check": "verification only (no timing)",
                 "query": f"{REPEAT} timed runs each (no verification)",
                 "both":  f"1 verified run + {REPEAT} timed runs each"}
    print(f"\nQuery benchmark  [adapter: {adapter.name}, mode: {mode}]")
    print(f"  {query_file} — {len(queries)} queries, {mode_desc[mode]}")

    rep = Report(writer, adapter.name)
    rep.ingest = ingest
    rep.target = target
    try:
        run_query_set(rep, adapter, queries, mode=mode)
    finally:
        if not DRY_RUN:
            adapter.close()
    return rep


def run_live(writer):
    """Poll a writer that is being written; each answer must be valid."""
    cls = load_plugin()
    load_dataset(writer, live=True)
    adapter, target = make_adapter(cls, writer, live=True)
    rep = Report(writer, adapter.name)
    rep.target = target
    final = DS.size()
    # The poll uses the query set's first query of type "all", so that the
    # live test needs no backend-specific query of its own.
    text = next((q["query"] for q in load_query_set(QUERY_FILE, cls)
                 if q["type"] == "all" and q["query"]), None)
    if not text:
        sys.exit(f"egress.py: {QUERY_FILE} has no query of type 'all' for adapter '{cls.name}'")
    if DRY_RUN:
        print(f"DRY RUN: would poll writer '{writer}' (target '{target}') every "
              f"{LIVE_POLL}s until {final} rows with:")
        print(f"    {text}")
        rep.add("live poll", "DRY", "not run (dry run)")
        return rep
    # Column names and literal formats were already applied in load_query_set.
    print(f"Polling writer '{writer}' (target '{target}') every {LIVE_POLL}s until {final} rows")
    counts, invalid, errors = [], 0, 0
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        live_adapter, _ = make_adapter(cls, writer, live=True)
        try:
            live_adapter.connect()
            _, rows = collect(live_adapter, text)
        except Exception as e:  # e.g. the target does not exist yet
            errors += 1
            print(f"    poll: error ({e})")
        else:
            vp, keys, _ = validate_rows(rows)
            if vp or len(keys) != len(set(keys)):
                invalid += 1
            counts.append(len(rows))
            print(f"    poll: {len(rows)} rows, {'valid' if not vp else vp[0]}")
            if len(rows) >= final:
                break
        finally:
            live_adapter.close()
        time.sleep(LIVE_POLL)
    monotonic = is_sorted(counts)
    complete = bool(counts) and counts[-1] == final
    shown = counts if len(counts) <= 6 else counts[:3] + ["..."] + counts[-3:]
    rep.add("rows never decrease", "PASS" if monotonic else "FAIL",
            "counts " + ", ".join(map(str, shown)))
    rep.add("every answer valid", "PASS" if invalid == 0 else "FAIL",
            f"{invalid} of {len(counts)} answers had invalid or duplicate rows")
    rep.add("final count reached", "PASS" if complete else "FAIL",
            f"{counts[-1] if counts else 0}/{final} rows")
    rep.add("query errors while writing", "INFO",
            f"{errors} polls returned an error (e.g. target not yet created)")
    return rep


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--writer", required=True,
                   help="name of the writer whose data to check or benchmark")
    p.add_argument("--mode", choices=("check", "query", "both"),
                   default="both",
                   help="check = verify only (no timing); "
                        "query = time only (no verification); "
                        "both = verify then time (default)")
    p.add_argument("--live", action="store_true")
    p.add_argument("--queries", default=QUERY_FILE,
                   help="query set file (default: QUERY_FILE, or queries/default.json)")
    a = p.parse_args()

    results_dir = os.path.join(OUTPUT_DIR, "results")
    os.makedirs(results_dir, exist_ok=True)

    tmp_log = os.path.join(OUTPUT_DIR, "logs", f"benchmark-{a.writer}-pending.log")
    tee = Tee(tmp_log)
    try:
        rep = run_live(a.writer) if a.live else run_checks(a.writer, a.queries, mode=a.mode)
    finally:
        tee.close()

    if a.live:
        result_name = f"benchmark-live-{a.writer}.json"
        log_name = f"benchmark-live-{a.writer}.log"
    else:
        result_name = f"benchmark-{a.writer}-{rep.adapter}.json"
        log_name = f"benchmark-{a.writer}-{rep.adapter}.log"
    log_path = os.path.join(OUTPUT_DIR, "logs", log_name)
    os.rename(tmp_log, log_path)

    target = getattr(rep, "target", a.writer)

    # Build a generic benchmark result: same structure regardless of backend.
    query_results = []
    for item in rep.items:
        entry = {
            "name": item["check"],
            "status": item["status"],
            "rows": item.get("rows"),
            "verified": item["status"] in ("PASS", "FAIL"),
        }
        # Timing fields (present when timed runs succeeded)
        for key in ("times_s", "latency_s", "throughput_per_s", "first_batch_s"):
            if key in item:
                entry[key] = item[key]
        if item.get("query"):
            entry["query"] = item["query"]
        if item["status"] == "FAIL":
            entry["detail"] = item.get("detail", "")
        query_results.append(entry)

    result = {
        "writer": a.writer,
        "target": target,
        "dataset": DS.params(),
        "servers": SERVERS,
        "dry_run": DRY_RUN,
        "ingest": {
            "backend": WRITE_BACKEND,
            "hosts": getattr(rep, "ingest", {}).get("hosts", []),
            "summary": getattr(rep, "ingest", {}).get("summary", {}),
        },
        "query": {
            "backend": QUERY_BACKEND,
            "adapter": rep.adapter,
            "query_file": None if a.live else a.queries,
            "mode": a.mode,
            "repeat": REPEAT,
            "results": query_results,
        },
        "all_passed": not rep.failed(),
    }

    out = os.path.join(results_dir, result_name)
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  results: {out}")

    # Write per-run raw query data for downstream analysis.
    if rep.raw_queries:
        raw_query = {
            "writer": a.writer,
            "target": target,
            "backend": QUERY_BACKEND,
            "adapter": rep.adapter,
            "query_file": None if a.live else a.queries,
            "repeat": REPEAT,
            "servers": SERVERS,
            "queries": rep.raw_queries,
        }
        raw_path = os.path.join(results_dir, f"egress-{rep.adapter}-raw.json")
        with open(raw_path, "w") as f:
            json.dump(raw_query, f, indent=2)
        print(f"  raw queries: {raw_path}")

    print(f"  log:     {log_path}")
    return 1 if rep.failed() else 0


if __name__ == "__main__":
    sys.exit(main())
