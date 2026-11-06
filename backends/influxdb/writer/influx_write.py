"""influx_write.py -- ingest adapter that writes the benchmark data set to an
InfluxDB 3 server.

Loaded by core/ingest.py, which generates the records (core/dataset.py, the
same values the SOS writers write, so the query engine verifies InfluxDB
answers against the same expected values), drives the loop and does all the
timing. The measurement (table) name is passed via --measurement; the key
attributes component_id and job_id become tags; the remaining attributes
become fields, their line-protocol encoding (integer/float/string) taken from
each attribute's declared type, so a dataset with different or additional
attributes needs no change here.

If the dataset declares list attributes (design/value-generation.md Sec
6), each one's entry instances are also written, one measurement per
list attribute named "<measurement>_<name>" -- see list_lines() below
and backends/influxdb/README.md's "List attribute data model mapping"
for the exact tag/field layout. A dataset with no list attributes (e.g.
the default data set) writes exactly as before.

Two interfaces, chosen by --interface:
  python  the influxdb_client_3 library (batched writes)
  http    POST line protocol to the InfluxDB 3 write API with urllib

write() buffers the line-protocol lines of a node record; commit() sends the
buffered batch in one request and returns when the server has accepted it.
There is no separate durable-sync step, so sync_s is 0.
"""
import os
import sys
import urllib.request
import urllib.error

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "..", "..", "..", "core"))
from ingest_adapter import BackendError, IngestAdapter   # noqa: E402

# Attributes that become InfluxDB tags rather than fields.
_TAGS = ("component_id", "job_id")
# Integer-typed dataset types that get an integer line-protocol suffix.
# Signed types use 'i'; unsigned types use 'u'.
_SIGNED_INT_TYPES = ("int8", "int16", "int32", "int64")
_UNSIGNED_INT_TYPES = ("uint8", "uint16", "uint32", "uint64")
_INT_TYPES = _SIGNED_INT_TYPES + _UNSIGNED_INT_TYPES


def _escape_string_field(v):
    return '"' + str(v).replace('\\', '\\\\').replace('"', '\\"') + '"'


def _field(name, v, attr_type):
    """One name=value line-protocol field, encoded per its declared type:
    signed integer types get the 'i' suffix, unsigned integer types get
    the 'u' suffix, string types are quoted, everything else
    (double) is a bare float. Shared by line() (flat attributes) and
    list_lines() (list-attribute entries) so the two encode identically.
    """
    if attr_type in _SIGNED_INT_TYPES:
        return f"{name}={int(v)}i"
    if attr_type in _UNSIGNED_INT_TYPES:
        return f"{name}={int(v)}u"
    if attr_type == "string":
        return f"{name}={_escape_string_field(v)}"
    return f"{name}={v}"                                # double


def line(measurement, comp, seq, values, ds):
    """One line-protocol record for the flat (node-level) measurement.
    Tags: component_id, job_id. Fields: every other dataset attribute
    (seq included, so rows can be identified)."""
    fields = []
    for name in ds.attr_names:
        if name in _TAGS or name == "timestamp":
            continue
        fields.append(_field(name, values[name], ds.attr_type(name)))
    ts = int(values["timestamp"])
    return (f"{measurement},component_id={comp},job_id={values['job_id']} "
            f"{','.join(fields)} {ts}")


def list_lines(measurement_base, comp, seq, values, lists, ds):
    """Line-protocol records for every list-attribute entry instance at
    this sample (design/value-generation.md Sec 6), one measurement per
    list attribute named "<measurement_base>_<name>" -- see
    backends/influxdb/README.md's list attribute mapping table.

    Tags: component_id, job_id, entry_index (the design doc's
    node_id/job_id/entry_index). Fields: the list's own entry attributes
    (a record entry's tuple fields, or a single "value" field for a
    bare-scalar entry), plus seq (kept, like the flat measurement's seq
    field, so an entry row can be tied back to the sample it belongs to
    without relying on point time alone). Point time is the same
    timestamp as the node sample this entry instance belongs to, so a
    native query can join an entry measurement back to the flat one on
    (component_id, time) -- it already uniquely identifies
    (component_id, seq), and job_id is a deterministic function of seq,
    so it never disagrees with that join.

    *lists* is Dataset.expected_lists(comp, seq). Returns an empty list for
    a dataset with no declared list attributes (e.g. the default data
    set), so callers need no special-casing.
    """
    job_id = values["job_id"]
    ts = int(values["timestamp"])
    lines = []
    for rec in lists:
        name = rec["list"]
        la = ds.list_attr(name)
        measurement = f"{measurement_base}_{name}"
        fields = [f"seq={seq}i"]
        if la["entries"].get("type") == "record":
            for tup in la["entries"].get("tuples", []):
                fname = tup["name"]
                fields.append(_field(fname, rec[fname], ds.list_tuple_type(name, fname)))
        else:
            fields.append(_field("value", rec["value"], ds.list_tuple_type(name)))
        lines.append(f"{measurement},component_id={comp},job_id={job_id},"
                     f"entry_index={rec['index']} {','.join(fields)} {ts}")
    return lines


class HttpWriter:
    """POST line protocol to the InfluxDB 3 write API."""
    def __init__(self, a):
        self.url = (f"{a.host.rstrip('/')}/api/v3/write_lp"
                    f"?db={a.database}&precision={a.precision}")
        self.headers = {"Content-Type": "text/plain; charset=utf-8",
                        "Authorization": f"Bearer {a.token}"}

    def write(self, lines):
        body = "\n".join(lines).encode()
        req = urllib.request.Request(self.url, data=body, headers=self.headers,
                                     method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                resp.read()
        except urllib.error.HTTPError as e:
            raise BackendError(
                f"write failed ({e.code}): {e.read().decode(errors='replace')[:200]}")

    def close(self):
        pass


class PythonWriter:
    """Batched writes through the influxdb_client_3 library."""
    def __init__(self, a):
        try:
            from influxdb_client_3 import InfluxDBClient3
        except ImportError as e:
            raise BackendError(f"the influxdb_client_3 module is not "
                               f"available: {e}. Use --interface http, or pip "
                               f"install influxdb3-python.")
        self.precision = a.precision
        self.client = InfluxDBClient3(host=a.host, database=a.database,
                                      token=a.token)

    def write(self, lines):
        self.client.write(record=lines, write_precision=self.precision)

    def close(self):
        self.client.close()


class InfluxIngest(IngestAdapter):
    name = "influx"
    durability = "none (server acknowledges the write)"

    @staticmethod
    def add_arguments(p):
        p.add_argument("--measurement", required=True, help="measurement/table name")
        p.add_argument("--interface", choices=["python", "http"], default="python")
        p.add_argument("--host", required=True)
        p.add_argument("--database", required=True)
        p.add_argument("--token", required=True, help="InfluxDB admin token")
        p.add_argument("--precision", choices=["ns", "us", "ms", "s"], default="s")

    def open(self):
        a = self.ctx.args
        self.ds = self.ctx.dataset
        self.measurement = a.measurement
        self.transport = HttpWriter(a) if a.interface == "http" else PythonWriter(a)
        self.buf = []

    def write(self, comp, seq, values, lists):
        self.buf.append(line(self.measurement, comp, seq, values, self.ds))
        self.buf.extend(list_lines(self.measurement, comp, seq, values, lists, self.ds))

    def commit(self):
        if self.buf:
            self.transport.write(self.buf)
            self.buf.clear()

    def close(self):
        self.transport.close()

    def info(self):
        a = self.ctx.args
        return {"measurement": a.measurement, "backend": f"influxdb/{a.interface}"}


ADAPTERS = {InfluxIngest.name: InfluxIngest}
DEFAULT_ADAPTER = InfluxIngest.name
