"""sos_write.py -- ingest adapter that writes the benchmark data set with the
SOS Python bindings.

Loaded by core/ingest.py, which generates the records (core/dataset.py, the
same values sos_write.c writes), drives the loop and does all the timing; this
adapter only calls the SOS API. Containers written by either writer answer
every query identically.

If the dataset declares list attributes (design/value-generation.md Sec
6), each one's entry instances are also written, one schema per list
attribute named "<dataset_name>_<name>" -- see ListWriter below,
backends/sos/make_schema.py (which builds that schema's template) and
backends/sos/README.md's "List attribute data model mapping". Every
attribute type maps onto a real SOS schema type (make_schema.py's
sos_attr()): a string attribute is written as the fixed-length
CHAR_ARRAY the template declares, and an unsigned 8/16/32/64-bit
attribute is written as a plain Python int into whichever widened SOS
integer attribute the template created for it -- nothing is dropped. A
dataset with no list attributes (e.g. dataset/default.json) writes
exactly as before.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "..", "..", "..", "core"))
from ingest_adapter import BackendError, IngestAdapter   # noqa: E402


def open_container(Sos, path, backend):
    be = Sos.BE_LSOS if backend == "lsos" else Sos.BE_MMOS
    cont = Sos.Container()
    if not os.path.exists(path):
        cont.create(path, o_mode=0o660, backend=be)
    cont.open(path, o_perm=Sos.PERM_RW)
    return cont


def get_schema(Sos, cont, template):
    schema = cont.schema_by_name(template["name"])
    if schema is None:
        schema = Sos.Schema()
        schema.from_template(template)
        schema.add(cont)
        schema = cont.schema_by_name(template["name"])
    return schema


class ListWriter:
    """One list attribute's schema plus the attr-name -> attr-id map it
    needs to write an entry instance -- built once per list attribute,
    used for every sample. For a record-entries list, *attr_names* is
    its declared tuple fields that the per-list template has a
    same-named SOS attribute for (every one of them, in practice -- see
    make_schema.py's sos_attr()). For a bare-scalar-entries list,
    there is a single "value" column instead (make_schema.py's
    convert_list())."""
    def __init__(self, Sos, cont, template, ds, name):
        self.schema = get_schema(Sos, cont, template)
        self.attr_ids = {n: self.schema.attr_by_name(n).attr_id()
                         for n in ("timestamp", "component_id", "job_id",
                                   "seq", "entry_index")}
        la = ds.list_attr(name)
        self.is_record = la["entries"].get("type") == "record"
        if self.is_record:
            self.attr_names = [a["name"] for a in la["entries"].get("tuples", [])
                               if self.schema.attr_by_name(a["name"]) is not None]
        else:
            self.attr_names = ["value"]
        for n in self.attr_names:
            self.attr_ids[n] = self.schema.attr_by_name(n).attr_id()

    def _set(self, obj, attr_name, t, v):
        aid = self.attr_ids[attr_name]
        if t == "double":
            obj[aid] = float(v)
        elif t == "string":
            obj[aid] = str(v)
        else:
            obj[aid] = int(v)

    def write(self, comp, job_id, seq, ts, rec, ds, name):
        obj = self.schema.alloc()
        obj[self.attr_ids["timestamp"]] = (int(ts), 0)
        obj[self.attr_ids["component_id"]] = int(comp)
        obj[self.attr_ids["job_id"]] = int(job_id)
        obj[self.attr_ids["seq"]] = int(seq)
        obj[self.attr_ids["entry_index"]] = int(rec["index"])
        if self.is_record:
            for attr_name in self.attr_names:
                self._set(obj, attr_name, ds.list_tuple_type(name, attr_name),
                          rec[attr_name])
        else:
            self._set(obj, "value", ds.list_tuple_type(name), rec["value"])
        rc = obj.index_add()
        obj.release()
        return rc


class SosIngest(IngestAdapter):
    name = "py"
    # The bindings do not export SOS_COMMIT_SYNC, so sync() flushes the
    # written pages of the whole host with os.sync().
    durability = "os.sync()"

    @staticmethod
    def add_arguments(p):
        p.add_argument("--path", required=True, help="container path")
        p.add_argument("--template", required=True, help="SOS schema template JSON file")
        p.add_argument("--storage", choices=["mmos", "lsos"], default="mmos")

    def open(self):
        try:
            from sosdb import Sos
        except ImportError as e:
            raise BackendError(f"the sosdb module is not available: {e}")
        a, ds = self.ctx.args, self.ctx.dataset
        with open(a.template) as f:
            template = json.load(f)
        self.cont = open_container(Sos, a.path, a.storage)
        self.schema = get_schema(Sos, self.cont, template["node"])
        self.attr_ids = {n: self.schema.attr_by_name(n).attr_id() for n in ds.attr_names}
        self.attr_types = {n: ds.attr_type(n) for n in ds.attr_names}
        self.list_writers = {name: ListWriter(Sos, self.cont, template["lists"][name], ds, name)
                             for name in template["lists"]}
        self.ds = ds
        self.in_txn = False

    def write(self, comp, seq, values, lists):
        if not self.in_txn:
            self.cont.begin()
            self.in_txn = True
        obj = self.schema.alloc()
        for name, aid in self.attr_ids.items():
            v = values[name]
            if name == "timestamp":
                obj[aid] = (int(v), 0)
            elif self.attr_types[name] == "double":
                obj[aid] = float(v)
            elif self.attr_types[name] == "string":
                obj[aid] = str(v)
            else:
                obj[aid] = int(v)
        rc = obj.index_add()
        obj.release()
        failed = [f"node record: error {rc}"] if rc else []
        for rec in lists:
            rc = self.list_writers[rec["list"]].write(
                comp, values["job_id"], seq, values["timestamp"],
                rec, self.ds, rec["list"])
            if rc:
                failed.append(f"{rec['list']}[{rec['index']}]: error {rc}")
        if failed:
            raise BackendError("; ".join(failed))

    def commit(self):
        if self.in_txn:
            self.cont.commit()
            self.cont.end()
            self.in_txn = False

    def sync(self):
        os.sync()

    def close(self):
        self.cont.close()

    def info(self):
        a = self.ctx.args
        return {"container": a.path, "backend": a.storage}


ADAPTERS = {SosIngest.name: SosIngest}
DEFAULT_ADAPTER = SosIngest.name
