#!/usr/bin/env python3
"""dsos_write.py -- ingest adapter that writes the benchmark data set through
the DSOS API.

Loaded by core/ingest.py, which generates the records, drives the loop and
does all the timing; this adapter only calls the DSOS API. It connects to
the local dsosd via a session file, opens (or creates) a container through the
distributed API, and inserts the same records that sos_write.py would write
locally. This measures the write path through dsosd, including the RPC
overhead.

The container ends up on the local server, stored by dsosd under the
path configured in its directory file.  Because dsosd owns the container,
the schema must be added through the session, not by opening the
container directly.

The records are the data set defined by the DATASET env var (see
core/dataset.py), the same values sos_write.py and sos_write.c write, so
that containers written by any of the three answer every query identically.

If the dataset declares list attributes (design/value-generation.md Sec
6), each one's entry instances are also written through the same dsosd
session, one schema per list attribute -- see sos_write.py's ListWriter
(mirrored here) and backends/sos/README.md's "List attribute data model
mapping". Every attribute type maps onto a real SOS schema type
(backends/sos/make_schema.py's sos_attr()): a string attribute is
written as the fixed-length CHAR_ARRAY the template declares, and an
unsigned 8/16/32/64-bit attribute is written as a plain Python int into
whichever widened SOS integer attribute the template created for it --
nothing is dropped.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "..", "..", "..", "core"))
from ingest_adapter import BackendError, IngestAdapter   # noqa: E402


def get_schema(cont, tmpl, Sos):
    schema = cont.schema_by_name(tmpl["name"])
    if schema is None:
        schema = Sos.Schema()
        schema.from_template(tmpl)
        schema = cont.schema_add(schema)
    return schema


def create_obj(schema, obj):
    try:
        schema.obj_create(obj)
        return 0
    except ValueError as e:
        return str(e) or 1


class ListWriter:
    """Mirrors sos_write.py's ListWriter -- one list attribute's schema
    plus the attr-name -> attr-id map needed to write an entry instance.
    For a record-entries list, *attr_names* is its declared tuple fields
    that the per-list template has a same-named SOS attribute for (every
    one of them, in practice -- see make_schema.py's sos_attr()). For a
    bare-scalar-entries list, there is a single "value" column instead
    (make_schema.py's convert_list())."""
    def __init__(self, cont, tmpl, ds, name, Sos):
        self.schema = get_schema(cont, tmpl, Sos)
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
        obj = self.schema.malloc()
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
        rc = create_obj(self.schema, obj)
        obj.release()
        return rc


class DsosIngest(IngestAdapter):
    name = "dsos"
    durability = "none (not separated from close)"

    @staticmethod
    def add_arguments(p):
        p.add_argument("--session", required=True,
                       help="path to the DSOS session file (one host per line)")
        p.add_argument("--target", required=True,
                       help="container / target name opened through dsosd")
        p.add_argument("--template", required=True,
                       help="SOS schema template JSON file")

    def open(self):
        try:
            from sosdb import Sos
        except ImportError as e:
            raise BackendError(f"the sosdb module is not available: {e}")
        a, ds = self.ctx.args, self.ctx.dataset
        with open(a.template) as f:
            template = json.load(f)
        # Connect to the local dsosd through the session file. PERM_CREAT makes
        # this open-or-create, mirroring sos_write.c's SOS_PERM_CREAT|SOS_PERM_RW
        # -- without it, open() requires the container to already exist, which
        # it never will since dsos_plugin.sh's plugin_write only records the
        # target -> path mapping in dsosd's directory file, it doesn't create
        # anything on disk itself.
        session = Sos.Session(a.session)
        self.cont = session.open(a.target, o_perm=Sos.PERM_RW | Sos.PERM_CREAT)
        self.schema = get_schema(self.cont, template["node"], Sos)
        self.attr_ids = {n: self.schema.attr_by_name(n).attr_id() for n in ds.attr_names}
        self.attr_types = {n: ds.attr_type(n) for n in ds.attr_names}
        self.list_writers = {name: ListWriter(self.cont, template["lists"][name], ds, name, Sos)
                             for name in template["lists"]}
        self.ds = ds
        self.in_txn = False

    def write(self, comp, seq, values, lists):
        if not self.in_txn:
            self.cont.transaction_begin()
            self.in_txn = True
        obj = self.schema.malloc()
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
        rc = create_obj(self.schema, obj)
        obj.release()
        failed = [f"node record: {rc}"] if rc else []
        for rec in lists:
            rc = self.list_writers[rec["list"]].write(
                comp, values["job_id"], seq, values["timestamp"],
                rec, self.ds, rec["list"])
            if rc:
                failed.append(f"{rec['list']}[{rec['index']}]: {rc}")
        if failed:
            raise BackendError("; ".join(failed))

    def commit(self):
        if self.in_txn:
            self.cont.commit()
            self.cont.transaction_end()
            self.in_txn = False

    # sync() stays the no-op default: the DSOS API used here has no separate
    # durability call, so the cost of closing the container is reported as
    # close_s.

    def close(self):
        self.cont.close()

    def info(self):
        return {"target": self.ctx.args.target}


ADAPTERS = {DsosIngest.name: DsosIngest}
DEFAULT_ADAPTER = DsosIngest.name
