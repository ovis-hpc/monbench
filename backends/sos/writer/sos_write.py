#!/usr/bin/env python3
"""sos_write.py -- write the benchmark data set with the SOS Python bindings.

Produces the data set defined by the DATASET env var (see
core/dataset.py), the same records as sos_write.c, so that containers
written by either writer answer every query identically. The value rules
themselves are not duplicated here: this writer imports core/dataset.py
and calls Dataset.expected() for every record, so it can never drift from
the Python reference.

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
import argparse
import json
import os
import sys
import time

from sosdb import Sos

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "..", "..", "..", "core"))
from dataset import Dataset              # noqa: E402


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--path", required=True)
    p.add_argument("--template", required=True)
    p.add_argument("--node-index", type=int, required=True)
    p.add_argument("--components", type=int, default=16)
    p.add_argument("--samples", type=int, default=5000)
    p.add_argument("--t0", type=int, default=1700000000)
    p.add_argument("--interval", type=int, default=1)
    p.add_argument("--job-len", type=int, default=500)
    p.add_argument("--batch", type=int, default=500,
                   help="node records committed together")
    p.add_argument("--rate", type=float, default=0.0,
                   help="records per second, 0 = unthrottled")
    p.add_argument("--storage", choices=["mmos", "lsos"], default="mmos")
    return p.parse_args()


def open_container(path, backend):
    be = Sos.BE_LSOS if backend == "lsos" else Sos.BE_MMOS
    cont = Sos.Container()
    if not os.path.exists(path):
        cont.create(path, o_mode=0o660, backend=be)
    cont.open(path, o_perm=Sos.PERM_RW)
    return cont


def get_schema(cont, template):
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
    def __init__(self, cont, template, ds, name):
        self.schema = get_schema(cont, template)
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


def main():
    a = parse_args()
    with open(a.template) as f:
        template = json.load(f)

    # Attribute list and types come from the dataset definition, not the
    # SOS schema template -- see backends/sos/README.md's "Write methods".
    ds = Dataset(Dataset.load_definition(os.environ["DATASET"]),
                partitions=1, component_ids=a.components, samples=a.samples,
                t0=a.t0, interval=a.interval, job_len=a.job_len)

    t_start = time.monotonic()
    cont = open_container(a.path, a.storage)
    schema = get_schema(cont, template["node"])
    attr_ids = {n: schema.attr_by_name(n).attr_id() for n in ds.attr_names}
    list_writers = {name: ListWriter(cont, template["lists"][name], ds, name)
                    for name in template["lists"]}

    base = (a.node_index + 1) * 1000
    written = errors = in_batch = list_entries = 0
    checkpoints = [[0, 0]]
    start_epoch = time.time()
    t_write = time.monotonic()

    cont.begin()
    for seq in range(a.samples):
        for c in range(a.components):
            comp = base + c
            values = ds.expected(comp, seq)
            obj = schema.alloc()
            for name in ds.attr_names:
                aid = attr_ids[name]
                v = values[name]
                if name == "timestamp":
                    obj[aid] = (int(v), 0)
                elif ds.attr_type(name) == "double":
                    obj[aid] = float(v)
                elif ds.attr_type(name) == "string":
                    obj[aid] = str(v)
                else:
                    obj[aid] = int(v)
            rc = obj.index_add()
            obj.release()
            if rc:
                errors += 1
                if errors <= 10:
                    print(f"error {rc} indexing record {written}", file=sys.stderr)
            written += 1
            for rec in ds.expected_lists(comp, seq):
                rc = list_writers[rec["list"]].write(
                    comp, values["job_id"], seq, values["timestamp"],
                    rec, ds, rec["list"])
                if not rc:
                    list_entries += 1
                if rc:
                    errors += 1
                    if errors <= 10:
                        print(f"error {rc} indexing {rec['list']}[{rec['index']}] "
                              f"for record {written}", file=sys.stderr)
            in_batch += 1
            if in_batch == a.batch:
                cont.commit()
                cont.end()
                checkpoints.append([round(time.monotonic() - t_write, 6), written])
                in_batch = 0
                cont.begin()
            if a.rate > 0:
                wait = t_write + written / a.rate - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
    cont.commit()
    cont.end()
    t_indexed = time.monotonic()
    if checkpoints[-1][1] != written:
        checkpoints.append([round(t_indexed - t_write, 6), written])
    # The bindings do not export SOS_COMMIT_SYNC, so close() is asynchronous;
    # os.sync() flushes the written pages so the timing is comparable with
    # the C writer's synchronous close.
    cont.close()
    os.sync()
    t_end = time.monotonic()

    print(json.dumps({
        "writer": "py", "container": a.path, "backend": a.storage,
        "node_index": a.node_index, "records": written, "errors": errors,
        "list_entries": list_entries,
        "batch": a.batch, "rate": a.rate,
        "start_epoch_s": round(start_epoch, 6),
        "open_s": round(t_write - t_start, 3),
        "write_s": round(t_indexed - t_write, 3),
        "close_sync_s": round(t_end - t_indexed, 3),
        "checkpoints": checkpoints,
    }))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
