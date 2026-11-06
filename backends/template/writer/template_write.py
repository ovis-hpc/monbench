"""Template ingest adapter. Copy this directory to backends/<name>/writer/ and
rename this file to <name>_write.py.

core/ingest.py loads this file, generates every (component_id, seq) record of
the partition assigned by node.sh (--node-index), and calls the adapter
methods below. The engine does the loop, the batching, the throttling and all
the timing, and writes the raw result file; this adapter only calls the
backend's API. See core/ingest_adapter.py for the contract (the four timed
phases and the rules for commit() and sync()), core/dataset.py for the
Dataset API, and EXTENDING.md for the full extension guide.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "..", "..", "..", "core"))
from ingest_adapter import BackendError, IngestAdapter   # noqa: E402


class TemplateIngest(IngestAdapter):
    name = "template"          # --adapter value
    durability = "none"        # what sync() guarantees

    @staticmethod
    def add_arguments(parser):
        # --target: the storage name for this partition, constructed by
        # plugin_write from ds_name() -- e.g. "monitoring_py".
        parser.add_argument("--target", required=True,
                            help="storage name (measurement/container/table)")
        # Backend-specific options -- add your own (host, port, token, path, ...).

    def open(self):
        # Everything done once before the first record: connect, open or
        # create self.ctx.args.target, create schemas. Raise BackendError if
        # the backend cannot be used.
        raise BackendError("template backend: open() not implemented")

    def write(self, comp, seq, values, lists):
        # Store one node record. values is Dataset.expected(comp, seq): the
        # dataset's attribute names to typed values. lists is
        # Dataset.expected_lists(comp, seq): one dict per list-attribute entry
        # instance, with 'list' (name), 'index' (entry_index) and the entry's
        # own values; empty for a dataset without list attributes, so a simple
        # backend can ignore it. Buffering is fine; raise on failure.
        raise NotImplementedError

    def commit(self):
        # Called at the end of every batch: complete what was written and
        # return once the backend has acknowledged it.
        raise NotImplementedError

    # def sync(self):
    #     Make the data durable and visible, if the backend has such a step.
    #
    # def close(self):
    #     Release connections and handles.
    #
    # def info(self):
    #     Extra fields for the raw result, e.g. {"target": self.ctx.args.target}.


ADAPTERS = {TemplateIngest.name: TemplateIngest}
DEFAULT_ADAPTER = TemplateIngest.name
