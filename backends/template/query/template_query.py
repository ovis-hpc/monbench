"""Template query adapter. Copy this directory to backends/<name>/query/
and rename this file to <name>_query.py.

See core/egress_adapter.py for the interface and backends/README.md for the rules.
"""
from egress_adapter import EgressAdapter, BackendError


class TemplateAdapter(EgressAdapter):
    name = "template"          # QUERY_ADAPTER value, and query-set key
    dialect = "template"       # query language; query sets may key on it

    def connect(self):
        # Connect using settings from self.ctx["env"] (<name>_backend.conf), then
        # confirm that self.ctx["target"] exists and holds self.ctx["schema"]
        # records. Raise BackendError otherwise.
        raise BackendError("template backend: connect() not implemented")

    def query(self, text):
        # Run the query and yield its result in batches, as the client
        # library delivers them.
        raise NotImplementedError

    def records(self, batch):
        # Return (columns, rows), rows being dicts keyed by attribute name.
        raise NotImplementedError


ADAPTERS = {TemplateAdapter.name: TemplateAdapter}
DEFAULT_ADAPTER = TemplateAdapter.name
