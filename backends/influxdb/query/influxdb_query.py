"""InfluxDB v3 query adapter for the benchmark engine.

Two adapters, selected via QUERY_ADAPTER or DEFAULT_ADAPTER:

  influxdb      Query through the influxdb_client_3 library using Arrow
                Flight. This is the default and the one to use for timings.

  influxdb-http Query through the InfluxDB 3 HTTP query API using urllib.
                No extra dependencies; useful where the Python client is not
                installed or as a cross-check.

Both adapters support two query languages, chosen by INFLUX_QUERY_LANG:
  sql      (default) standard SQL
  influxql InfluxQL

Query sets address these adapters by name or by dialect:
  adapter key:  "influxdb" or "influxdb-http"
  dialect key:  "sql" or "influxql"

Settings (from influxdb_backend.conf / environment):
  INFLUX_HOST         server URL, e.g. http://localhost:8181
  INFLUX_DATABASE     database name
  INFLUX_TOKEN_FILE   path to the JSON token file written by serve-start
  INFLUX_INTERFACE    python (default) or http — selects the default adapter
  INFLUX_QUERY_LANG   sql (default) or influxql
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

from egress_adapter import EgressAdapter, BackendError

_PROBE_SQL      = "SELECT COUNT(*) AS n FROM {measurement}"
_PROBE_INFLUXQL = "SELECT COUNT(seq) FROM {measurement} LIMIT 1"


# ------------------------------------------------------------------ helpers

def _read_token(env):
    """Read the token string from the JSON file written by plugin_serve_start."""
    path = env.get("INFLUX_TOKEN_FILE", "")
    if not path:
        raise BackendError(
            "INFLUX_TOKEN_FILE is not set; was serve-write run?")
    if not os.path.exists(path):
        raise BackendError(
            f"token file not found: {path}; was serve-write run?")
    with open(path) as f:
        return json.load(f)["token"]


def _lang(env):
    lang = env.get("INFLUX_QUERY_LANG", "sql").lower()
    if lang not in ("sql", "influxql"):
        raise BackendError(
            f"INFLUX_QUERY_LANG must be 'sql' or 'influxql', got '{lang}'")
    return lang


def _scalar(v):
    """Convert a pyarrow scalar or numpy type to a plain Python value."""
    if hasattr(v, "as_py"):
        return v.as_py()
    if hasattr(v, "item"):
        return v.item()
    return v


def _row_to_dict(row):
    """Normalise one row to a plain dict with Python scalars."""
    if isinstance(row, dict):
        return {k: _scalar(v) for k, v in row.items()}
    try:
        return {k: _scalar(row[k]) for k in row.keys()}
    except Exception:
        return dict(row)


# ------------------------------------------------------------------ HTTP adapter

class InfluxHttpAdapter(EgressAdapter):
    """Query InfluxDB 3 via the HTTP query API (no client library needed)."""

    name = "influxdb-http"
    dialect = None
    type_overrides = {
        "timestamp": {"column": "time", "literal": "to_timestamp_seconds({})"},
    }

    def __init__(self, ctx):
        super().__init__(ctx)
        env = ctx["env"]
        self.host = env.get("INFLUX_HOST", "http://localhost:8181").rstrip("/")
        self.database = env.get("INFLUX_DATABASE", "monbench")
        self.lang = _lang(env)
        self.dialect = self.lang
        self._token = None

    def _request(self, query):
        url = (f"{self.host}/api/v3/query_sql" if self.lang == "sql"
               else f"{self.host}/api/v3/query_influxql")
        params = {"db": self.database, "q": query, "format": "json"}
        body = urllib.parse.urlencode(params).encode()
        headers = {"Content-Type": "application/x-www-form-urlencoded",
                   "Accept": "application/json",
                   "Authorization": f"Bearer {self._token}"}
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read().decode()
        except urllib.error.HTTPError as e:
            raise BackendError(
                f"HTTP {e.code} from {url}: {e.read().decode(errors='replace')[:300]}")
        except urllib.error.URLError as e:
            raise BackendError(f"cannot reach {url}: {e.reason}")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            raise BackendError(f"non-JSON response from {url}: {raw[:200]}")

    def connect(self):
        self._token = _read_token(self.ctx["env"])
        probe = (_PROBE_SQL if self.lang == "sql" else _PROBE_INFLUXQL).format(
            measurement=self.ctx["schema"])
        try:
            rows = self._request(probe)
        except BackendError:
            raise
        except Exception as e:
            raise BackendError(f"probe query failed: {e}")
        if not rows or rows[0]["n"] == 0:
            raise BackendError(
                f"measurement '{self.ctx['schema']}' not found or empty in "
                f"database '{self.database}' at {self.host}")

    def query(self, text):
        rows = self._request(text)
        if rows:
            self._columns = list(rows[0].keys())
        yield rows

    @staticmethod
    def count(batch):
        return len(batch)

    def records(self, batch):
        cols = list(batch[0].keys()) if batch else []
        return cols, [_row_to_dict(r) for r in batch]


# ------------------------------------------------------------------ Python (Flight) adapter

class InfluxPythonAdapter(EgressAdapter):
    """Query InfluxDB 3 via the influxdb_client_3 library (Arrow Flight)."""

    name = "influxdb"
    dialect = None
    type_overrides = {
        "timestamp": {"column": "time", "literal": "to_timestamp_seconds({})"},
    }

    def __init__(self, ctx):
        super().__init__(ctx)
        env = ctx["env"]
        self.host = env.get("INFLUX_HOST", "http://localhost:8181")
        self.database = env.get("INFLUX_DATABASE", "monbench")
        self.lang = _lang(env)
        self.dialect = self.lang
        self._client = None

    def connect(self):
        try:
            from influxdb_client_3 import InfluxDBClient3
        except ImportError as e:
            raise BackendError(
                f"the influxdb_client_3 module is not available: {e}. "
                f"Use QUERY_ADAPTER=influxdb-http, or pip install influxdb3-python.")

        token = _read_token(self.ctx["env"])
        try:
            self._client = InfluxDBClient3(
                host=self.host, database=self.database, token=token)
        except Exception as e:
            raise BackendError(
                f"cannot create InfluxDB client for {self.host}: {e}")

        probe = (_PROBE_SQL if self.lang == "sql" else _PROBE_INFLUXQL).format(
            measurement=self.ctx["schema"])
        try:
            table = self._client.query(query=probe, language=self.lang, mode="pandas")
        except Exception as e:
            raise BackendError(
                f"probe query for '{self.ctx['schema']}' failed: {e}")
        if table is None or table["n"][0] == 0:
            raise BackendError(
                f"measurement '{self.ctx['schema']}' not found or empty in "
                f"database '{self.database}' at {self.host}")

    def query(self, text):
        if self._client is None:
            raise BackendError("connect() was not called")
        try:
            reader = self._client.query(
                query=text, language=self.lang, mode="chunk")
            for batch, _ in reader:
                yield batch
        except Exception as e:
            raise RuntimeError(f"query failed: {e}") from e

    @staticmethod
    def count(batch):
        return batch.num_rows

    def records(self, batch):
        columns = batch.schema.names
        rows = [{col: _scalar(batch.column(col)[i])
                 for col in columns}
                for i in range(batch.num_rows)]
        return columns, rows

    def close(self):
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None


# ------------------------------------------------------------------ registry

ADAPTERS = {
    InfluxPythonAdapter.name: InfluxPythonAdapter,
    InfluxHttpAdapter.name:   InfluxHttpAdapter,
}

_iface = os.environ.get("INFLUX_INTERFACE", "python").lower()
DEFAULT_ADAPTER = "influxdb" if _iface == "python" else "influxdb-http"
