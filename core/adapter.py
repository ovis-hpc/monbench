"""adapter.py -- the interface every backend's query adapter implements.

A backend plugin provides backends/<name>/query/<name>_query.py, which defines one or more
subclasses of Adapter and two module attributes:

    ADAPTERS = {"<adapter-name>": AdapterClass, ...}
    DEFAULT_ADAPTER = "<adapter-name>"

The engine (core/bench.py) loads that module by file path, so plugins need no
installation and no change to the core.
"""


class BackendError(Exception):
    """The backend cannot be used: not reachable, wrong type, or no data."""


class Adapter:
    #: Name used in QUERY_ADAPTER and as a key in query-set files.
    name = None
    #: Query language. Query sets may give one string per dialect, so
    #: adapters that share a language share query strings.
    dialect = None
    #: Map dataset attribute *types* to backend-specific overrides.
    #: Keys are type names from the dataset definition (e.g. "timestamp",
    #: "uint64", "string").  Each value is a dict with optional keys:
    #:   "column"  — the backend's column name for attributes of this type
    #:              (e.g. InfluxDB always stores the timestamp as ``time``)
    #:   "literal" — a format string applied to placeholder values of this
    #:              type (e.g. ``"to_timestamp({})"`` for InfluxDB timestamps)
    #: The engine resolves which attributes are affected using the dataset
    #: definition, so query strings and the verifier always use dataset
    #: names and bare values; each backend only declares its conventions
    #: by type.
    type_overrides = None

    def __init__(self, ctx):
        """ctx: dict with
             target     storage target name (container, measurement, ...).
                        Populated by bench.py from the backend plugin's meta
                        file at OUTPUT_DIR/meta/ingest-handoff-<writer>.json,
                        which the plugin writes during the write phase. The
                        query adapter does not need to know how the name was
                        chosen.
             schema     data-set name (the record type), from DS.name
             servers    list of server hosts from SERVER_HOSTS
             output_dir run output directory on this node (with meta/,
                        nodes/<hostname>/, results/, logs/ subdirectories)
             env        the harness settings (os.environ)
        """
        self.ctx = ctx

    def connect(self):
        """Connect and confirm the backend holds the target's data.

        Raise BackendError if it does not: that stops the run. Not timed.
        """
        raise NotImplementedError

    def query(self, text):
        """Run one query; yield its result in batches.

        The engine times from the call until the last batch has been
        consumed, so do only what a normal client of this backend would do.
        """
        raise NotImplementedError

    @staticmethod
    def count(batch):
        """Number of rows in a batch. Called inside the timed region."""
        return len(batch)

    def records(self, batch):
        """Return (columns, rows) for a batch; rows are dicts keyed by
        attribute name. Used for verification only; not timed."""
        raise NotImplementedError

    def postprocess(self, text, columns, rows):
        """Backend-specific shaping of a query's full, assembled result,
        after every batch has been merged and column names reversed back
        to dataset attribute names (core/bench.py's collect()) -- used
        for verification only, like records(), and likewise not timed.

        Default: no-op (most backends need none). Exists for a backend
        whose native query language cannot express something another
        backend's query *text* can (e.g. a JOIN or DISTINCT): rather than
        inventing non-native syntax, such a backend's query text can
        target a simpler, real native query, and this hook does the rest
        of the assembly in the adapter's own code -- the same thing a
        normal client without that language feature would have to do
        itself. See backends/sos/query/sos_query.py's and
        backends/dsos/query/dsos_query.py's postprocess() for a concrete
        case: collapsing multiple matching list-attribute entry instances
        of one node sample into the one node-shaped row design/
        value-generation.md Sec 6.5's name[*].attr existential condition
        expects, which the SOS/DSOS query language has no DISTINCT/JOIN
        to do natively.

        Only called from the untimed verification path (collect()), not
        from the timed one (timed_run()), so it never affects a timing
        measurement; a backend that needs the same shaping to get a
        *correct* row count for a timed run already has to do it inside
        query()/count() themselves, as query()'s own docstring says.
        """
        return rows

    def close(self):
        pass
