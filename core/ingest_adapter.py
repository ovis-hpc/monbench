"""ingest_adapter.py -- the interface every backend's ingest adapter implements.

A backend plugin provides backends/<name>/writer/<name>_write.py (the file
name ends in _write.py), which defines one or more subclasses of IngestAdapter
and two module attributes:

    ADAPTERS = {"<adapter-name>": AdapterClass, ...}
    DEFAULT_ADAPTER = "<adapter-name>"

The ingest engine (core/ingest.py) loads that module by file path, so plugins
need no installation and no change to the core. The engine owns everything
that is measured: it generates the records, drives the loop, batches,
throttles, reads the clock and writes the raw result file. An adapter only
calls its backend's API, so no adapter implements timing, checkpoints or the
result format. The query side has the same split (core/egress_adapter.py,
core/egress.py).

The engine times four phases, in this order:

    open    everything done once before the first record: connect, open or
            create the container, add schemas, discover the cluster.
    write   first write() -> the last commit() returns. Record generation is
            part of this phase.
    sync    sync(): make the data durable and visible to readers.
    close   close(): teardown only.

Rules that keep the phases comparable across storage technologies:

  * commit() returns only after the backend has acknowledged the batch.
  * Nothing needed for completeness is left to close().
  * sync() does whatever makes the written data durable and visible, and
    does nothing if the backend has no such step.
"""


class BackendError(Exception):
    """The backend cannot be used: not reachable, wrong type, or refused."""


class IngestAdapter:
    #: Name used with --adapter, and as a key in ADAPTERS.
    name = None
    #: What sync() guarantees, in a few words ("fsync", "replicated", "none").
    #: Copied into the raw result so a reader can tell what the durable
    #: figures mean for this backend.
    durability = ""

    @staticmethod
    def add_arguments(parser):
        """Add this adapter's own command-line options to *parser*."""

    def __init__(self, ctx):
        """ctx has
             args     argparse namespace: the common options (node_index,
                      components, samples, batch, ...) and this adapter's
             dataset  the core/dataset.py Dataset being written
             env      the harness settings (os.environ)
        """
        self.ctx = ctx

    def open(self):
        """One-time setup. Raise BackendError if the backend cannot be used."""
        raise NotImplementedError

    def write(self, comp, seq, values, lists):
        """Store one node record and its list-attribute entries.

        values  Dataset.expected(comp, seq): attribute name -> value.
        lists   Dataset.expected_lists(comp, seq): one dict per entry
                instance ('list', 'index' and the entry's attributes);
                empty for a dataset without list attributes.

        The adapter may buffer. Raise to report the sample as failed; the
        engine counts it and goes on.
        """
        raise NotImplementedError

    def commit(self):
        """Complete everything written since the last commit and return
        once the backend has acknowledged it. Called at the end of every
        batch and once more at the end."""
        raise NotImplementedError

    def sync(self):
        """Make the data durable and visible. May be a no-op."""

    def close(self):
        """Release connections and handles. Not for completeness."""

    def info(self):
        """Extra fields for the raw result file (target, storage, ...)."""
        return {}
