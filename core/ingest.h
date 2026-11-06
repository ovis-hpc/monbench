/*
 * ingest.h -- the C ingest engine and the interface a C ingest adapter
 * implements. The C counterpart of core/ingest.py and core/ingest_adapter.py:
 * the engine parses the common command line, loads the data set, generates
 * every record of the partition, drives the loop, batches, throttles, reads
 * the clock and writes the raw result file (same fields as core/ingest.py,
 * see its docstring). An adapter only calls its backend's API; it implements
 * no loop, clock, checkpoint or JSON code.
 *
 * Phases and costs, timed by the engine (record by record for generation and
 * write; see core/ingest.py's docstring for every raw field):
 *
 *   open_s   adapter->open()
 *   write_s  the adapter->write() calls plus the adapter->commit() calls
 *            (record generation is timed apart, as generate_s)
 *   sync_s   adapter->sync()   make the data durable and visible
 *   close_s  adapter->close()  teardown only
 *
 * commit() returns only after the backend has acknowledged the batch, and
 * nothing needed for completeness is left to close().
 *
 * An adapter's main() fills in a struct ingest_adapter and calls
 * ingest_run(). Every callback returns 0 on success; a failed write() counts
 * one failed sample, a failed open/commit/sync/close ends the run.
 */
#ifndef MONBENCH_INGEST_H
#define MONBENCH_INGEST_H

#include <stdint.h>
#include "dataset.h"

struct ingest_opts {
	const char *writer;		/* name recorded in the result */
	const char *output;		/* raw result file */
	int node_index;
	uint64_t components;
	uint64_t samples;
	uint64_t t0;
	uint64_t interval;
	uint64_t job_len;
	uint64_t batch;			/* node records per commit, 0 = one commit */
	double rate;			/* records per second, 0 = unthrottled */
};

/* Extra string fields for the raw result: a {NULL, NULL}-terminated array. */
struct ingest_kv {
	const char *key;
	const char *value;
};

struct ingest_adapter {
	const char *name;		/* recorded as "adapter" */
	const char *durability;		/* what sync() guarantees */
	int (*open)(void *ctx, const dataset_t *ds, const struct ingest_opts *o);
	/* One node record and its list-attribute entries (n_lists may be 0). */
	int (*write)(void *ctx, uint64_t comp, uint64_t seq,
		     const ds_value_t *values,
		     const ds_list_rec_t *lists, int n_lists);
	int (*commit)(void *ctx);
	int (*sync)(void *ctx);		/* may be NULL */
	int (*close)(void *ctx);	/* may be NULL */
	/* Called after close(); may be NULL. */
	const struct ingest_kv *(*info)(void *ctx);
};

/* An adapter's own string option, --<name> VALUE. The value is stored in
 * *value (NULL when absent and not required). */
struct ingest_extra_opt {
	const char *name;
	const char **value;
	int required;
};

/* Parse argv (common options plus the adapter's extra ones, the latter a
 * {NULL}-terminated array or NULL), run the whole ingest and write the raw
 * result. Returns the process exit status: 0, 1 if any sample failed, 2 if
 * the run could not complete. DATASET names the data set definition. */
int ingest_run(int argc, char **argv, const struct ingest_adapter *adapter,
	       void *ctx, const struct ingest_extra_opt *extra);

#endif
