/*
 * ingest.c -- the C ingest engine. See ingest.h.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <getopt.h>
#include <time.h>
#include "ingest.h"

#define MAX_ERRORS_SHOWN 10
/* Max entry instances one (comp, seq) sample can produce, across every
 * declared list attribute. Every dataset seen so far declares at most a
 * handful per list attribute (dataset/curated_metrics_like.json: 2 cpu + up
 * to 4 netdev); this leaves room for a wider length range. */
#define MAX_LIST_RECS 256

static double now_s(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

static double now_epoch_s(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* Progress checkpoints: seconds since the write began and samples done so
 * far, one per batch. They are reported raw; core/stats.py derives every
 * rate and percentile from them. */
struct checkpoint {
	double t;
	uint64_t n;
};

struct checkpoints {
	struct checkpoint *v;
	size_t n, cap;
};

static int add_checkpoint(struct checkpoints *c, double t, uint64_t n)
{
	if (c->n == c->cap) {
		size_t cap = c->cap ? c->cap * 2 : 1024;
		struct checkpoint *p = realloc(c->v, cap * sizeof(*p));
		if (!p)
			return -1;
		c->v = p;
		c->cap = cap;
	}
	c->v[c->n].t = t;
	c->v[c->n].n = n;
	c->n++;
	return 0;
}

static void usage(const char *prog, const struct ingest_extra_opt *extra)
{
	fprintf(stderr,
		"usage: %s --writer NAME --output FILE --node-index N\n"
		"          [--components N] [--samples N] [--t0 SECS] [--interval SECS]\n"
		"          [--job-len N] [--batch N] [--rate RECS_PER_SEC]\n", prog);
	for (; extra && extra->name; extra++)
		fprintf(stderr, "          %s--%s VALUE%s\n",
			extra->required ? "" : "[", extra->name,
			extra->required ? "" : "]");
	fprintf(stderr, "       (DATASET env var must name the dataset JSON file)\n");
	exit(2);
}

enum { OPT_WRITER = 1, OPT_OUTPUT, OPT_NODE, OPT_COMPONENTS, OPT_SAMPLES,
       OPT_T0, OPT_INTERVAL, OPT_JOB_LEN, OPT_BATCH, OPT_RATE, OPT_EXTRA };

static int parse_opts(int argc, char **argv, struct ingest_opts *o,
		      const struct ingest_extra_opt *extra)
{
	size_t n_extra = 0, i;
	while (extra && extra[n_extra].name)
		n_extra++;
	struct option *lo = calloc(11 + n_extra + 1, sizeof(*lo));
	if (!lo)
		return -1;
	struct option common[] = {
		{ "writer", required_argument, 0, OPT_WRITER },
		{ "output", required_argument, 0, OPT_OUTPUT },
		{ "node-index", required_argument, 0, OPT_NODE },
		{ "components", required_argument, 0, OPT_COMPONENTS },
		{ "samples", required_argument, 0, OPT_SAMPLES },
		{ "t0", required_argument, 0, OPT_T0 },
		{ "interval", required_argument, 0, OPT_INTERVAL },
		{ "job-len", required_argument, 0, OPT_JOB_LEN },
		{ "batch", required_argument, 0, OPT_BATCH },
		{ "rate", required_argument, 0, OPT_RATE },
	};
	memcpy(lo, common, sizeof(common));
	for (i = 0; i < n_extra; i++) {
		lo[10 + i].name = extra[i].name;
		lo[10 + i].has_arg = required_argument;
		lo[10 + i].val = OPT_EXTRA + (int)i;
	}
	int opt;
	while ((opt = getopt_long(argc, argv, "", lo, NULL)) != -1) {
		switch (opt) {
		case OPT_WRITER: o->writer = optarg; break;
		case OPT_OUTPUT: o->output = optarg; break;
		case OPT_NODE: o->node_index = atoi(optarg); break;
		case OPT_COMPONENTS: o->components = strtoull(optarg, NULL, 0); break;
		case OPT_SAMPLES: o->samples = strtoull(optarg, NULL, 0); break;
		case OPT_T0: o->t0 = strtoull(optarg, NULL, 0); break;
		case OPT_INTERVAL: o->interval = strtoull(optarg, NULL, 0); break;
		case OPT_JOB_LEN: o->job_len = strtoull(optarg, NULL, 0); break;
		case OPT_BATCH: o->batch = strtoull(optarg, NULL, 0); break;
		case OPT_RATE: o->rate = strtod(optarg, NULL); break;
		default:
			if (opt >= OPT_EXTRA) {
				*extra[opt - OPT_EXTRA].value = optarg;
				break;
			}
			free(lo);
			return -1;
		}
	}
	free(lo);
	for (i = 0; i < n_extra; i++)
		if (extra[i].required && !*extra[i].value)
			return -1;
	if (!o->writer || !o->output || o->node_index < 0 || !o->job_len || !o->interval)
		return -1;
	return 0;
}

static void put_json_string(FILE *f, const char *s)
{
	fputc('"', f);
	for (; *s; s++) {
		if (*s == '"' || *s == '\\')
			fputc('\\', f);
		if ((unsigned char)*s >= 0x20)
			fputc(*s, f);
	}
	fputc('"', f);
}

static int save(const struct ingest_adapter *ad, void *ctx,
		const struct ingest_opts *o, uint64_t records,
		uint64_t list_entries, uint64_t errors, double start_epoch,
		double open_s, double write_s, double sync_s, double close_s,
		const struct checkpoints *cps)
{
	char tmp[4096];
	snprintf(tmp, sizeof(tmp), "%s.tmp", o->output);
	FILE *f = fopen(tmp, "w");
	if (!f) {
		perror(tmp);
		return -1;
	}
	fprintf(f, "{\"writer\": ");
	put_json_string(f, o->writer);
	fprintf(f, ", \"adapter\": ");
	put_json_string(f, ad->name);
	fprintf(f, ", \"durability\": ");
	put_json_string(f, ad->durability ? ad->durability : "");
	fprintf(f, ", \"node_index\": %d, \"records\": %" PRIu64
		", \"list_entries\": %" PRIu64 ", \"errors\": %" PRIu64
		", \"batch\": %" PRIu64 ", \"rate\": %.0f, "
		"\"start_epoch_s\": %.6f, \"open_s\": %.6f, \"write_s\": %.6f, "
		"\"sync_s\": %.6f, \"close_s\": %.6f",
		o->node_index, records, list_entries, errors, o->batch, o->rate,
		start_epoch, open_s, write_s, sync_s, close_s);
	const struct ingest_kv *kv = ad->info ? ad->info(ctx) : NULL;
	for (; kv && kv->key; kv++) {
		fprintf(f, ", ");
		put_json_string(f, kv->key);
		fprintf(f, ": ");
		put_json_string(f, kv->value ? kv->value : "");
	}
	fprintf(f, ", \"checkpoints\": [");
	for (size_t k = 0; k < cps->n; k++)
		fprintf(f, "%s[%.6f, %" PRIu64 "]", k ? ", " : "", cps->v[k].t, cps->v[k].n);
	fprintf(f, "]}\n");
	if (fclose(f) || rename(tmp, o->output)) {
		perror(o->output);
		return -1;
	}
	return 0;
}

int ingest_run(int argc, char **argv, const struct ingest_adapter *ad,
	       void *ctx, const struct ingest_extra_opt *extra)
{
	struct ingest_opts o = {
		.node_index = -1, .components = 16, .samples = 5000,
		.t0 = 1700000000, .interval = 1, .job_len = 500, .batch = 500,
	};
	if (parse_opts(argc, argv, &o, extra))
		usage(argv[0], extra);

	const char *dataset_path = getenv("DATASET");
	if (!dataset_path) {
		fprintf(stderr, "error: DATASET env var is not set\n");
		return 2;
	}
	/* The attribute list, types and decode rules come from the dataset
	 * definition, loaded once through core/dataset.c -- the same file the
	 * Python engine loads through core/dataset.py, so every writer
	 * produces identical values for a given dataset. */
	dataset_t ds;
	char ds_err[256];
	if (ds_load(&ds, dataset_path, o.t0, o.interval, o.job_len, ds_err, sizeof(ds_err))) {
		fprintf(stderr, "error loading dataset '%s': %s\n", dataset_path, ds_err);
		return 2;
	}

	int status = 2;
	struct checkpoints cps = { 0 };
	static ds_list_rec_t list_recs[MAX_LIST_RECS];
	uint64_t records = 0, errors = 0, list_entries = 0, in_batch = 0, done = 0;
	double open_s = 0, write_s = 0, sync_s = 0, close_s = 0, start_epoch;
	double t;

	t = now_s();
	if (ad->open(ctx, &ds, &o)) {
		fprintf(stderr, "error: %s adapter: open failed\n", ad->name);
		goto out;
	}
	open_s = now_s() - t;

	uint64_t base = (uint64_t)(o.node_index + 1) * 1000;
	double t_write;
	start_epoch = now_epoch_s();
	t_write = now_s();
	if (add_checkpoint(&cps, 0.0, 0))
		goto nomem;
	for (uint64_t seq = 0; seq < o.samples; seq++) {
		for (uint64_t c = 0; c < o.components; c++) {
			uint64_t comp = base + c;
			ds_value_t values[DS_MAX_ATTRS];
			int n_lists = 0;

			ds_expected(&ds, comp, seq, values);
			if (ds.n_lists) {
				n_lists = ds_expected_lists(&ds, comp, ds_job_id(&ds, seq),
							    seq, list_recs, MAX_LIST_RECS);
				if (n_lists < 0) {
					fprintf(stderr,
						"error: record %" PRIu64 ": more than %d list "
						"entry instances; raise MAX_LIST_RECS\n",
						done, MAX_LIST_RECS);
					goto out;
				}
			}
			if (ad->write(ctx, comp, seq, values, list_recs, n_lists)) {
				errors++;
				if (errors <= MAX_ERRORS_SHOWN)
					fprintf(stderr, "error: %s adapter: record %" PRIu64
						" (%" PRIu64 ", %" PRIu64 ") failed\n",
						ad->name, done, comp, seq);
			} else {
				records++;
				list_entries += n_lists;
			}
			done++;
			if (o.batch && ++in_batch >= o.batch) {
				if (ad->commit(ctx)) {
					fprintf(stderr, "error: %s adapter: commit failed\n", ad->name);
					goto out;
				}
				if (add_checkpoint(&cps, now_s() - t_write, done))
					goto nomem;
				in_batch = 0;
			}
			if (o.rate > 0) {
				double wait = t_write + done / o.rate - now_s();
				if (wait > 0) {
					struct timespec ts = {
						.tv_sec = (time_t)wait,
						.tv_nsec = (long)((wait - (time_t)wait) * 1e9)
					};
					nanosleep(&ts, NULL);
				}
			}
		}
	}
	if (ad->commit(ctx)) {
		fprintf(stderr, "error: %s adapter: commit failed\n", ad->name);
		goto out;
	}
	write_s = now_s() - t_write;
	if (cps.v[cps.n - 1].n != done && add_checkpoint(&cps, write_s, done))
		goto nomem;

	t = now_s();
	if (ad->sync && ad->sync(ctx)) {
		fprintf(stderr, "error: %s adapter: sync failed\n", ad->name);
		goto out;
	}
	sync_s = now_s() - t;

	t = now_s();
	if (ad->close && ad->close(ctx)) {
		fprintf(stderr, "error: %s adapter: close failed\n", ad->name);
		goto out;
	}
	close_s = now_s() - t;

	if (save(ad, ctx, &o, records, list_entries, errors, start_epoch,
		 open_s, write_s, sync_s, close_s, &cps))
		goto out;
	printf("ingest: %" PRIu64 " records, write %.2fs, sync %.2fs, %" PRIu64
	       " errors -> %s\n", records, write_s, sync_s, errors, o.output);
	status = errors ? 1 : 0;
	goto out;
nomem:
	fprintf(stderr, "error: out of memory recording checkpoints\n");
out:
	free(cps.v);
	ds_free(&ds);
	return status;
}
