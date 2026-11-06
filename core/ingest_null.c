/*
 * ingest_null -- C ingest adapter that stores nothing. It measures the C
 * ingest engine itself (record generation and the loop): the floor under
 * every other C adapter's figures. Build with 'make -C core ingest_null'.
 */
#include "ingest.h"

static int null_open(void *ctx, const dataset_t *ds, const struct ingest_opts *o)
{
	(void)ctx; (void)ds; (void)o;
	return 0;
}

static int null_write(void *ctx, uint64_t comp, uint64_t seq,
		      const ds_value_t *values,
		      const ds_list_rec_t *lists, int n_lists)
{
	(void)ctx; (void)comp; (void)seq; (void)values; (void)lists; (void)n_lists;
	return 0;
}

static int null_commit(void *ctx)
{
	(void)ctx;
	return 0;
}

int main(int argc, char **argv)
{
	static const struct ingest_adapter adapter = {
		.name = "null",
		.durability = "none",
		.open = null_open,
		.write = null_write,
		.commit = null_commit,
	};
	return ingest_run(argc, argv, &adapter, NULL, NULL);
}
