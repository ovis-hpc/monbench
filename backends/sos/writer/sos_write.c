/*
 * sos_write -- write a deterministic data set into a local SOS
 * container using the SOS C API.
 *
 * Every record is a pure function of (component_id, seq), so any reader
 * can verify a record without knowing which node wrote it. component_id,
 * timestamp, job_id and seq follow:
 *
 *   component_id = (node_index + 1) * 1000 + c        c in [0, components)
 *   timestamp    = t0 + seq * interval                seq in [0, samples)
 *   job_id       = 1 + seq / job_len
 *
 * Every other attribute's value comes from the dataset named by the
 * DATASET env var, loaded and evaluated through core/dataset.h -- see
 * that header's own comment for why this writer has no JSON-parsing or
 * attribute-formula code of its own.
 *
 * If the dataset declares list attributes (design/value-generation.md
 * Sec 6), each one's entry instances are also written, one schema per
 * list attribute -- see struct list_writer below and
 * backends/sos/README.md's "List attribute data model mapping".
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <getopt.h>
#include <time.h>
#include <uuid/uuid.h>
#include <jansson.h>
#include <sos/sos.h>
#include "dataset.h"

#define MAX_JOIN_ATTRS 16
/* Max entry instances a single (comp, seq) sample can produce, across
 * every declared list attribute -- see write_list_entry()'s caller in
 * main(). Every dataset JSON seen so far declares at most a handful per
 * list attribute (dataset/curated_metrics_like.json: 2 cpu + up to 4
 * netdev); this is comfortably larger, with room for a list attribute
 * declaring a wider length range. */
#define MAX_LIST_RECS 256

struct opts {
	const char *path;
	const char *template_path;
	const char *backend;
	int node_index;
	uint64_t components;
	uint64_t samples;
	uint64_t t0;
	uint64_t interval;
	uint64_t job_len;
	uint64_t batch;
	double rate;		/* records per second, 0 = unthrottled */
};

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s --path PATH --template JSON --node-index N\n"
		"          [--components N] [--samples N] [--t0 SECS] [--interval SECS]\n"
		"          [--job-len N] [--batch N] [--rate RECS_PER_SEC]\n"
		"          [--storage mmos|lsos]\n"
		"       (DATASET env var must name the dataset JSON file)\n", prog);
	exit(2);
}

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

/* Progress checkpoints: seconds since the write began and node records
 * written so far, one per batch. They are reported raw; core/stats.py
 * derives every rate and percentile from them. */
struct checkpoint {
	double t;
	uint64_t n;
};
static struct checkpoint *cps;
static size_t n_cps, cap_cps;

static void add_checkpoint(double t, uint64_t n)
{
	if (n_cps == cap_cps) {
		size_t cap = cap_cps ? cap_cps * 2 : 1024;
		struct checkpoint *p = realloc(cps, cap * sizeof(*p));
		if (!p) {
			fprintf(stderr, "error: out of memory recording checkpoints\n");
			exit(1);
		}
		cps = p;
		cap_cps = cap;
	}
	cps[n_cps].t = t;
	cps[n_cps].n = n;
	n_cps++;
}

static const struct {
	const char *name;
	sos_type_t type;
} type_map[] = {
	{ "INT16", SOS_TYPE_INT16 },	 { "INT32", SOS_TYPE_INT32 },
	{ "INT64", SOS_TYPE_INT64 },	 { "UINT16", SOS_TYPE_UINT16 },
	{ "UINT32", SOS_TYPE_UINT32 },	 { "UINT64", SOS_TYPE_UINT64 },
	{ "FLOAT", SOS_TYPE_FLOAT },	 { "DOUBLE", SOS_TYPE_DOUBLE },
	{ "TIMESTAMP", SOS_TYPE_TIMESTAMP }, { "JOIN", SOS_TYPE_JOIN },
	/* A string attribute (backends/sos/make_schema.py's sos_attr())
	 * becomes a fixed-length character array -- the template's "size"
	 * field (schema_from_template_obj() below) gives its length. */
	{ "CHAR_ARRAY", SOS_TYPE_CHAR_ARRAY },
};

static int lookup_type(const char *name, sos_type_t *type)
{
	size_t i;
	for (i = 0; i < sizeof(type_map) / sizeof(type_map[0]); i++) {
		if (0 == strcasecmp(name, type_map[i].name)) {
			*type = type_map[i].type;
			return 0;
		}
	}
	return EINVAL;
}

/*
 * Build a schema from one already-parsed template object (the same shape
 * the Python Schema.from_template() method accepts: {"name", "uuid",
 * "attrs"}), so that both writers produce an identical schema.
 *
 * Takes an already-parsed json_t* rather than loading its own file,
 * because the template file now holds more than one such object --
 * {"node": {...}, "lists": {"<name>": {...}, ...}} (see
 * backends/sos/make_schema.py) -- loaded once by main() into a single
 * json_t* tree that this function's caller slices up.
 */
static sos_schema_t schema_from_template_obj(json_t *tmpl)
{
	json_t *attrs, *attr;
	size_t i;
	uuid_t uuid;
	sos_schema_t schema = NULL;
	int rc;

	const char *name = json_string_value(json_object_get(tmpl, "name"));
	const char *uuid_str = json_string_value(json_object_get(tmpl, "uuid"));
	attrs = json_object_get(tmpl, "attrs");
	if (!name || !uuid_str || !json_is_array(attrs)) {
		fprintf(stderr, "error: template needs 'name', 'uuid' and 'attrs'\n");
		goto out;
	}
	if (uuid_parse(uuid_str, uuid)) {
		fprintf(stderr, "error: invalid uuid '%s'\n", uuid_str);
		goto out;
	}
	schema = sos_schema_create(name, uuid);
	if (!schema) {
		fprintf(stderr, "error %d: sos_schema_create\n", errno);
		goto out;
	}
	json_array_foreach(attrs, i, attr) {
		const char *aname = json_string_value(json_object_get(attr, "name"));
		const char *tname = json_string_value(json_object_get(attr, "type"));
		sos_type_t type;
		if (!aname || !tname || lookup_type(tname, &type)) {
			fprintf(stderr, "error: bad attribute %zu in template\n", i);
			goto err;
		}
		if (type == SOS_TYPE_JOIN) {
			char *join_list[MAX_JOIN_ATTRS];
			json_t *jattrs = json_object_get(attr, "join_attrs");
			size_t j, count = json_array_size(jattrs);
			if (!count || count > MAX_JOIN_ATTRS) {
				fprintf(stderr, "error: join '%s' needs 1..%d join_attrs\n",
					aname, MAX_JOIN_ATTRS);
				goto err;
			}
			for (j = 0; j < count; j++)
				join_list[j] = (char *)json_string_value(json_array_get(jattrs, j));
			rc = sos_schema_attr_add(schema, aname, type, count, join_list);
		} else {
			/* "size" is set for a CHAR_ARRAY attribute (a string's
			 * fixed length -- backends/sos/make_schema.py's
			 * sos_attr()); every other type ignores it and takes
			 * the previous, unsized form. */
			json_t *size_j = json_object_get(attr, "size");
			size_t size = size_j ? (size_t)json_integer_value(size_j) : 0;
			rc = sos_schema_attr_add(schema, aname, type, size);
		}
		if (rc) {
			fprintf(stderr, "error %d adding attribute '%s'\n", rc, aname);
			goto err;
		}
		json_t *idx = json_object_get(attr, "index");
		if (!idx)
			continue;
		const char *idx_type = json_string_value(json_object_get(idx, "type"));
		const char *idx_key = json_string_value(json_object_get(idx, "key"));
		const char *idx_args = json_string_value(json_object_get(idx, "args"));
		rc = sos_schema_index_add(schema, aname);
		if (!rc)
			rc = sos_schema_index_modify(schema, aname,
						     idx_type ? idx_type : "BXTREE",
						     idx_key, idx_args);
		if (rc) {
			fprintf(stderr, "error %d indexing attribute '%s'\n", rc, aname);
			goto err;
		}
	}
	goto out;
err:
	sos_schema_free(schema);
	schema = NULL;
out:
	/* tmpl is owned by the caller (it is a slice of main()'s single
	 * parsed template tree); this function does not decref it. */
	return schema;
}

/* Get or create, in *sos*, the schema described by the already-parsed
 * template object *tmpl* ({"name", "uuid", "attrs"}) -- used for both the
 * node schema (tmpl = root["node"]) and each list attribute's schema
 * (tmpl = root["lists"][name]). */
static sos_schema_t get_schema_obj(sos_t sos, json_t *tmpl)
{
	const char *name = json_string_value(json_object_get(tmpl, "name"));
	if (!name) {
		fprintf(stderr, "error: template missing 'name'\n");
		return NULL;
	}
	sos_schema_t schema = sos_schema_by_name(sos, name);
	if (!schema) {
		sos_schema_t new_schema = schema_from_template_obj(tmpl);
		if (!new_schema)
			return NULL;
		int rc = sos_schema_add(sos, new_schema);
		if (rc) {
			fprintf(stderr, "error %d adding schema '%s'\n", rc, name);
			return NULL;
		}
		schema = sos_schema_by_name(sos, name);
	}
	return schema;
}

/* set_u16/set_u32/set_u64 each write to the union member sized to match
 * the SOS attribute's own declared width -- required because an object's
 * on-disk/in-memory layout allocates exactly that attribute's declared
 * size (e.g. 2 bytes for a UINT16), so writing the wider prim.uint64_
 * member for anything narrower than 8 bytes would write past the
 * attribute's storage into whatever follows it in the object. Dataset
 * uint8/uint16 attributes both become a SOS UINT16 (make_schema.py's
 * sos_attr() -- SOS has no UINT8), so both use set_u16; uint32 uses
 * set_u32; uint64 uses set_u64. See set_value() below, which picks the
 * matching one by the attribute's type_str. */
static inline void set_u16(sos_obj_t obj, sos_attr_t attr, uint16_t v)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.uint16_ = v;
	sos_value_put(val);
}

static inline void set_u32(sos_obj_t obj, sos_attr_t attr, uint32_t v)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.uint32_ = v;
	sos_value_put(val);
}

static inline void set_u64(sos_obj_t obj, sos_attr_t attr, uint64_t v)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.uint64_ = v;
	sos_value_put(val);
}

/* set_i16/set_i32/set_i64 are the signed analogs of set_u16/set_u32/
 * set_u64 above, same width-matching rationale: a dataset int8/int16
 * attribute becomes a SOS INT16 (make_schema.py's sos_attr() -- SOS has
 * no INT8 either), so both share set_i16; int32 uses set_i32; int64 uses
 * set_i64. */
static inline void set_i16(sos_obj_t obj, sos_attr_t attr, int16_t v)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.int16_ = v;
	sos_value_put(val);
}

static inline void set_i32(sos_obj_t obj, sos_attr_t attr, int32_t v)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.int32_ = v;
	sos_value_put(val);
}

static inline void set_i64(sos_obj_t obj, sos_attr_t attr, int64_t v)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.int64_ = v;
	sos_value_put(val);
}

static inline void set_double(sos_obj_t obj, sos_attr_t attr, double v)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.double_ = v;
	sos_value_put(val);
}

static inline void set_ts(sos_obj_t obj, sos_attr_t attr, uint64_t secs)
{
	struct sos_value_s v_;
	sos_value_t val = sos_value_init(&v_, obj, attr);
	val->data->prim.timestamp_.tv.tv_sec = (uint32_t)secs;
	val->data->prim.timestamp_.tv.tv_usec = 0;
	sos_value_put(val);
}

/* Write a NUL-terminated string into a fixed-length CHAR_ARRAY attribute
 * (backends/sos/make_schema.py's sos_attr() "size" field gave it exactly
 * *len* characters, matching the attribute's declared "length" -- the
 * same len ds_attr_t.length carries, so the two always agree).
 *
 * CHAR_ARRAY is a SOS *array* type, and an array attribute's storage is
 * not part of the fixed-size object layout sos_obj_new() allocates --
 * sos_value_init() on a freshly-allocated object returns NULL for it
 * (sos_value.c checks ref_val->array_info.offset == 0 and bails). The
 * correct API for writing into an array attribute is sos_array_new(),
 * which allocates (reallocating the object as needed) and returns a
 * valid sos_value_t pointing at that storage -- see sos.c's
 * sos_obj_attr_value_set_va() and the LDMS store_sos plugin's
 * CHAR_ARRAY handling, both of which call sos_array_new() before
 * writing, never sos_value_init(). sos_array_new() sets
 * val->data->array.count itself (mirrors sos_value_data_new()'s array
 * path in sos_value.c), so this function only needs to copy the bytes
 * into val->data->array.data.char_ once allocation succeeds. */
static inline int set_char_array(sos_obj_t obj, sos_attr_t attr,
                                  const char *s, size_t len)
{
	struct sos_value_s v_;
	sos_value_t val = sos_array_new(&v_, attr, obj, len);
	if (!val) {
		fprintf(stderr, "error %d allocating char array (len %zu)\n",
			errno, len);
		return -1;
	}
	memcpy(val->data->array.data.char_, s, len);
	sos_value_put(val);
	return 0;
}

static sos_attr_t must_attr(sos_schema_t schema, const char *name)
{
	sos_attr_t attr = sos_schema_attr_by_name(schema, name);
	if (!attr) {
		fprintf(stderr, "error: schema has no attribute '%s'\n", name);
		exit(1);
	}
	return attr;
}

/* One non-identity dataset attribute, resolved to both its SOS attr
 * handle and the index ds_expected() will fill its value at -- built
 * once from the loaded dataset_t, used by every record. *attr* is kept
 * so set_value() can dispatch on its exact type_str too -- needed to
 * tell a SOS-UINT16-via-uint8 attribute from a SOS-UINT16-via-uint16
 * one, a SOS-INT16-via-int8 from a SOS-INT16-via-int16, etc. */
struct value_attr {
	sos_attr_t sos_attr;
	size_t idx;		/* index into dataset_t.attrs[] / ds_expected()'s values[],
				 * or (for a list attribute's record entries) into
				 * ds_list_t.tuples[] / a ds_list_rec_t's values[] */
	const ds_attr_t *attr;
};

/* Write one resolved value attribute's value, dispatching on *da*'s
 * family and (for DS_FAMILY_UINT/DS_FAMILY_INT) exact declared width --
 * the single place both the flat write loop and write_list_entry() go
 * through, so the two can never diverge on how a given family/width is
 * written. Only ever called for a UINT/INT/DOUBLE/STRING attribute (see
 * the flat resolution loop in main() and build_list_writer(), which are
 * what decide which families get a value_attr at all; DS_FAMILY_TIMESTAMP
 * never does, since the identity timestamp attribute is written directly
 * via set_ts(), not through this dispatcher).
 *
 * Returns 0 on success, -1 if the underlying SOS write failed (currently
 * only possible for DS_FAMILY_STRING, if sos_array_new() fails -- see
 * set_char_array()); callers count that like any other write error. */
static int set_value(sos_obj_t obj, const ds_attr_t *da, sos_attr_t sa,
                       const ds_value_t *v)
{
	switch (da->family) {
	case DS_FAMILY_UINT:
		if (strcasecmp(da->type_str, "uint32") == 0)
			set_u32(obj, sa, (uint32_t)v->u64);
		else if (strcasecmp(da->type_str, "uint64") == 0)
			set_u64(obj, sa, v->u64);
		else	/* uint8 or uint16: both map to a SOS UINT16 */
			set_u16(obj, sa, (uint16_t)v->u64);
		return 0;
	case DS_FAMILY_INT:
		if (strcasecmp(da->type_str, "int32") == 0)
			set_i32(obj, sa, (int32_t)v->i64);
		else if (strcasecmp(da->type_str, "int64") == 0)
			set_i64(obj, sa, v->i64);
		else	/* int8 or int16: both map to a SOS INT16 */
			set_i16(obj, sa, (int16_t)v->i64);
		return 0;
	case DS_FAMILY_DOUBLE:
		set_double(obj, sa, v->d);
		return 0;
	case DS_FAMILY_STRING:
		return set_char_array(obj, sa, v->str, da->length);
	case DS_FAMILY_TIMESTAMP:
	default:
		/* Unreachable: never resolved into a value_attr (see this
		 * function's doc comment). */
		return 0;
	}
}

/* One list attribute's schema plus everything needed to write one entry
 * instance of it, built once from the loaded dataset_t and the matching
 * slice of the parsed template tree -- the per-list analog of the flat
 * schema/value_attrs[] pair above. For a record-entries list,
 * value_attrs indexes into a ds_list_rec_t's values[] in list->tuples
 * order; for a bare-scalar-entries list, a_value/scalar_entry write the
 * single "value" column from values[0] instead (see
 * backends/sos/make_schema.py's convert_list()). */
struct list_writer {
	sos_schema_t schema;
	sos_attr_t a_ts, a_comp, a_job, a_seq, a_idx;
	int is_record;
	struct value_attr value_attrs[DS_MAX_ATTRS];
	size_t n_value_attrs;
	sos_attr_t a_value;		/* !is_record only */
	const ds_attr_t *scalar_entry;	/* !is_record only */
};

/* Resolve one list attribute's schema and value attrs. *list_tmpl* is
 * root["lists"][list->name] from main()'s parsed template tree -- built
 * by backends/sos/make_schema.py's convert_list(), which gives every
 * entry attribute a real SOS type, signed integers included. Every
 * tuple field (or the bare-scalar "value" column) gets a value_attr
 * here and is written through set_value(), the same dispatcher the flat
 * loop uses. */
static int build_list_writer(sos_t sos, json_t *list_tmpl, const ds_list_t *list,
                              struct list_writer *lw)
{
	size_t ai;

	lw->schema = get_schema_obj(sos, list_tmpl);
	if (!lw->schema)
		return -1;
	lw->a_ts = must_attr(lw->schema, "timestamp");
	lw->a_comp = must_attr(lw->schema, "component_id");
	lw->a_job = must_attr(lw->schema, "job_id");
	lw->a_seq = must_attr(lw->schema, "seq");
	lw->a_idx = must_attr(lw->schema, "entry_index");

	lw->is_record = list->is_record;
	lw->n_value_attrs = 0;
	if (lw->is_record) {
		for (ai = 0; ai < list->n_tuples; ai++) {
			const ds_attr_t *da = &list->tuples[ai];
			sos_attr_t sa = sos_schema_attr_by_name(lw->schema, da->name);
			if (!sa)
				continue;	/* defensive: should always be present */
			lw->value_attrs[lw->n_value_attrs].sos_attr = sa;
			lw->value_attrs[lw->n_value_attrs].idx = ai;
			lw->value_attrs[lw->n_value_attrs].attr = da;
			lw->n_value_attrs++;
		}
	} else {
		lw->a_value = must_attr(lw->schema, "value");
		lw->scalar_entry = &list->scalar_entry;
	}
	return 0;
}

/* Write one entry instance (a ds_expected_lists() record) into its list
 * attribute's schema -- the per-list analog of the flat write loop's
 * body below. Returns non-zero on failure: either sos_obj_index()'s rc,
 * or -1 if any of this entry's values failed to write (see set_value()),
 * checked before indexing so a failed value write is never silently
 * indexed as if it had succeeded. */
static int write_list_entry(struct list_writer *lw, uint64_t comp, uint64_t job_id,
                             uint64_t seq, uint64_t ts, const ds_list_rec_t *rec)
{
	size_t vi;
	int value_err = 0;
	sos_obj_t obj = sos_obj_new(lw->schema);
	if (!obj) {
		fprintf(stderr, "error %d allocating list object\n", errno);
		return -1;
	}
	set_ts(obj, lw->a_ts, ts);
	set_u64(obj, lw->a_comp, comp);
	set_u64(obj, lw->a_job, job_id);
	set_u64(obj, lw->a_seq, seq);
	set_u64(obj, lw->a_idx, rec->index);
	if (lw->is_record) {
		for (vi = 0; vi < lw->n_value_attrs; vi++) {
			const struct value_attr *va = &lw->value_attrs[vi];
			if (set_value(obj, va->attr, va->sos_attr, &rec->values[va->idx]))
				value_err = -1;
		}
	} else {
		if (set_value(obj, lw->scalar_entry, lw->a_value, &rec->values[0]))
			value_err = -1;
	}
	int rc = sos_obj_index(obj);
	sos_obj_put(obj);
	return value_err ? value_err : rc;
}

int main(int argc, char **argv)
{
	struct opts o = {
		.backend = "mmos", .node_index = -1, .components = 16,
		.samples = 5000, .t0 = 1700000000, .interval = 1,
		.job_len = 500, .batch = 500, .rate = 0,
	};
	static struct option long_opts[] = {
		{ "path", required_argument, 0, 'p' },
		{ "template", required_argument, 0, 'T' },
		{ "node-index", required_argument, 0, 'n' },
		{ "components", required_argument, 0, 'c' },
		{ "samples", required_argument, 0, 's' },
		{ "t0", required_argument, 0, 't' },
		{ "interval", required_argument, 0, 'i' },
		{ "job-len", required_argument, 0, 'j' },
		{ "batch", required_argument, 0, 'b' },
		{ "rate", required_argument, 0, 'r' },
		{ "storage", required_argument, 0, 'B' },
		{ 0, 0, 0, 0 }
	};
	int opt;
	while ((opt = getopt_long(argc, argv, "", long_opts, NULL)) != -1) {
		switch (opt) {
		case 'p': o.path = optarg; break;
		case 'T': o.template_path = optarg; break;
		case 'n': o.node_index = atoi(optarg); break;
		case 'c': o.components = strtoull(optarg, NULL, 0); break;
		case 's': o.samples = strtoull(optarg, NULL, 0); break;
		case 't': o.t0 = strtoull(optarg, NULL, 0); break;
		case 'i': o.interval = strtoull(optarg, NULL, 0); break;
		case 'j': o.job_len = strtoull(optarg, NULL, 0); break;
		case 'b': o.batch = strtoull(optarg, NULL, 0); break;
		case 'r': o.rate = strtod(optarg, NULL); break;
		case 'B': o.backend = optarg; break;
		default: usage(argv[0]);
		}
	}
	if (!o.path || !o.template_path || o.node_index < 0 ||
	    !o.batch || !o.job_len || !o.interval)
		usage(argv[0]);

	const char *dataset_path = getenv("DATASET");
	if (!dataset_path) {
		fprintf(stderr, "error: DATASET env var is not set\n");
		return 1;
	}
	/* The authoritative attribute list, types, and decode rules come
	 * from the dataset definition (DATASET env var) -- loaded and
	 * resolved once here via core/dataset.c, the same file
	 * sos_write.py, dsos_write.py and influx_write.py all load
	 * through core/dataset.py, so every writer produces identical
	 * values for a given dataset with no change needed here when the
	 * dataset's JSON changes. */
	dataset_t ds;
	char ds_err[256];
	if (ds_load(&ds, dataset_path, o.t0, o.interval, o.job_len, ds_err, sizeof(ds_err))) {
		fprintf(stderr, "error loading dataset '%s': %s\n", dataset_path, ds_err);
		return 1;
	}

	sos_perm_t perm = SOS_PERM_CREAT | SOS_PERM_RW;
	perm |= (0 == strcasecmp(o.backend, "lsos")) ? SOS_BE_LSOS : SOS_BE_MMOS;

	double t_start = now_s();
	sos_t sos = sos_container_open(o.path, perm, 0660);
	if (!sos) {
		fprintf(stderr, "error %d opening container '%s'\n", errno, o.path);
		return 1;
	}
	/* The template file now holds the node schema plus one schema per
	 * declared list attribute (see backends/sos/make_schema.py's
	 * build_templates()): {"node": {...}, "lists": {<name>: {...}}}.
	 * Loaded once here into a single parsed tree; get_schema_obj()/
	 * build_list_writer() below take slices of it directly rather than
	 * each re-opening the file. */
	json_error_t tmpl_jerr;
	json_t *tmpl_root = json_load_file(o.template_path, 0, &tmpl_jerr);
	if (!tmpl_root) {
		fprintf(stderr, "error: %s:%d: %s\n", o.template_path,
			tmpl_jerr.line, tmpl_jerr.text);
		return 1;
	}
	json_t *node_tmpl = json_object_get(tmpl_root, "node");
	json_t *lists_tmpl = json_object_get(tmpl_root, "lists");
	if (!node_tmpl) {
		fprintf(stderr, "error: %s: missing 'node'\n", o.template_path);
		return 1;
	}

	sos_schema_t schema = get_schema_obj(sos, node_tmpl);
	if (!schema)
		return 1;
	sos_attr_t a_ts = must_attr(schema, "timestamp");
	sos_attr_t a_comp = must_attr(schema, "component_id");
	sos_attr_t a_job = must_attr(schema, "job_id");
	sos_attr_t a_seq = must_attr(schema, "seq");
	int idx_ts = ds_attr_index(&ds, "timestamp");
	int idx_comp = ds_attr_index(&ds, "component_id");
	int idx_job = ds_attr_index(&ds, "job_id");
	int idx_seq = ds_attr_index(&ds, "seq");

	/* Resolve every other dataset attribute to its SOS attr handle
	 * once, up front -- see the struct value_attr comment. Every
	 * family (including signed integers) is resolved here and written
	 * through set_value(). */
	struct value_attr value_attrs[DS_MAX_ATTRS];
	size_t n_value_attrs = 0;
	size_t ai;
	for (ai = 0; ai < ds.n_attrs; ai++) {
		const ds_attr_t *da = &ds.attrs[ai];
		if (da->is_identity)
			continue;
		value_attrs[n_value_attrs].sos_attr = must_attr(schema, da->name);
		value_attrs[n_value_attrs].idx = ai;
		value_attrs[n_value_attrs].attr = da;
		n_value_attrs++;
	}

	/* Resolve each declared list attribute's schema and value attrs
	 * once, up front, same as the flat attrs above -- see struct
	 * list_writer and build_list_writer(). */
	struct list_writer list_writers[DS_MAX_LISTS];
	size_t li;
	for (li = 0; li < ds.n_lists; li++) {
		const ds_list_t *list = &ds.lists[li];
		json_t *list_tmpl = lists_tmpl
			? json_object_get(lists_tmpl, list->name) : NULL;
		if (!list_tmpl) {
			fprintf(stderr,
				"error: %s: 'lists' has no template for list attribute "
				"'%s' (DATASET and the template file must come from "
				"the same dataset JSON, via make_schema.py)\n",
				o.template_path, list->name);
			return 1;
		}
		if (build_list_writer(sos, list_tmpl, list, &list_writers[li]))
			return 1;
	}

	uint64_t base = (uint64_t)(o.node_index + 1) * 1000;
	uint64_t written = 0, in_batch = 0, errors = 0, list_entries = 0;
	uint64_t seq, c;
	int rc;
	double start_epoch = now_epoch_s();
	double t_write = now_s();
	add_checkpoint(0.0, 0);

	rc = sos_begin_x(sos);
	if (rc) {
		fprintf(stderr, "error %d beginning transaction\n", rc);
		return 1;
	}
	for (seq = 0; seq < o.samples; seq++) {
		for (c = 0; c < o.components; c++) {
			uint64_t comp = base + c;
			ds_value_t values[DS_MAX_ATTRS];
			size_t vi;
			int value_err = 0;

			ds_expected(&ds, comp, seq, values);

			sos_obj_t obj = sos_obj_new(schema);
			if (!obj) {
				fprintf(stderr, "error %d allocating object\n", errno);
				return 1;
			}
			set_ts(obj, a_ts, values[idx_ts].u64);
			set_u64(obj, a_comp, values[idx_comp].u64);
			set_u64(obj, a_job, values[idx_job].u64);
			set_u64(obj, a_seq, values[idx_seq].u64);
			for (vi = 0; vi < n_value_attrs; vi++) {
				const struct value_attr *va = &value_attrs[vi];
				if (set_value(obj, va->attr, va->sos_attr, &values[va->idx]))
					value_err = 1;
			}
			rc = sos_obj_index(obj);
			sos_obj_put(obj);
			if (rc || value_err) {
				errors++;
				if (errors <= 10)
					fprintf(stderr, "error %d indexing record %" PRIu64 "%s\n",
						rc, written,
						value_err ? " (value write failed)" : "");
			}
			written++;

			if (ds.n_lists) {
				static ds_list_rec_t list_recs[MAX_LIST_RECS];
				int n_recs = ds_expected_lists(
					&ds, comp, values[idx_job].u64, seq,
					list_recs, MAX_LIST_RECS);
				int ri;
				if (n_recs < 0) {
					fprintf(stderr,
						"error: record %" PRIu64 ": more than %d list "
						"entry instances; raise MAX_LIST_RECS\n",
						written, MAX_LIST_RECS);
					return 1;
				}
				for (ri = 0; ri < n_recs; ri++) {
					const ds_list_rec_t *rec = &list_recs[ri];
					/* rec->list points into ds.lists[] (see
					 * ds_expected_lists() in core/dataset.c),
					 * so its offset from ds.lists is that list
					 * attribute's index into list_writers[],
					 * built in the same order above. */
					size_t lwi = (size_t)(rec->list - ds.lists);
					rc = write_list_entry(&list_writers[lwi], comp,
						values[idx_job].u64, seq,
						values[idx_ts].u64, rec);
					if (!rc)
						list_entries++;
					if (rc) {
						errors++;
						if (errors <= 10)
							fprintf(stderr,
								"error %d indexing %s[%" PRIu64 "] "
								"for record %" PRIu64 "\n",
								rc, rec->list->name, rec->index, written);
					}
				}
			}

			if (++in_batch == o.batch) {
				sos_container_commit(sos, SOS_COMMIT_ASYNC);
				sos_end_x(sos);
				add_checkpoint(now_s() - t_write, written);
				in_batch = 0;
				sos_begin_x(sos);
			}
			if (o.rate > 0) {
				double due = t_write + written / o.rate;
				double wait = due - now_s();
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
	sos_container_commit(sos, SOS_COMMIT_ASYNC);
	sos_end_x(sos);
	double t_indexed = now_s();
	if (cps[n_cps - 1].n != written)
		add_checkpoint(t_indexed - t_write, written);
	sos_container_close(sos, SOS_COMMIT_SYNC);
	double t_end = now_s();

	/* One JSON line, parsed by bin/node.sh and bin/run.sh. It holds raw
	 * inputs only; core/stats.py derives every rate and percentile. */
	size_t k;
	printf("{\"writer\": \"c\", \"container\": \"%s\", \"backend\": \"%s\", "
	       "\"node_index\": %d, \"records\": %" PRIu64 ", "
	       "\"list_entries\": %" PRIu64 ", \"errors\": %" PRIu64 ", "
	       "\"batch\": %" PRIu64 ", \"rate\": %.0f, "
	       "\"start_epoch_s\": %.6f, "
	       "\"open_s\": %.3f, \"write_s\": %.3f, \"close_sync_s\": %.3f, "
	       "\"checkpoints\": [",
	       o.path, o.backend, o.node_index, written, list_entries, errors,
	       o.batch, o.rate, start_epoch,
	       t_write - t_start, t_indexed - t_write, t_end - t_indexed);
	for (k = 0; k < n_cps; k++)
		printf("%s[%.6f, %" PRIu64 "]", k ? ", " : "", cps[k].t, cps[k].n);
	printf("]}\n");
	free(cps);
	ds_free(&ds);
	json_decref(tmpl_root);
	return errors ? 1 : 0;
}
