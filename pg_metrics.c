#include "postgres.h"

#include "access/hash.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/guc.h"

#define MAXMETRICSNAMELEN		127


PG_MODULE_MAGIC;


typedef enum
{
	PGMET_METRICTYPE_COUNTER,
} pgmetMetricType;

typedef struct pgmetMetricData
{
	pgmetMetricType	type;	/* type of the metric; immutable */

	slock_t			mutex;	/* protects the fields below */

	int64			value;	/* the current value of the counter */
} pgmetMetricData;

/*
 * Global shared state
 */
typedef struct pgmetSharedState
{
	/* protects the hashtable */
	LWLockId		lock;
} pgmetSharedState;

/*
 * The key for a hash table entry in shared memory.
 */
typedef struct pgmetSharedHashKey
{
	const char *name_ptr;
	int			name_len;
} pgmetSharedHashKey;

/*
 * Hash table entry for a metric in shared memory.  The hash table only
 * contains an offset into the "metrics" array in the shared state structure.
 */
typedef struct pgmetSharedEntry
{
	pgmetSharedHashKey	key;			/* hash key of entry - MUST BE FIRST */
	pgmetMetricData		metric_data;	/* metric data */
	char				metric_name[1];	/* VARIABLE LENGTH ARRAY - MUST BE LAST */
} pgmetSharedEntry;


extern Datum pgmet_counter_add(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgmet_counter_add);

extern void _PG_init(void);
extern void _PG_fini(void);

static pgmetMetricData *pgmet_upsert_metric(const char *name, pgmetMetricType type);
static void pgmet_shmem_startup(void);
static uint32 pgmet_shared_hash_fn(const void *key, Size keysize);
static int pgmet_shared_match_fn(const void *key1, const void *key2, Size keysize);
static Size pgmet_memsize(void);


static int pgmet_max = -1;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static pgmetSharedState *pgmet = NULL;
static HTAB *pgmet_shared_hash = NULL;


Datum
pgmet_counter_add(PG_FUNCTION_ARGS)
{
	text *metric_name = PG_GETARG_TEXT_PP(0);
	int64 increment = PG_GETARG_INT64(1);
	volatile pgmetMetricData *metric;
	int64 prev;

	if (VARSIZE(metric_name) > MAXMETRICSNAMELEN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("metric name must not be longer than 127 characters in length")));
	}

	if (!pgmet)
	{
		/* no-op if shm has not been initialized */
		PG_RETURN_NULL();
	}

	metric = pgmet_upsert_metric(text_to_cstring(metric_name), PGMET_METRICTYPE_COUNTER);
	if (!metric)
	{
		/* couldn't whatever or whatever */
		PG_RETURN_NULL();
	}

	SpinLockAcquire(&metric->mutex);
	prev = metric->value;
	metric->value += increment;
	SpinLockRelease(&metric->mutex);

	PG_RETURN_INT64(prev);
}

static pgmetMetricData *
pgmet_upsert_metric(const char *name, pgmetMetricType type)
{
	pgmetSharedHashKey	key;
	pgmetSharedEntry   *entry;
	bool				found;

	Assert(pgmet != NULL);

	key.name_ptr = name;
	key.name_len = strlen(name);

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgmet->lock, LW_SHARED);

	entry = (pgmetSharedEntry *) hash_search(pgmet_shared_hash, &key, HASH_FIND, NULL);
	if (entry)
	{
		LWLockRelease(pgmet->lock);
		return &entry->metric_data;
	}

	/* Must acquire exclusive lock to add a new entry. */
	LWLockRelease(pgmet->lock);
	LWLockAcquire(pgmet->lock, LW_EXCLUSIVE);
	if (hash_get_num_entries(pgmet_shared_hash) >= pgmet_max)
	{
		LWLockRelease(pgmet->lock);
		return NULL;
	}

	entry = (pgmetSharedEntry *) hash_search(pgmet_shared_hash, &key, HASH_ENTER, &found);
	if (found)
	{
		LWLockRelease(pgmet->lock);
		return &entry->metric_data;
	}
	else
	{
		/* New entry, initialize it */

		/* dynahash tried to copy the key for us, but must fix name_ptr */
		entry->key.name_ptr = entry->metric_name;
		/* reset the statistics */
		memset(&entry->metric_data, 0, sizeof(pgmetMetricData));
		entry->metric_data.type = type;
		SpinLockInit(&entry->metric_data.mutex);
		entry->metric_data.value = 0;
		memcpy(entry->metric_name, key.name_ptr, key.name_len);
		entry->metric_name[key.name_len] = '\0';

		LWLockRelease(pgmet->lock);

		return &entry->metric_data;
	}
}

void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_metrics.max",
	  "Sets the maximum number of merics which can be created.",
							NULL,
							&pgmet_max,
							50,
							10,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	RequestAddinShmemSpace(pgmet_memsize());
	RequestAddinLWLocks(1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgmet_shmem_startup;
}

void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}

static void
pgmet_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgmet = NULL;
	pgmet_shared_hash = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgmet = ShmemInitStruct("pg_metrics",
							sizeof(pgmetSharedState),
							&found);

	if (!found)
	{
		/* First time through ... */
		pgmet->lock = LWLockAssign();
	}

	memset(&info, 0, sizeof(info));
	info.keysize = MAXMETRICSNAMELEN + 1;
	info.entrysize = offsetof(pgmetSharedEntry, metric_name) + MAXMETRICSNAMELEN + 1;
	info.hash = pgmet_shared_hash_fn;
	info.match = pgmet_shared_match_fn;
	pgmet_shared_hash = ShmemInitHash("pg_metrics hash",
									  pgmet_max, pgmet_max,
									  &info,
									  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);
}

/*
 * Calculate hash value for a key
 */
static uint32
pgmet_shared_hash_fn(const void *key, Size keysize)
{
	const pgmetSharedHashKey *k = (const pgmetSharedHashKey *) key;

	/* we don't bother to include encoding in the hash */
	return DatumGetUInt32(hash_any((const unsigned char *) k->name_ptr,
								   k->name_len));
}

/*
 * Compare two keys - zero means match
 */
static int
pgmet_shared_match_fn(const void *key1, const void *key2, Size keysize)
{
	const pgmetSharedHashKey *k1 = (const pgmetSharedHashKey *) key1;
	const pgmetSharedHashKey *k2 = (const pgmetSharedHashKey *) key2;

	if (k1->name_len == k2->name_len &&
		memcmp(k1->name_ptr, k2->name_ptr, k1->name_len) == 0)
		return 0;
	else
		return 1;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgmet_memsize(void)
{
	Size		size;
	Size		entrysize;

	size = MAXALIGN(sizeof(pgmetSharedState));
	entrysize = offsetof(pgmetSharedEntry, metric_name) + MAXMETRICSNAMELEN + 1;
	size = add_size(size, hash_estimate_size(pgmet_max, entrysize));

	return size;
}
