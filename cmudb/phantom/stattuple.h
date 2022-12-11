#ifndef __PHANTOM_STATTUPLE_H__
#define __PHANTOM_STATTUPLE_H__

#include <stdlib.h>
#include <stddef.h>
#include "c.h"

typedef struct PhantomStatTuple
{
	Oid         relid;
	TimestampTz ts;
	uint64		table_len;
	uint64		tuple_count;
	uint64		tuple_len;
	uint64		dead_tuple_count;
	uint64		dead_tuple_len;
	uint64		free_space;		/* free/reusable space in bytes */
} PhantomStatTuple;

#define PHANTOM_STATS_PER_CHUNK 32

struct PhantomStatsChunk {
	PhantomStatTuple stats[PHANTOM_STATS_PER_CHUNK];
	struct PhantomStatsChunk* next;
	int32_t num_stats;
};

/** Assert that this fits in a page size allocation. */
StaticAssertDecl(sizeof(struct PhantomStatsChunk) < 4096, "PhantomStatsChunk is too large");

extern struct PhantomStatTuple* GetPhantomStatsEntry(void);

extern void phantom_init(void);
extern void phantom_stat_relation(Oid relid, char* page);
extern void phantom_dump(void);

#endif
