/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include "palm.h"

batch* new_batch()
{
	return new_node(Leaf, 0);
}

void free_batch(batch *b)
{
	free((void *)b);
}

int batch_add(batch *b, const void *key, uint32_t len, const void *val)
{
	return node_insert(b, key, len, val);
}

void batch_clear(batch *b)
{
	b->pre  = 0;
	b->keys = 0;
	b->off  = 0;
}
