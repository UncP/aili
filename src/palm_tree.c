/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include <stdlib.h>

#include "palm_tree.h"

#define Read  0
#define Write 1

batch* new_batch()
{
	return new_node(Leaf, 0);
}

void free_batch(batch *b)
{
	free((void *)b);
}

void batch_clear(batch *b)
{
	b->pre  = 0;
	b->keys = 0;
	b->off  = 0;
}

int batch_add_write(batch *b, const void *key, uint32_t len, const void *val)
{
	return node_put(b, Write, key, len, val);
}

int batch_add_read(batch *b, const void *key, uint32_t len)
{
	return node_put(b, Read, key, len, val);
}

palm_tree* new_palm_tree()
{
	palm_tree *pt = (palm_tree *)malloc(sizeof(palm_tree));
	pt->root = new_node(Root, 0);
	return pt;
}

void free_palm_tree(palm_tree *pt)
{

}
