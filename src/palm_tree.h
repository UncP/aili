/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#ifndef _palm_tree_h_
#define _palm_tree_h_

#include "node.h"

// batch is a wrapper for node
// it accepts kv pairs and divide them into different threads for further process
typedef node batch;

typedef struct palm_tree
{
	node *root;
}palm_tree;

batch* new_batch();
void free_batch(batch *b);
void batch_clear(batch *b);
int batch_add(batch *b, const void *key, uint32_t len, const void *val);

palm_tree* new_palm_tree();
void free_palm_tree();
void palm_tree_execute(batch *b, uint32_t cur, uint32_t total);

#endif /* _palm_tree_h_ */