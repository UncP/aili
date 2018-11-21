/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#ifndef _blink_tree_h_
#define _blink_tree_h_

#include "node.h"
#include "bounded_mapping_queue.h"

typedef struct blink_tree
{
  blink_node *root;

  bounded_mapping_queue *queue;
}blink_tree;

blink_tree* new_blink_tree(int thread_num);
void free_blink_tree(blink_tree *bt);
int blink_tree_write(blink_tree *bt, const void *key, uint32_t len, const void *val);
int blink_tree_read(blink_tree *bt, const void *key, uint32_t len, void **val);
void blink_tree_enqueue(blink_tree *bt, int is_write, const void *key, uint32_t len, const void *val);
void blink_tree_flush(blink_tree *bt);

#endif /* _blink_tree_h_ */