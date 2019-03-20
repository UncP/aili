/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#ifndef _palm_tree_h_
#define _palm_tree_h_

#include <pthread.h>

#include "node.h"
#include "worker.h"
#include "bounded_queue.h"

typedef struct palm_tree
{
  node *root;

  int        worker_num;
  int        running;
  pthread_t *ids;

  bounded_queue *queue;

  worker **workers;

}palm_tree;

palm_tree* new_palm_tree(int worker_num, int queue_size);
void free_palm_tree(palm_tree *pt);
void palm_tree_flush(palm_tree *pt);
void palm_tree_execute(palm_tree *pt, batch *b);

#ifdef Test

void palm_tree_validate(palm_tree *pt);

#endif /* Test */

#endif /* _palm_tree_h_ */
