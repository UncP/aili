/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#ifndef _palm_tree_h_
#define _palm_tree_h_

#include "node.h"
#include "worker.h"

typedef struct palm_tree
{
  node *root;
}palm_tree;

palm_tree* new_palm_tree(int worker_num);
void free_palm_tree(palm_tree *pt);
void palm_tree_execute(palm_tree *pt, batch *b, worker *w);

#ifdef Test

void palm_tree_validate(palm_tree *pt);

#endif /* Test */

#endif /* _palm_tree_h_ */