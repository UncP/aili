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

palm_tree* new_palm_tree();
void free_palm_tree();
void palm_tree_execute(palm_tree *pt, batch *b, worker *w);

#endif /* _palm_tree_h_ */