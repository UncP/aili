/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#ifndef _mass_tree_h_
#define _mass_tree_h_

#include "node.h"

typedef struct mass_tree
{
  node *root;
}mass_tree;

mass_tree* new_mass_tree();
void free_mass_tree();
void mass_tree_put(mass_tree *mt, const void *key, uint32_t len, const void *val);
void* mass_tree_get(mass_tree *mt, const void *key, uint32_t len);

#endif /* _mass_tree_h_ */