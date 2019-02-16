/**
 *    author:     UncP
 *    date:    2019-02-16
 *    license:    BSD-3
**/

#include <stdlib.h>

#include "art.h"
#include "art_node.h"

struct adaptive_radix_tree
{
  art_node *root;
};

adaptive_radix_tree* new_adaptive_radix_tree()
{
  adaptive_radix_tree *art = malloc(sizeof(adaptive_radix_tree));
  art->root = new_art_node();

  return art;
}

int adaptive_radix_tree_put(adaptive_radix_tree *art, const void *key, size_t len, const void *val)
{

}

void* adaptive_radix_tree_get(adaptive_radix_tree *art, const void *key, size_t len)
{

}

