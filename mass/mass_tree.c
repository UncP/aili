/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>

#include "mass_tree.h"

mass_tree* new_mass_tree()
{
  mass_tree *mt = (mass_tree *)malloc(sizeof(mass_tree));

  mt->root = new_node(Border);

  return mt;
}

void free_mass_tree()
{
  // TODO
}

static border_node* find_border_node(node *r, const void *key, uint32_t len)
{
  uint32_t version;
  node *n;
  retry:
  	n = r;
    version = node_get_stable_version(n);
    if (!is_root(version)) {
      n = (node *)node_get_parent(n);
      goto retry;
    }

  descend:
    if (is_border(version))
      return n;

    if (node_get_version(n) ^ version <= LOCK_BIT) {

    	goto descend;
    }

    uint32_t version2 = node_get_stable_version(n);
    if (get_vsplit(version2) != get_vsplit(version))
    	goto retry;
    version = version2;
    goto descend;
}
