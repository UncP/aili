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
  uint32_t ptr, version;
  node *n;
  retry:
    ptr = 0; // index to start comparation
    n = r;
    version = node_get_stable_version(n);
    if (!is_root(version)) {
      n = (node *)node_get_parent(n);
      goto retry;
    }

  descend:
    if (is_border(version))
      return n;

    node *n1 = node_locate_child(n, key, len, ptr);
    uint32_t version1 = node_get_stable_version(n1);

    if (node_get_version(n) ^ version <= LOCK_BIT) {
      // case 1: neither insert nor split happens between last stable version and current version
      ptr += sizeof(uint64_t);
      n = n1;
      version = version1;
      goto descend;
    }

    uint32_t version2 = node_get_stable_version(n);
    if (get_vsplit(version2) != get_vsplit(version))
      // case 2: this node had a split, retry from root, pessimistic
      goto retry;

    version = version2;
    // case 3: this node inserted a key, retry this node
    goto descend;
}
