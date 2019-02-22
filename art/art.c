/**
 *    author:     UncP
 *    date:    2019-02-16
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <stdint.h>

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

// create a subtree and insert key into
static art_node* adaptive_radix_tree_create_subtree(const void *key, size_t len, size_t off, const void *val)
{
  debug_assert(off < len);

  art_node *root = 0, *parent = 0;
  do {
    art_node *an = new_art_node();
    if (root == 0)
      root = an;
    if (parent)
      art_node_add_child(parent, ((unsigned char *)key)[off++], an);

    int prefix_len = len - off;
    if (prefix_len > 8)
      prefix_len = 8;
    else
      --prefix_len;
    art_node_set_prefix(an, key, off, prefix_len);
    off += prefix_len;
    parent = an;
  } while (off < len - 1);

  debug_assert(off == len - 1);

  // wrap the length at key pointer's low 8 bits, assume key length is less than 256
  uintptr_t ptr = (uintptr_t)key | (len & 0xff);
  art_node_add_child(parent, ((unsigned char *)key)[len - 1], (art_node *)ptr);
  return root;
}

static int _adaptive_radix_tree_put(art_node **an, const void *key, size_t len, size_t off, const void *val)
{
  art_node *cur = *an;

  debug_assert(off < len);

  if (cur == 0) { // recursivelly create new node
    art_node *sub_root = adaptive_radix_tree_create_subtree(key, len, off, val);
    *an = sub_root;
    return 0;
  }

  if (is_leaf(cur)) {
    art_node *new = new_art_node();
    uintptr_t key2 = art_node_find_conflict()
  }
}

// return 0 on success
int adaptive_radix_tree_put(adaptive_radix_tree *art, const void *key, size_t len, const void *val)
{
  return _adaptive_radix_tree_put(&art->root, key, len, 0, val);
}

static void* _adaptive_radix_tree_get(art_node *an, const void *key, size_t len, size_t off)
{
  if (an == 0)
    return 0;

  if (is_leaf(an))
    return art_node_find_value(an, key, len, off);

  if (art_node_prefix_compare(an, key, len, off) != prefix_len(an))
    return 0;

  off += prefix_len(an);

  debug_assert(off <= len);
  if (off == len)
    return 0;

  an = art_node_find_child(an, ((unsigned char *)key)[off]);
  return _adaptive_radix_tree_get(an, key, len, off);
}

void* adaptive_radix_tree_get(adaptive_radix_tree *art, const void *key, size_t len)
{
  return _adaptive_radix_tree_get(art->root, key, len, 0);
}

