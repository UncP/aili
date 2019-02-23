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
  (void)val; // fow now just ignore val
  debug_assert(off < len);

  art_node *root = 0, *parent = 0;
  do {
    art_node *an = new_art_node();
    if (root == 0)
      root = an;
    if (parent)
      art_node_add_child(parent, ((unsigned char *)key)[off++], an);
    parent = an;

    int prefix_len = len - off;
    if (prefix_len > 8)
      prefix_len = 8;
    else
      --prefix_len;
    art_node_set_prefix(an, key, off, prefix_len);
    off += prefix_len;
  } while (off < len - 1);

  debug_assert(off == len - 1);

  uintptr_t ptr = make_leaf(key, len);
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
    const char *k1 = get_key(cur), *k2 = (const char *)key;
    size_t l1 = get_len(cur), l2 = len, i;
    for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
      ;
    if (i == l1 && i == l2)
      return 1; // key exists
    art_node *new = new_art_node();
    art_node_set_prefix(new, k1, off, i - off);
    off = i;
    unsigned char byte;
    byte = off == l1 ? 0 : k1[off];
    art_node_add_child(new, byte, cur);
    byte = off == l2 ? 0 : k2[off];
    art_node_add_child(new, byte, (art_node *)make_leaf(k2, l2));
    return 0;
  }

  size_t p = art_node_prefix_compare(cur, key, len, off);
  if (p != prefix_len(cur)) {
    art_node *new = new_art_node();
    art_node_set_prefix(new, key, off, p);
    unsigned char byte;
    byte = (off + p < len) ? ((unsigned char *)key)[off + p] : 0;
    art_node_add_child(new, byte, (art_node *)make_leaf(key, len));
    byte = art_node_truncate_prefix(cur, p);
    art_node_add_child(new, byte, cur);
    *an = new;
    return 0;
  }

  off += prefix_len(cur);
  art_node *next = art_node_find_child(cur, ((unsigned char *)key)[off]);
  if (next)
    return _adaptive_radix_tree_put(&next, key, len, off + 1, val);
  if (art_node_is_full(cur))
    art_node_grow(an);
  art_node_add_child(cur, ((unsigned char *)key)[off], (art_node *)make_leaf(key, len));
  return 0;
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

