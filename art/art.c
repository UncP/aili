/**
 *    author:     UncP
 *    date:    2019-02-16
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <stdint.h>
#ifdef Debug
#include <string.h>
#include <stdio.h>
#endif

#include "art.h"
#include "art_node.h"

struct adaptive_radix_tree
{
  art_node *root;
};

adaptive_radix_tree* new_adaptive_radix_tree()
{
  adaptive_radix_tree *art = malloc(sizeof(adaptive_radix_tree));
  art->root = 0;

  return art;
}

void free_adaptive_radix_tree(adaptive_radix_tree *art)
{
  (void)art;
}

// return  0 on success,
// return +1 on failure,
// return -1 for retry
static int _adaptive_radix_tree_put(art_node **an, const void *key, size_t len, size_t off, const void *val)
{
  art_node *cur;
  uint64_t v, v1;
  int p;

  begin:
  // NOTE: __ATOMIC_RELAXED is not ok
  __atomic_load(an, &cur, __ATOMIC_ACQUIRE);

  debug_assert(off < len);

  // NOTE: this should be a rare case
  if (unlikely(cur == 0)) {
    art_node *leaf = (art_node *)make_leaf(key, len);
    if (likely(__atomic_compare_exchange_n(an, &cur, leaf, 0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED)))
      return 0;
    else
      return -1;
  }

  if (unlikely(is_leaf(cur))) {
    const char *k1 = get_leaf_key(cur), *k2 = (const char *)key;
    size_t l1 = get_leaf_len(cur), l2 = len, i;
    for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
      ;
    if (unlikely(i == l1 && i == l2))
      return 1; // key exists
    art_node *new = new_art_node();
    art_node_set_prefix(new, k1, off, i - off);
    off = i;
    unsigned char byte;
    byte = off == l1 ? 0 : k1[off];
    art_node_add_child(new, byte, cur);
    byte = off == l2 ? 0 : k2[off];
    art_node_add_child(new, byte, (art_node *)make_leaf(k2, l2));
    if (likely(__atomic_compare_exchange_n(an, &cur, new, 0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED))) {
      return 0;
    } else {
      free_art_node(new);
      return -1;
    }
  }

  // now we are talking!
  descend:
  v = art_node_get_stable_version(cur);

  p = art_node_prefix_compare(cur, v, key, len, off);

  v1 = art_node_get_version(cur);

  if (art_node_version_compare_expand(v, v1)) // prefix changed
    goto begin;

  if (p != art_node_version_get_prefix_len(v)) {
    if (art_node_lock(cur))
      goto begin;
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

  art_node **next = art_node_find_child(cur, ((unsigned char *)key)[off]);
  if (next)
    return _adaptive_radix_tree_put(next, key, len, off + 1, val);

  if (art_node_is_full(cur))
    art_node_grow(an);
  art_node_add_child(*an, ((unsigned char *)key)[off], (art_node *)make_leaf(key, len));
  return 0;
}

// return 0 on success
int adaptive_radix_tree_put(adaptive_radix_tree *art, const void *key, size_t len, const void *val)
{
  //char buf[256] = {0};
  //memcpy(buf, key, len);
  //printf("%s\n", buf);
  return _adaptive_radix_tree_put(&art->root, key, len, 0, val);
}

static void* _adaptive_radix_tree_get(art_node *an, const void *key, size_t len, size_t off)
{
  if (an == 0)
    return 0;

  if (is_leaf(an)) {
    const char *k1 = get_leaf_key(an), *k2 = (const char *)key;
    size_t l1 = get_leaf_len(an), l2 = len, i;
    for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
      ;
    if (i == l1 && i == l2)
      return (void *)k1; // key exists
    return 0;
  }

  if (art_node_prefix_compare(an, key, len, off) != art_node_get_prefix_len(an))
    return 0;

  off += art_node_get_prefix_len(an);

  debug_assert(off <= len);
  if (off == len)
    return 0;

  art_node **next = art_node_find_child(an, ((unsigned char *)key)[off]);
  return next ? _adaptive_radix_tree_get(*next, key, len, off + 1) : 0;
}

void* adaptive_radix_tree_get(adaptive_radix_tree *art, const void *key, size_t len)
{
  return _adaptive_radix_tree_get(art->root, key, len, 0);
}

