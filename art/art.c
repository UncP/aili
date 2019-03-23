/**
 *    author:     UncP
 *    date:    2019-02-16
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
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
static int _adaptive_radix_tree_put(art_node *parent, art_node **ptr, const void *key, size_t len, size_t off, const void *val)
{
  // TODO: val should be packed with key
  (void)val;
  art_node *an;
  uint64_t v, v1;
  int p;

  begin:
  // NOTE: __ATOMIC_RELAXED is not ok
  __atomic_load(ptr, &an, __ATOMIC_ACQUIRE);

  // this is an empty tree
  if (unlikely(an == 0)) {
    assert(parent == 0);
    art_node *leaf = (art_node *)make_leaf(key);
    if (likely(__atomic_compare_exchange_n(ptr, &an, leaf, 0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)))
      return 0;
    else
      return -1;
  }

  // this is a leaf
  if (unlikely(is_leaf(an))) {
    int r = art_node_replace_leaf_child(parent, ptr, key, len, off);
    if (unlikely(r == -2))
      goto begin; // `an` is changed
    return r;
  }

  // verify node prefix
  v = art_node_get_stable_expand_version(an);
  if (art_node_version_is_old(v))
    goto begin;

  p = art_node_prefix_compare(an, v, key, len, off);

  v1 = art_node_get_version(an);
  if (unlikely(art_node_version_is_old(v1) || art_node_version_compare_expand(v, v1)))
    goto begin;
  v = v1;

  if (p != art_node_version_get_prefix_len(v)) {
    if (unlikely(art_node_lock(an)))
      goto begin;
    art_node *new = art_node_expand_and_insert(an, key, len, off, p);
    parent = art_node_lock_force(parent);
    if (likely(parent)) {
      debug_assert(off);
      art_node_replace_child(parent, ((unsigned char *)key)[off - 1], an, new);
    } else { // this is root
      __atomic_store(ptr, &new, __ATOMIC_RELEASE);
    }
    art_node_unlock(parent);
    art_node_unlock(an);
    return 0;
  }

  off += p;
  debug_assert(off < len);

  // prefix is matched, we can descend
  art_node **next = art_node_find_child(an, v, ((unsigned char *)key)[off]);

  v = art_node_get_version(an);

  if (unlikely(art_node_version_is_old(v))) {
    off -= p;
    goto begin;
  }

  if (next)
    return _adaptive_radix_tree_put(an, next, key, len, off + 1, val);

  if (unlikely(art_node_lock(an))) {
    off -= p;
    goto begin;
  }

  art_node *new = 0;
  next = art_node_add_child(an, ((unsigned char *)key)[off], (art_node *)make_leaf(key), &new);
  if (unlikely(new)) {
    parent = art_node_lock_force(parent);
    if (parent) {
      debug_assert((int)off > p);
      art_node_replace_child(parent, ((unsigned char *)key)[off - p - 1], an, new);
    } else {
      __atomic_store(ptr, &new, __ATOMIC_RELEASE);
    }
    art_node_unlock(parent);
  }
  art_node_unlock(an);

  // ptrother thread might inserted same byte before we acquire lock
  if (unlikely(next))
    return _adaptive_radix_tree_put(an, next, key, len, off + 1, val);

  return 0;
}

// return 0 on success
// return 1 on duplication
int adaptive_radix_tree_put(adaptive_radix_tree *art, const void *key, size_t len, const void *val)
{
  //print_key(key, len);
  int ret;
  // retry should be rare
  while (unlikely((ret = _adaptive_radix_tree_put(0 /* parent */, &art->root, key, len, 0, val)) == -1))
    ;
  return ret;
}

// TODO: for now we retry from root for every failure, but this can be relaxed
static void* _adaptive_radix_tree_get(art_node *parent, art_node **ptr, const void *key, size_t len, size_t off)
{
  (void)parent;
  art_node *an;
  uint64_t v, v1;

  debug_assert(off <= len);

  begin:
  __atomic_load(ptr, &an, __ATOMIC_ACQUIRE);

  if (unlikely(an == 0)) {
    return 0;
  }

  if (unlikely(is_leaf(an))) {
    const char *k1 = get_leaf_key(an), *k2 = (const char *)key;
    size_t l1 = get_leaf_len(an), l2 = len, i;
    for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
      ;
    if (i == l1 && i == l2)
      return (void *)k1; // key exists
    //art_node_print(parent);
    //print_key(k1, l1);
    //printf("off:%lu\n", off);
    return 0;
  }

  v = art_node_get_stable_expand_version(an);
  if (art_node_version_is_old(v)) {
    assert(0);
    goto begin;
  }

  int p = art_node_prefix_compare(an, v, key, len, off);

  v1 = art_node_get_version(an);
  if (art_node_version_is_old(v1) || art_node_version_compare_expand(v, v1)) {
    assert(0);
    goto begin;
  }
  v = v1;

  if (p != art_node_version_get_prefix_len(v)) {
    assert(0);
    return 0;
  }

  off += art_node_version_get_prefix_len(v);
  debug_assert(off <= len);

  int advptrce = off != len;
  unsigned char byte = advptrce ? ((unsigned char *)key)[off] : 0;

  art_node **next = art_node_find_child(an, v, byte);

  v1 = art_node_get_version(an);

  if (art_node_version_is_old(v1)) {
    assert(0);
    goto begin;
  }

  if (next) {
    parent = *ptr;
    ptr = next;
    off += advptrce;
    goto begin;
  }
  return 0;
}

void* adaptive_radix_tree_get(adaptive_radix_tree *art, const void *key, size_t len)
{
  return _adaptive_radix_tree_get(art->root, &art->root, key, len, 0);
}

