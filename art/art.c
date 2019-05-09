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
// return +1 on existed,
// return -1 on retry
static int adaptive_radix_tree_replace_leaf(art_node *parent, art_node **ptr, art_node *an,
  const void *key, size_t len, size_t off)
{
  art_node *new = art_node_replace_leaf_child(an, key, len, off);
  if (likely(new)) {
    if (likely(parent)) {
      if (unlikely(art_node_lock(parent))) {
        // parent is old
        free_art_node(new);
        return -1;
      } else {
        art_node *now;
        __atomic_load(ptr, &now, __ATOMIC_ACQUIRE);
        if (unlikely(now != an)) {
          // leaf has been replaced by another thread
          art_node_unlock(parent);
          free_art_node(new);
          return -1;
        }
        __atomic_store(ptr, &new, __ATOMIC_RELEASE);
        art_node_unlock(parent);
        return 0;
      }
    } else { // art with only one leaf
      if (likely(__atomic_compare_exchange_n(ptr, &an, new, 0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE))) {
        return 0;
      } else {
        free_art_node(new);
        return -1;
      }
    }
  } else {
    return 1;
  }
}

// return  0 on success,
// return +1 on existed,
// return -1 for retry
static int _adaptive_radix_tree_put(art_node *parent, art_node **ptr, const void *key, size_t len, size_t off)
{
  art_node *an;
  int first = 1;

  begin:
  // this is fucking ugly!
  if (first)  {
    first = 0;
  } else if (parent) {
    // this is not the first time we enter `begin`, so
    // we need to make sure that `ptr` is still valid because `parent` might changed
    uint64_t pv = art_node_get_version(parent);
    if (art_node_version_is_old(pv))
      return -1; // return -1 so that we can retry from root
    // `ptr` is still valid, we can proceed
  }

  // NOTE: __ATOMIC_RELAXED is not ok
  __atomic_load(ptr, &an, __ATOMIC_ACQUIRE);

  if (unlikely(is_leaf(an)))
    return adaptive_radix_tree_replace_leaf(parent, ptr, an, key, len, off);

  // verify node prefix
  uint64_t v = art_node_get_stable_expand_version(an);
  if (unlikely(art_node_version_get_offset(v) != off))
    goto begin;

  if (unlikely(art_node_version_is_old(v)))
    goto begin;

  int p = art_node_prefix_compare(an, v, key, len, off);

  uint64_t v1 = art_node_get_version(an);
  if (unlikely(art_node_version_is_old(v1) || art_node_version_compare_expand(v, v1)))
    goto begin;
  v = v1;

  if (p != art_node_version_get_prefix_len(v)) {
    if (unlikely(art_node_lock(an)))
      goto begin;
    // still need to check whether prefix has been changed!
    if (unlikely(art_node_version_compare_expand(v, art_node_get_version_unsafe(an)))) {
      art_node_unlock(an);
      goto begin;
    }
    debug_assert(art_node_version_is_old(art_node_get_version_unsafe(an)) == 0);
    art_node *new = art_node_expand_and_insert(an, key, len, off, p);
    parent = art_node_get_locked_parent(an);
    art_node_set_parent_unsafe(an, new);
    if (likely(parent)) {
      debug_assert(off);
      art_node_replace_child(parent, ((unsigned char *)key)[off - 1], an, new);
      art_node_unlock(parent);
    } else { // this is root
      __atomic_store(ptr, &new, __ATOMIC_RELEASE);
    }
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
    return _adaptive_radix_tree_put(an, next, key, len, off + 1);

  if (unlikely(art_node_lock(an))) {
    off -= p;
    goto begin;
  }

  art_node *new = 0;
  next = art_node_add_child(an, ((unsigned char *)key)[off], (art_node *)make_leaf(key), &new);
  if (unlikely(new)) {
    parent = art_node_get_locked_parent(an);
    if (likely(parent)) {
      debug_assert((int)off > p);
      art_node_replace_child(parent, ((unsigned char *)key)[off - p - 1], an, new);
      art_node_unlock(parent);
    } else {
      __atomic_store(ptr, &new, __ATOMIC_RELEASE);
    }
  }
  art_node_unlock(an);

  // another thread might inserted same byte before we acquire lock
  if (unlikely(next))
    return _adaptive_radix_tree_put(an, next, key, len, off + 1);

  return 0;
}

// return 0 on success
// return 1 on duplication
int adaptive_radix_tree_put(adaptive_radix_tree *art, const void *key, size_t len)
{
  //print_key(key, len);

  art_node *root = art->root;
  if (unlikely(root == 0)) { // empty art
    art_node *leaf = (art_node *)make_leaf(key);
    if (__atomic_compare_exchange_n(&art->root, &root, leaf, 0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE))
      return 0;
    // else another thread has replaced empty root
  }
  int ret;
  // retry should be rare
  while (unlikely((ret = _adaptive_radix_tree_put(0 /* parent */, &art->root, key, len, 0 /* off */)) == -1))
    ;
  return ret;
}

static void* _adaptive_radix_tree_get(art_node *parent, art_node **ptr, const void *key, size_t len, size_t off)
{
  art_node *an;

  debug_assert(off <= len);

  int first = 1; // is this the first time we enter `begin`
  begin:
  // this is fucking ugly!
  if (first)  {
    first = 0;
  } else if (parent) {
    // this is not the first time we enter `begin`, so
    // we need to make sure that `ptr` is still valid because `parent` might changed
    uint64_t pv = art_node_get_version(parent);
    if (art_node_version_is_old(pv))
      return (void *)1; // return 1 so that we can retry from root
    // `ptr` is still valid, we can proceed
  }
  __atomic_load(ptr, &an, __ATOMIC_ACQUIRE);

  if (unlikely(is_leaf(an))) {
    const char *k1 = get_leaf_key(an), *k2 = (const char *)key;
    size_t l1 = get_leaf_len(an), l2 = len, i;
    for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
      ;
    if (i == l1 && i == l2)
      return (void *)k1; // key exists
    art_node_print(parent);
    print_key(k1, l1);
    printf("off:%lu\n", off);
    return 0;
  }

  uint64_t v = art_node_get_stable_expand_version(an);
  if (unlikely(art_node_version_get_offset(v) != off))
    goto begin;
  if (unlikely(art_node_version_is_old(v)))
    goto begin;

  int p = art_node_prefix_compare(an, v, key, len, off);

  uint64_t v1 = art_node_get_version(an);
  if (unlikely(art_node_version_is_old(v1) || art_node_version_compare_expand(v, v1)))
    goto begin;
  v = v1;

  if (p != art_node_version_get_prefix_len(v))
    return 0;

  off += art_node_version_get_prefix_len(v);
  debug_assert(off <= len);

  int advance = off != len;
  unsigned char byte = advance ? ((unsigned char *)key)[off] : 0;

  art_node **next = art_node_find_child(an, v, byte);

  v1 = art_node_get_version(an);

  if (unlikely(art_node_version_is_old(v1)))
    goto begin;

  if (next)
    return _adaptive_radix_tree_get(an, next, key, len, off + advance);

  art_node_print(an);
  printf("off:%lu\n", off);
  return 0;
}

void* adaptive_radix_tree_get(adaptive_radix_tree *art, const void *key, size_t len)
{
  void *ret;
  if (unlikely(art->root == 0))
    return 0;
  while (unlikely((uint64_t)(ret = _adaptive_radix_tree_get(0, &art->root, key, len, 0)) == 1))
    ;
  return ret;
}

