/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "mass_tree.h"

mass_tree* new_mass_tree()
{
  mass_tree *mt = (mass_tree *)malloc(sizeof(mass_tree));

  node *r = new_node(Border);
  uint32_t version = node_get_version(r);
  version = set_root(version);
  node_set_version(r, version);

  mt->root = r;

  return mt;
}

// require: no other thread is visiting this tree
void free_mass_tree(mass_tree *mt)
{
  free_node(mt->root);
}

// require: `n` and `n1` are locked
static node* mass_tree_grow(node *n, uint64_t fence, node *n1)
{
  node *r = new_node(Interior);

  // TODO: some operations below is not required to be atomic
  node_set_root(r);

  node_set_first_child(r, n);
  uint32_t ptr = 0;
  assert((int)node_insert(r, &fence, sizeof(uint64_t), &ptr, n1, 1 /* is_link */) == 1);

  node_set_parent(n, r);
  node_set_parent(n1, r);

  node_unset_root(n);
  node_unset_root(n1);

  return r;
}

static node* find_border_node(node *r, const void *key, uint32_t len, uint32_t *ptr)
{
  uint32_t version, ori = *ptr;
  node *n;
  retry:
    *ptr = ori; // index to start comparation
    n = r;
    assert(n);
    version = node_get_stable_version(n);
    // it's possible that a root has split
    if (!is_root(version)) {
      n = node_get_parent(n);
      goto retry;
    }

  descend:
    if (is_border(version))
      return n;

    uint32_t pre = *ptr; // save the offset of comparation for retry
    node *n1 = node_locate_child(n, key, len, ptr);
    assert(n1);
    uint32_t version1 = node_get_stable_version(n1);

    // TODO: fix this
    if ((node_get_version(n) ^ version) <= LOCK_BIT) {
      // case 1: neither insert nor split happens between last stable version and current version,
      //         descend to child node
      n = n1;
      version = version1;
      goto descend;
    }

    uint32_t version2 = node_get_stable_version(n);
    if (get_vsplit(version2) != get_vsplit(version))
      // case 2: this node had a split, retry from root, pessimistic
      goto retry;

    *ptr = pre; // restore offset
    version = version2;
    // case 3: this node inserted a key, retry this node
    goto descend;
}

// require: `n` and `n1` is locked
static void split_and_promote(mass_tree *mt, node *n, uint64_t fence, node *n1)
{
  node *p;
  ascend:
  p = node_get_locked_parent(n);
  if (unlikely(p == 0)) {
    node *new_root = mass_tree_grow(n, fence, n1);
    __atomic_store(&mt->root, &new_root, __ATOMIC_RELEASE); // replace the root
    node_unlock(n);
    node_unlock(n1);
    return ;
  }

  // need to set parent here instead of `node_split`
  node_set_parent(n1, p);

  uint32_t version;
  node *p1;
  version = node_get_version(p);
  if (unlikely(is_border(version))) { // this is a sub tree
    p1 = mass_tree_grow(n, fence, n1);
    node_swap_child(p, n, p1);
    node_unlock(n);
    node_unlock(n1);
    node_unlock(p);
  } else if (unlikely(node_is_full(p))) {
    uint32_t tmp = 0;
    assert((int)node_insert(p, &fence, sizeof(uint64_t), &tmp, n1, 1 /* is_link */) == 1);
    node_unlock(n);
    node_unlock(n1);
    node_unlock(p);
  } else {
    version = set_split(version);
    node_set_version(p, version);
    node_unlock(n);
    uint64_t fence1 = 0;
    p1 = node_split(p, &fence1);
    assert(fence1);
    uint32_t tmp = 0;
    if (fence < fence1)
      assert((int)node_insert(p, &fence, sizeof(uint64_t), &tmp, n1, 1 /* is_link */) == 1);
    else
      assert((int)node_insert(p1, &fence, sizeof(uint64_t), &tmp, n1, 1 /* is_link */) == 1);
    node_unlock(n1);
    n = p;
    fence = fence1;
    n1 = p1;
    goto ascend;
  }
}

int mass_tree_put(mass_tree *mt, const void *key, uint32_t len, const void *val)
{
  uint32_t ptr = 0;
  node *r, *n;
  __atomic_load(&mt->root, &r, __ATOMIC_ACQUIRE);

  again:
  n = find_border_node(r, key, len, &ptr);
  // before we write this node, a lock must be obtained
  node_lock(n);

  void *v = node_insert(n, key, len, &ptr, val, 0 /* is_link */);
  switch ((uint64_t)v) {
    case 0: // key existed
    case 1: // key inserted
      node_unlock(n);
      return (int)v;
    case -1: { // need to create a deeper layer
      node *n1 = new_node(Border);
      void *ckey;
      uint32_t clen;
      int index = node_get_conflict_key(n, key, len, &ptr, &ckey, &clen);

      uint32_t tmp, pre;
      tmp = 0, pre = ptr;
      assert((int)node_insert(n1, ckey, clen, &ptr, 0, 0 /* is_link */) == 1);
      tmp = 0, ptr = pre;
      assert((int)node_insert(n1, key, len, &ptr, val, 0 /* is_link */) == 1);

      node_set_parent(n1, n);

      // replace value with new layer `n1`
      node_update_at(n, index, n1);
      return 1;
    }
    case -2: { // node is full, need split and promote
      uint64_t fence = 0;
      node *n1 = node_split(n, &fence);
      assert(fence);
      uint64_t cur = 0;
      if ((ptr + sizeof(uint64_t)) > len)
        memcpy(&cur, key, len - ptr); // other bytes will be 0
      else
        cur = *((uint64_t *)((char *)key + ptr));
      // equal is not possible
      if (cur < fence)
        assert((int)node_insert(n, key, len, &ptr, val, 0 /* is_link */) == 1);
      else
        assert((int)node_insert(n1, key, len, &ptr, val, 0 /* is_link */) == 1);
      split_and_promote(mt, n, fence, n1);
      return 1;
    }
    default: // need to go to a deeper layer
      node_unlock(n);
      r = (node *)v;
      goto again;
  }
}

void* mass_tree_get(mass_tree *mt, const void *key, uint32_t len)
{
  uint32_t ptr = 0, version;
  node *r, *n;
  __atomic_load(&mt->root, &r, __ATOMIC_ACQUIRE);

  again:
  n = find_border_node(r, key, len, &ptr);

  forward:
  version = node_get_version(n);
  if (is_deleted(version))
    goto again;

  void *suffix;
  node *next_layer = node_search(n, key, len, &ptr, &suffix);

  // TODO: fix this
  if ((node_get_version(n) ^ version) > LOCK_BIT) {
    version = node_get_stable_version(n);
    node *next = node_get_next(n);
    // there might be splits happened, traverse through the link
    while (!is_deleted(version) && next && node_include_key(next, key, len, ptr)) {
      n = next;
      version = node_get_stable_version(n);
      next = node_get_next(n);
    }
    goto forward;
  }

  if (suffix) return suffix; // key found
  if (!next_layer) return 0; // key not exists
  if ((int)next_layer == 1) goto forward; // unstable

  n = next_layer;
  goto again;
}
