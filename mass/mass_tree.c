/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>
#include <string.h>

// TODO: remove this
#include <stdio.h>

#include "mass_tree.h"

#ifdef Allocator
#include "../palm/allocator.h"
#endif // Allocator

mass_tree* new_mass_tree(int thread_num)
{
#ifdef Allocator
  init_allocator();
#endif // Allocator

  (void)thread_num;
  mass_tree *mt = (mass_tree *)malloc(sizeof(mass_tree));

  node *r = new_node(Border);
  node_set_root_unsafe(r);

  node_insert_lowest_key(r);

  __atomic_store(&mt->root, &r, __ATOMIC_RELEASE);

  return mt;
}

void free_mass_tree(mass_tree *mt)
{
  (void)mt;
  // TODO: uncomment this
  // free_node(mt->root);
}

#ifdef Test

void mass_tree_validate(mass_tree *mt)
{
  node_validate(mt->root);
}

#endif // Test

// require: `n` and `n1` are locked
static node* mass_tree_grow(node *n, uint64_t fence, node *n1)
{
  node *r = new_node(Interior);

  // NOTE: not necessary, lock it to make `node_insert` happy
  node_lock_unsafe(r);

  node_set_root_unsafe(r);

  node_set_first_child(r, n);

  node_set_parent_unsafe(n1, r);
  node_set_parent_unsafe(n, r);

  assert((int)node_insert(r, &fence, sizeof(uint64_t), 0 /* off */, n1, 1 /* is_link */) == 1);

  node_unset_root_unsafe(n1);
  node_unset_root_unsafe(n);

  node_unlock_unsafe(r);

  return r;
}

// find the border node and record stable version
static node* find_border_node(node *r, uint64_t cur, uint32_t *version)
{
  uint32_t v;
  node *n;

  retry:
    n = r;
    assert(n);
    v = node_get_stable_version(n);
    // it's possible that a root has split
    if (!is_root(v)) {
      r = node_get_parent(n);
      goto retry;
    }

  descend:
    if (is_border(v)) {
      *version = v;
      return n;
    }

    node *n1 = node_descend(n, cur);
    assert(n1);
    // this is a crucial step, must not move the line below to case 1
    uint32_t v1 = node_get_stable_version(n1);

    uint32_t diff = node_get_version(n) ^ v;
    if (diff == LOCK_BIT || diff == 0) {
      // case 1: neither insert nor split happens between last stable version and current version,
      //         descend to child node
      n = n1;
      v = v1;
      goto descend;
    }

    uint32_t v2 = node_get_stable_version(n);
    // case 2: this node had a split, retry from root, pessimistic
    if (get_vsplit(v2) != get_vsplit(v))
      goto retry;

    // case 3: this node inserted a key, retry this node
    v = v2;
    goto descend;
}

// require: `n` is locked
// create a subtree and then insert kv into it, at last replace kv with subtree
static void create_new_layer(node *n, const void *key, uint32_t len, uint32_t off, const void *val)
{
  void *ckey;
  uint32_t clen;
  int idx = node_get_conflict_key_index(n, key, len, off, &ckey, &clen);

  // advance key offset that causes this conflict
  off += sizeof(uint64_t);

  // these 2 key can still be have mutiple common prefix keyslice, we need to loop and create
  // subtree until they don't
  node *head = 0, *parent = 0;
  uint64_t lks;
  uint32_t noff = off + sizeof(uint64_t);
  while (noff <= clen && noff <= len) {
    uint64_t ks1 = get_next_keyslice(ckey, clen, off);
    uint64_t ks2 = get_next_keyslice(key, len, off);
    // no need to use `compare_key`
    if (ks1 != ks2) break;

    node *bn = new_node(Border);
    node_set_root_unsafe(bn);
    node_insert_lowest_key(bn);
    if (head == 0) head = bn;
    if (parent) {
      node_lock_unsafe(parent);
      assert((int)node_insert(parent, &lks, sizeof(uint64_t), 0 /* off */, bn, 1 /* is_link */) == 1);
      node_unlock_unsafe(parent);
      node_set_parent_unsafe(bn, parent);
    }
    lks = ks1;
    parent = bn;
    off += sizeof(uint64_t);
    noff = off + sizeof(uint64_t);
  }

  // insert these 2 keys without conflict into border node
  node *bn = new_node(Border);
  node_set_root_unsafe(bn);
  node_insert_lowest_key(bn);
  node_lock_unsafe(bn);
  assert((int)node_insert(bn, ckey, clen, off, 0, 0 /* is_link */) == 1);
  assert((int)node_insert(bn, key, len, off, val, 0 /* is_link */) == 1);
  node_unlock_unsafe(bn);

  if (parent) {
    node_lock_unsafe(parent);
    assert((int)node_insert(parent, &lks, sizeof(uint64_t), 0 /* off */, bn, 1 /* is_link */) == 1);
    node_unlock_unsafe(parent);
    node_set_parent_unsafe(bn, parent);
  }

  // now replace previous key with new subtree link,
  // all the unsafe operation will be seen by other threads
  if (head == 0) {
    node_set_parent_unsafe(bn, n);
    node_replace_at_index(n, idx, bn);
  } else {
    node_set_parent_unsafe(head, n);
    node_replace_at_index(n, idx, head);
  }
}

// require: `n` and `n1` is locked
static void mass_tree_promote_split_node(mass_tree *mt, node *n, uint64_t fence, node *n1)
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

  uint32_t v;
  v = node_get_version(p);
  if (unlikely(is_border(v))) { // `n` is a sub tree
    node *sub_root = mass_tree_grow(n, fence, n1);
    node_set_parent_unsafe(sub_root, p);
    node_swap_child(p, n, sub_root);
    node_unlock(n);
    node_unlock(n1);
    node_unlock(p);
  } else if (likely(node_is_full(p) == 0)) {
    node_set_parent_unsafe(n1, p);
    assert((int)node_insert(p, &fence, sizeof(uint64_t), 0 /* off */, n1, 1 /* is_link */) == 1);
    node_unlock(n);
    node_unlock(n1);
    node_unlock(p);
  } else { // node is full
    v = set_split(v);
    node_set_version(p, v);
    node_unlock(n);
    uint64_t fence1 = 0;
    node *p1 = node_split(p, &fence1);
    assert(fence1);
    if (compare_key(fence, fence1) < 0) {
      node_set_parent_unsafe(n1, p);
      assert((int)node_insert(p, &fence, sizeof(uint64_t), 0 /* off */, n1, 1 /* is_link */) == 1);
    } else {
      node_set_parent_unsafe(n1, p1);
      assert((int)node_insert(p1, &fence, sizeof(uint64_t), 0 /* off */, n1, 1 /* is_link */) == 1);
    }
    node_unlock(n1);
    n = p;
    fence = fence1;
    n1 = p1;
    goto ascend;
  }
}

int mass_tree_put(mass_tree *mt, const void *key, uint32_t len, const void *val)
{
  uint32_t off = 0, v;
  uint64_t cur;
  node *r, *n;
  __atomic_load(&mt->root, &r, __ATOMIC_ACQUIRE);

  again:
  cur = get_next_keyslice(key, len, off);
  n = find_border_node(r, cur, &v);
  // before we write this node, a lock must be obtained
  node_lock(n);

  // it's ok to use `relaxed` operation since node is locked
  uint32_t diff = node_get_version_unsafe(n) ^ v;
  if (diff != LOCK_BIT) { // node has changed between we acquire this node and lock this node
    while (1) {
      node *next = node_get_next(n);
      if (next == 0)
        break;
      node_lock(next);
      // there might be splits happened, traverse through the link
      // we don't have to worry about the offset, it's valid
      if (!node_include_key(next, cur)) {
        node_unlock(next);
        break;
      } else {
        // move to next node
        node_unlock(n);
        n = next;
      }
    }
  }

  void *ret = node_insert(n, key, len, off, val, 0 /* is_link */);
  switch ((uint64_t)ret) {
    case 0: // key existed
    case 1: // key inserted
      node_unlock(n);
      return (int)ret;
    case -1: { // need to create a deeper layer
      create_new_layer(n, key, len, off, val);
      node_unlock(n);
      return 1;
    }
    case -2: { // node is full, need to split and promote
      uint64_t fence = 0;
      node *n1 = node_split(n, &fence);
      assert(fence);
      uint64_t cur = get_next_keyslice(key, len, off);
      // equal is not possible
      if (compare_key(cur, fence) < 0)
        assert((int)node_insert(n, key, len, off, val, 0 /* is_link */) == 1);
      else
        assert((int)node_insert(n1, key, len, off, val, 0 /* is_link */) == 1);

      mass_tree_promote_split_node(mt, n, fence, n1);
      return 1;
    }
    default: // need to go to a deeper layer
      node_unlock(n);
      r = (node *)ret;
      // if we need to advance to next layer, then key offset will not exceed key length
      off += sizeof(uint64_t);
      goto again;
  }
}

void* mass_tree_get(mass_tree *mt, const void *key, uint32_t len)
{
  uint32_t off = 0, v;
  uint64_t cur;
  node *r, *n;
  __atomic_load(&mt->root, &r, __ATOMIC_ACQUIRE);

  again:
  cur = get_next_keyslice(key, len, off);
  n = find_border_node(r, cur, &v);

  forward:
  if (is_deleted(v)) {
    // NOTE: remove this if we ever implement `mass_tree_delete`
    assert(0);
    goto again;
  }

  void *suffix;
  node *next_layer = node_search(n, key, len, off, &suffix);

  uint32_t diff = node_get_version(n) ^ v;
  if (diff != LOCK_BIT && diff != 0) {
    v = node_get_stable_version(n);
    node *next = node_get_next(n);
    // there might be splits happened, traverse through the link
    while (!is_deleted(v) && next && node_include_key(next, cur)) {
      n = next;
      v = node_get_stable_version(n);
      next = node_get_next(n);
    }
    goto forward;
  }

  if (suffix) return suffix; // key found
  if (!next_layer) return 0; // key not exists
  if ((uint64_t)next_layer == 1) goto forward; // unstable

  r = next_layer;
  // if we need to advance to next layer, then key offset will not exceed key length
  off += sizeof(uint64_t);
  goto again;
}
