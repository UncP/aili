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

mass_tree* new_mass_tree()
{
  mass_tree *mt = (mass_tree *)malloc(sizeof(mass_tree));

  mass_node *r = new_mass_node(Border);
  mass_node_set_root_unsafe(r);

  mt->root = r;

  return mt;
}

void free_mass_tree(mass_tree *mt)
{
  free_mass_node(mt->root);
}

#ifdef Test

void mass_tree_validate(mass_tree *mt)
{
  mass_node_validate(mt->root);
}

#endif // Test

// require: `n` and `n1` are locked
static mass_node* mass_tree_grow(mass_node *n, uint64_t fence, mass_node *n1)
{
  mass_node *r = new_mass_node(Interior);

  // NOTE: not necessary, lock it to make `mass_node_insert` happy
  mass_node_lock_unsafe(r);

  mass_node_set_root_unsafe(r);

  mass_node_set_first_child(r, n);

  interior_mass_node_insert(r, fence, n1);

  mass_node_unset_root_unsafe(n1);
  mass_node_unset_root_unsafe(n);

  mass_node_unlock_unsafe(r);

  return r;
}

// find the border mass_node and record stable version
static mass_node* find_border_mass_node(mass_node *r, uint64_t cur, uint32_t *version)
{
  uint32_t v;
  mass_node *n;

  retry:
    n = r;
    assert(n);
    v = mass_node_get_stable_version(n);
    // it's possible that a root has split
    if (!is_root(v)) {
      r = mass_node_get_parent(n);
      goto retry;
    }

  descend:
    if (is_border(v)) {
      *version = v;
      return n;
    }

    mass_node_prefetch(n);

    mass_node *n1 = mass_node_descend(n, cur);
    assert(n1);
    // this is a crucial step, must not move the line below to case 1
    uint32_t v1 = mass_node_get_stable_version(n1);

    uint32_t diff = mass_node_get_version(n) ^ v;
    if (diff == LOCK_BIT || diff == 0) {
      // case 1: neither insert nor split happens between last stable version and current version,
      //         descend to child mass_node
      n = n1;
      v = v1;
      goto descend;
    }

    uint32_t v2 = mass_node_get_stable_version(n);
    // case 2: this mass_node had a split, retry from root, pessimistic
    if (get_vsplit(v2) != get_vsplit(v))
      goto retry;

    // case 3: this mass_node inserted a key, retry this mass_node
    v = v2;
    goto descend;
}

// require: `n` is locked
// create a subtree lazily and then insert kv into it, at last replace kv with this subtree
static void create_new_layer(mass_node *n, const void *key, uint32_t len, uint32_t off, const void *val)
{
  void *ckey;
  uint32_t clen;
  int idx = mass_node_get_conflict_key_index(n, key, len, off, &ckey, &clen);

  // advance key offset that causes this conflict
  off += sizeof(uint64_t);

  // these 2 key can still be have mutiple common prefix keyslice, we need to loop and create
  // subtree until they don't
  mass_node *head = 0, *parent = 0;
  uint64_t lks;
  uint32_t noff = off + sizeof(uint64_t);
  while (noff <= clen && noff <= len) {
    uint64_t ks1 = get_next_keyslice(ckey, clen, off);
    uint64_t ks2 = get_next_keyslice(key, len, off);
    // no need to use `mass_compare_key`
    if (ks1 != ks2) break;

    mass_node *bn = new_mass_node(Border);
    mass_node_set_root_unsafe(bn);
    if (head == 0) head = bn;
    if (parent) {
      mass_node_lock_unsafe(parent);
      assert((uint64_t)border_mass_node_insert(parent, &lks, sizeof(uint64_t), 0 /* off */, bn, 1 /* is_link */) == 1);
      mass_node_unlock_unsafe(parent);
    }
    lks = ks1;
    parent = bn;
    off += sizeof(uint64_t);
    noff = off + sizeof(uint64_t);
  }

  // insert these 2 keys without conflict into border mass_node
  mass_node *bn = new_mass_node(Border);
  mass_node_set_root_unsafe(bn);
  mass_node_lock_unsafe(bn);
  assert((uint64_t)border_mass_node_insert(bn, ckey, clen, off, 0, 0 /* is_link */) == 1);
  assert((uint64_t)border_mass_node_insert(bn, key, len, off, val, 0 /* is_link */) == 1);
  mass_node_unlock_unsafe(bn);

  if (parent) {
    mass_node_lock_unsafe(parent);
    assert((uint64_t)border_mass_node_insert(parent, &lks, sizeof(uint64_t), 0 /* off */, bn, 1 /* is_link */) == 1);
    mass_node_unlock_unsafe(parent);
  }

  // now replace previous key with new subtree link
  if (head == 0)
    mass_node_replace_at_index(n, idx, bn);
  else
    mass_node_replace_at_index(n, idx, head);
}

// require: `n` and `n1` is locked
static void mass_tree_promote_split_mass_node(mass_tree *mt, mass_node *n, uint64_t fence, mass_node *n1)
{
  mass_node *p;
  ascend:
  p = mass_node_get_locked_parent(n);
  if (unlikely(p == 0)) {
    mass_node *new_root = mass_tree_grow(n, fence, n1);
    mt->root = new_root;
    mass_node_unlock(n);
    mass_node_unlock(n1);
    return ;
  }

  uint32_t v;
  v = mass_node_get_version(p);
  if (unlikely(is_border(v))) { // `n` is a sub tree
    mass_node *sub_root = mass_tree_grow(n, fence, n1);
    mass_node_swap_child(p, n, sub_root);
    mass_node_unlock(n);
    mass_node_unlock(n1);
    mass_node_unlock(p);
  } else if (likely(mass_node_is_full(p) == 0)) {
    interior_mass_node_insert(p, fence, n1);
    mass_node_unlock(n);
    mass_node_unlock(n1);
    mass_node_unlock(p);
  } else { // mass_node is full
    v = set_split(v);
    mass_node_set_version(p, v);
    mass_node_unlock(n);
    uint64_t fence1 = 0;
    mass_node *p1 = mass_node_split(p, &fence1);
    assert(fence1);
    if (mass_compare_key(fence, fence1) < 0)
      interior_mass_node_insert(p, fence, n1);
    else
      interior_mass_node_insert(p1, fence, n1);
    mass_node_unlock(n1);
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
  // it's ok to use stale root
  mass_node *r = mt->root, *n;

  again:
  cur = get_next_keyslice(key, len, off);
  n = find_border_mass_node(r, cur, &v);

  forward:
  if (unlikely(is_deleted(v))) {
    // NOTE: remove this if we ever implement `mass_tree_delete`
    assert(0);
    goto again;
  }

  // before we write this mass_node, a lock must be obtained
  mass_node_lock(n);

  // it's ok to use `unsafe` operation since mass_node is locked
  uint32_t diff = mass_node_get_version_unsafe(n) ^ v;
  if (diff != LOCK_BIT) { // mass_node has changed between we acquire this mass_node and lock this mass_node
    // unlock first
    mass_node_unlock(n);

    v = mass_node_get_stable_version(n);
    mass_node *next = mass_node_get_next(n);
    // there might be inserts or inserts happened, traverse through the link
    while (!is_deleted(v) && next && mass_node_include_key(next, cur)) {
      n = next;
      v = mass_node_get_stable_version(n);
      next = mass_node_get_next(n);
    }
    goto forward;
  }

  border_mass_node_prefetch_write(n);

  void *ret = border_mass_node_insert(n, key, len, off, val, 0 /* is_link */);
  switch ((uint64_t)ret) {
    case 0: // key existed
      mass_node_unlock(n);
      return 0;
    case 1: // key inserted
      mass_node_unlock(n);
      return 1;
    case -1: { // need to create a deeper layer
      create_new_layer(n, key, len, off, val);
      mass_node_unlock(n);
      return 1;
    }
    case -2: { // mass_node is full, need to split and promote
      uint64_t fence = 0;
      mass_node *n1 = mass_node_split(n, &fence);
      assert(fence);
      uint64_t cur = get_next_keyslice(key, len, off);
      // equal is not possible
      if (mass_compare_key(cur, fence) < 0)
        assert((uint64_t)border_mass_node_insert(n, key, len, off, val, 0 /* is_link */) == 1);
      else
        assert((uint64_t)border_mass_node_insert(n1, key, len, off, val, 0 /* is_link */) == 1);

      mass_tree_promote_split_mass_node(mt, n, fence, n1);
      return 1;
    }
    default: // need to go to a deeper layer
      mass_node_unlock(n);
      r = (mass_node *)ret;
      // if we need to advance to next layer, then key offset will not exceed key length
      off += sizeof(uint64_t);
      goto again;
  }
}

void* mass_tree_get(mass_tree *mt, const void *key, uint32_t len)
{
  uint32_t off = 0, v;
  uint64_t cur;
  // it's ok to use stale root
  mass_node *r = mt->root, *n;

  again:
  cur = get_next_keyslice_and_advance(key, len, &off);
  n = find_border_mass_node(r, cur, &v);

  forward:
  if (is_deleted(v)) {
    // NOTE: remove this if we ever implement `mass_tree_delete`
    assert(0);
    goto again;
  }

  border_mass_node_prefetch_read(n);

  void *suffix;
  void *lv = mass_node_search(n, cur, &suffix);

  uint32_t diff = mass_node_get_version(n) ^ v;
  if (diff != LOCK_BIT && diff != 0) {
    v = mass_node_get_stable_version(n);
    mass_node *next = mass_node_get_next(n);
    // there might be inserts or inserts happened, traverse through the link
    while (!is_deleted(v) && next && mass_node_include_key(next, cur)) {
      n = next;
      v = mass_node_get_stable_version(n);
      next = mass_node_get_next(n);
    }
    goto forward;
  }

  // case 1: unstable state, need to retry
  if (unlikely((uint64_t)lv == 1)) goto forward;

  if (suffix) {
    uint32_t clen = (uint32_t)((uint64_t)lv);
    uint32_t coff = (uint32_t)((uint64_t)lv >> 32);
    assert(coff == off);
    // case 2: key exists
    if (clen == len && !memcmp((char *)key + off, (char *)suffix + off, len - off))
      return suffix;
  } else if (lv) {
    // case 3: goto a deeper layer
    r = (mass_node *)lv;
    // offset is already set in `get_next_keyslice_and_advance`
    goto again;
  }

  // case 4: key does not exist
  return 0;
}
