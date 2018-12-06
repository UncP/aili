/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "mass_tree.h"

mass_tree* new_mass_tree(int thread_num)
{
  (void)thread_num;
  mass_tree *mt = (mass_tree *)malloc(sizeof(mass_tree));

  node *r = new_node(Border);
  uint32_t v = node_get_version(r);
  v = set_root(v);
  node_set_version(r, v);

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
  uint32_t off = 0;
  assert((int)node_insert(r, &fence, sizeof(uint64_t), &off, n1, 1 /* is_link */) == 1);

  node_set_parent(n, r);
  node_set_parent(n1, r);

  node_unset_root(n);
  node_unset_root(n1);

  return r;
}

// finx the border node and record stable version
static node* find_border_node(node *r, const void *key, uint32_t len, uint32_t *off, uint32_t *version)
{
  uint32_t v, ori = *off;
  node *n;
  retry:
    *off = ori; // index to start comparation
    n = r;
    assert(n);
    v = node_get_stable_version(n);
    // it's possible that a root has split
    if (!is_root(v)) {
      n = node_get_parent(n);
      goto retry;
    }

  descend:
    if (is_border(v)) {
      *version = v;
      return n;
    }

    uint32_t pre = *off; // save the offset of comparation for retry
    node *n1 = node_locate_child(n, key, len, off);
    assert(n1);
    uint32_t v1 = node_get_stable_version(n1);

    // if there is any state change happened to this node, we need to retry
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
    *off = pre; // restore offset
    v = v2;
    goto descend;
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

  // need to set parent here instead of `node_split`
  node_set_parent(n1, p);

  uint32_t v;
  node *p1;
  v = node_get_version(p);
  if (unlikely(is_border(v))) { // `n` is a sub tree
    p1 = mass_tree_grow(n, fence, n1);
    node_swap_child(p, n, p1);
    node_unlock(n);
    node_unlock(n1);
    node_unlock(p);
  } else if (likely(node_is_full(p) == 0)) {
    uint32_t tmp = 0;
    assert((int)node_insert(p, &fence, sizeof(uint64_t), &tmp, n1, 1 /* is_link */) == 1);
    node_unlock(n);
    node_unlock(n1);
    node_unlock(p);
  } else { // node is full
    v = set_split(v);
    node_set_version(p, v);
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
  uint32_t off = 0, v;
  node *r, *n;
  __atomic_load(&mt->root, &r, __ATOMIC_ACQUIRE);

  again:
  n = find_border_node(r, key, len, &off, &v);
  // before we write this node, a lock must be obtained
  node_lock(n);

  uint32_t diff = node_get_version(n) ^ v;
  if (diff != LOCK_BIT) { // node has changed between we acquire this node and lock this node
    while (1) {
      node *next = node_get_next(n);
      if (next == 0)
        // there is no split happened, node is valid
        break;
      // must lock `next` first
      node_lock(next);
      // there might be splits happened, traverse through the link
      if (!node_include_key(next, key, len, off)) {
        node_unlock(next);
        break;
      } else {
        // move to next node
        node_unlock(n);
        n = next;
      }
    }
  }

  void *v = node_insert(n, key, len, &off, val, 0 /* is_link */);
  switch ((uint64_t)v) {
    case 0: // key existed
    case 1: // key inserted
      node_unlock(n);
      return (int)v;
    case -1: { // need to create a deeper layer
      node *n1 = new_node(Border);
      node_set_root(n1);
      void *ckey;
      uint32_t clen;
      int idx = node_get_conflict_key_index(n, key, len, &off, &ckey, &clen);

      uint32_t pre = off;
      assert((int)node_insert(n1, ckey, clen, &off, 0, 0 /* is_link */) == 1);
      off = pre;
      assert((int)node_insert(n1, key, len, &off, val, 0 /* is_link */) == 1);

      node_set_parent(n1, n);

      node_replace_at_index(n, idx, n1);
      return 1;
    }
    case -2: { // node is full, need to split and promote
      uint64_t fence = 0;
      node *n1 = node_split(n, &fence);
      assert(fence);
      uint64_t cur = 0;
      if ((off + sizeof(uint64_t)) > len)
        memcpy(&cur, key, len - off); // other bytes will be 0
      else
        cur = *((uint64_t *)((char *)key + off));
      // equal is not possible
      if (cur < fence)
        assert((int)node_insert(n, key, len, &off, val, 0 /* is_link */) == 1);
      else
        assert((int)node_insert(n1, key, len, &off, val, 0 /* is_link */) == 1);

      mass_tree_promote_split_node(mt, n, fence, n1);
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
  uint32_t off = 0, v;
  node *r, *n;
  __atomic_load(&mt->root, &r, __ATOMIC_ACQUIRE);

  again:
  n = find_border_node(r, key, len, &off, &v);

  forward:
  if (is_deleted(v))
    goto again;

  void *suffix;
  node *next_layer = node_search(n, key, len, &off, &suffix);

  uint32_t diff = node_get_version(n) ^ v;
  if (diff != LOCK_BIT && diff != 0) {
    v = node_get_stable_version(n);
    node *next = node_get_next(n);
    // there might be splits happened, traverse through the link
    // WARN: is there any problem???
    while (!is_deleted(v) && next && node_include_key(next, key, len, off)) {
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
  goto again;
}
