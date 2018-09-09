/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
// TODO: remove this
#include <stdio.h>

#include "palm_tree.h"

palm_tree* new_palm_tree()
{
  palm_tree *pt = (palm_tree *)malloc(sizeof(palm_tree));
  pt->root = new_node(Root, 0);
  return pt;
}

void free_palm_tree(palm_tree *pt)
{
  free_btree_node(pt->root);
}

#ifdef Test

void palm_tree_validate(palm_tree *pt)
{
  node *ptr = pt->root;
  while (ptr) {
    node *next = ptr->first;
    node *cur = ptr;
    while (cur) {
      btree_node_validate(cur);
      cur = cur->next;
    }
    ptr = next;
  }
}

#endif /* Test */

static void handle_root_split(palm_tree *pt, worker *w)
{
  uint32_t number;
  fence *fences;
  worker_get_fences(w, pt->root->level, &fences, &number);

  if (number == 0) return ;

  node *new_root = new_node(Root, pt->root->level + 1);
  // adjust old root type
  pt->root->type = pt->root->level == 0 ? Leaf : Branch;
  // set old root as new root's first child
  new_root->first = pt->root;

  for (uint32_t i = 0; i < number; ++i)
    assert(node_insert(new_root, fences[i].key, fences[i].len, fences[i].ptr) == 1);

  // replace old root
  pt->root = new_root;
}

// for each key in [beg, end), we descend to leaf node, and store each key's descending path
static void descend_to_leaf(node *root, batch *b, uint32_t beg, uint32_t end, worker *w)
{
  for (uint32_t i = beg; i < end; ++i) {
    uint32_t  op;
    void    *key;
    uint32_t len;
    void    *val;
    // get kv info
    assert(batch_read_at(b, i, &op, &key, &len, &val));

    path* p = worker_get_new_path(w);
    path_set_kv_id(p, i);

    // loop until we reach level 0 node, push all the node to `p` along the way
    uint32_t level = root->level;
    node *cur = root;
    while (level--) {
      node *pre = cur;
      cur = node_descend(cur, key, len);
      assert(cur);
      path_push_node(p, pre);
    }

    // TODO: remove this
    assert(cur && cur->level == 0);

    path_push_node(p, cur);
  }
}

// process keys assigned to this worker in leaf nodes, worker has already obtained the path information
static void execute_on_leaf_nodes(batch *b, worker *w)
{
  char *fence_key;
  uint32_t fence_len;
  node *pn = 0; // previous node
  node *to_process; // node actually to process the key
  node *next = 0; // node next to `to_process`

  path_iter iter;
  path *cp;

  init_path_iter(&iter, w);
  // iterate all the path and write or read the key in the leaf node
  while ((cp = next_path(&iter))) {
    node *cn = path_get_node_at_level(cp, 0);
    // TODO: remove this
    assert(cn);

    uint32_t  op;
    void    *key;
    uint32_t len;
    void    *val;
    assert(batch_read_at(b, path_get_kv_id(cp), &op, &key, &len, &val));

    if (cn == pn) {
      if (next && compare_key(key, len, fence_key, fence_len) >= 0) {
        to_process = next;
        next = 0;
      }
    } else {
      to_process = cn;
      next = 0; // previous split has no influence on current key
    }

    if (op == Write) {
      switch (node_insert(to_process, key, len, (const void *)*(val_t *)val)) {
        case 1:  // key insert succeed, we set value to 1
          set_val(val, 1);
          break;
        case 0:  // key already inserted, we set value to 0
          set_val(val, 0);
          break;
        case -1: { // node does not have enough space, needs to split
          node *nn = new_node(Leaf, to_process->level);
          // record fence key for later promotion
          fence *f = worker_get_new_fence(w, 0);
          f->pth = cp;
          f->ptr = nn;
          node_split(to_process, nn, f->key, &f->len);
          fence_key = f->key;
          fence_len = f->len;

          // compare current key with fence key to determine which node to insert
          if (compare_key(key, len, fence_key, fence_len) < 0) {
            assert(node_insert(to_process, key, len, (const void *)*(val_t *)val) == 1);
            next = nn;
          } else {
            assert(node_insert(nn, key, len, (const void *)*(val_t *)val) == 1);
            to_process = nn;
            next = 0;
          }
          set_val(val, 1);
          break;
        }
        default:
          assert(0);
      }
    } else { // Read
      set_val(val, (val_t)node_search(to_process, key, len));
    }

    pn = cn; // record previous node
  }
}

// this function does exactly the same work as `execute_on_leaf_nodes`,
// but with some critical difference
// TODO: maybe they can be combined?
static void execute_on_branch_nodes(worker *w, uint32_t level)
{
  char *fence_key;
  uint32_t fence_len;
  node *pn = 0; // previous node
  node *to_process; // node actually to process the key
  node *next = 0; // node next to `to_process`

  fence_iter iter;
  fence *cf;

  init_fence_iter(&iter, w, level);
  // iterate all the fence and insert key in the branch node
  while ((cf = next_fence(&iter))) {
    path *cp = cf->pth;
    node *cn = path_get_node_at_level(cp, level);
    // TODO: remove this
    assert(cn);

    void    *key = (void *)cf->key;
    uint32_t len = cf->len;
    void    *val = cf->ptr;

    if (cn == pn) {
      if (next && compare_key(key, len, fence_key, fence_len) >= 0) {
        to_process = next;
        next = 0;
      }
    } else {
      to_process = cn;
      next = 0; // previous split has no influence on current key
    }

    switch (node_insert(to_process, key, len, val)) {
      case 1:  // key insert succeed
        break;
      case 0:  // key already inserted
        assert(0);
        break;
      case -1: { // node does not have enough space, needs to split
        node *nn = new_node(Branch, to_process->level);
        // record fence key for later promotion
        fence *f = worker_get_new_fence(w, level);
        f->pth = cp;
        f->ptr = nn;
        node_split(to_process, nn, f->key, &f->len);
        fence_key = f->key;
        fence_len = f->len;

        // compare current key with fence key to determine which node to insert
        if (compare_key(key, len, fence_key, fence_len) < 0) {
          assert(node_insert(to_process, key, len, val) == 1);
          next = nn;
        } else {
          assert(node_insert(nn, key, len, val) == 1);
          to_process = nn;
          next = 0;
        }
        break;
      }
      default:
        assert(0);
    }

    pn = cn;
  }
}

// this is the entrance for all the write/read operations
void palm_tree_execute(palm_tree *pt, batch *b, worker *w)
{
  worker_reset(w);

  // calculate [beg, end) in a batch that current thread needs to process
  // it's possible that a worker has no key to process
  uint32_t part = (uint32_t)ceilf((float)b->keys / w->total);
  uint32_t beg = w->id * part;
  uint32_t end = beg + part > b->keys ? b->keys : beg + part;

  // descend to leaf for each key that belongs to this worker in this batch
  descend_to_leaf(pt->root, b, beg, end, w);

  // TODO: point-to-point synchronization
  // wait until all the worker collected the path information
  if (w->bar) barrier_wait(w->bar);

  // try to find overlap nodes in previoud worker and next worker,
  // if there is a previous worker owns the same leaf node in current worker,
  // it will be processed by previous worker
  worker_redistribute_work(w);

  // now we process all the paths that belong to this worker
  execute_on_leaf_nodes(b, w);

  // wait until all the worker finished leaf node operation
  if (w->bar) barrier_wait(w->bar);

  // TODO: early temination
  // fix the split level by level
  uint32_t level = 1, root_level = pt->root->level;
  while (level <= root_level) {
    worker_redistribute_split_work(w, level);

    execute_on_branch_nodes(w, level);

    if (w->bar) barrier_wait(w->bar);

    ++level;

    // this is a very fucking smart and simple and elegant optimization, we use `level` as an external
    // synchronization value, although `level` is on each thread's stack, it is
    // globally equal at the same stage, so it can be used to avoid concurrency problems and
    // save a lot of small but frequent memory allocation for split information at the same time
    worker_switch_fence(w, level);
  }

  if (w->id == 0)
    handle_root_split(pt, w);

  // we don't need to sync here since if there is a root split, it always falls on worker 0
  return ;
}
