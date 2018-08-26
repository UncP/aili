/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <math.h>

#include "palm_tree.h"

palm_tree* new_palm_tree()
{
  palm_tree *pt = (palm_tree *)malloc(sizeof(palm_tree));
  pt->root = new_node(Leaf, 0); // set root as leaf to avoid some adjustment later
  return pt;
}

void free_palm_tree(palm_tree *pt)
{
  // TODO

}

void palm_tree_execute(palm_tree *pt, batch *b, worker *w)
{
  // calculate [beg, end) in a batch that current thread needs to process
  // it's possible that a worker has no key to process
  uint32_t part = (uint32_t)ceilf((float)b->keys / w->total);
  uint32_t beg = w->id * part;
  uint32_t end = beg + part > b->keys ? b->keys;

  uint32_t level = root->level;

  worker_clear(w);

  // for each key, we descend to leaf node, and store each key's descending path
  for (uint32_t i = beg; i < end; ++i) {
    uint32_t  op;
    void    *key;
    uint32_t len;
    void    *val;
    // get basic info of this kv
    assert(batch_read_at(b, i, &op, &key, &len, &val));

    path* p = worker_get_new_path(w);
    path_set_kv_id(p, i);

    // loop until we reach level 0 node, push all the node to `p` along the way
    node *cur = root;
    while (level--) {
      node *pre = cur;
      cur = node_descend(cur, key, len);
      assert(cur);
      path_push_node(p, pre);
    }

    path_push_node(p, cur);
  }

  // TODO: point-to-point synchronization
  // wait until all the threads collected the path information
  barrier_wait(w->barrier);

  // try to find overlap nodes in other worker, if there is a previous worker owns
  // the same leaf node in current worker, it will be processed by previous worker
  int i = 0;
  if (w->prev) {
    path *lp = worker_get_path(w->prev, worker_get_path_count(w->prev) - 1);
    node *ln = path_get_leaf_node(lp);
    // after this loop, `i` will be the index that this worker is responsible to start process
    for (; i < worker_get_path_count(w); ++i) {
      path *cp = worker_get_path_at(w, i);
      node *cn = path_get_leaf_node(cp);
      if (ln != cn)
        break;
    }
  }
  int j = 0;
  if (w->next) {
    path *cp = worker_get_path(w, worker_get_path_count(w) - 1);
    node *cn = path_get_leaf_node(cp);
    // after this loop, `j` will be the path count that this worker is responsible to process for `w->next`
    for (; j < worker_get_path_count(w->next); ++j) {
      path *np = worker_get_path_at(w->next, i);
      node *nn = path_get_leaf_node(np);
      if (nn != cn)
        break;
    }
  }

  // now we can process all the paths that belong to this worker

  char *fence_key;
  uint32_t fence_len;
  node *pre = 0; // previous node
  node *to_process; // node actually to process the key
  node *next = 0; // node next to `to_process`
  for (; i < worker_get_path_count(w); ++i) {
    path *cp = worker_get_path_at(w, i);
    node *cn = path_get_leaf_node(cp);
    uint32_t  op;
    void    *key;
    uint32_t len;
    void    *val;
    assert(batch_read_at(b, path_get_kv_id(cp), &op, &key, &len, &val));

    if (cn == pre) {
      if (next) {
        to_process = compare_key(key, len, fence_key, fence_len) < 0 ? to_process : next;
      } else {
        // `to_process` is the latest node, continue to use it
      }
    } else {
      to_process = cn;
      next = 0; // previous split has no influence on current key
    }

    if (op == Write) {
      switch (node_insert(to_process, key, len, val)) {
      case 1:  // key insert succeed, we set value to 1
        *(val_t *)&val = 1;
        break;
      case 0:  // key already insert, we set value to 0
        *(val_t *)&val = 0;
        break;
      case -1: // node does not have enough space, needs to split
        node *new = new_node(to_process->type, to_process->level);
        // record fence key to for later promotion
        fence *f = worker_get_new_fence(w);
        f->ptr = new;
        f->id = path_get_kv_id(cp);
        node_split(to_process, new, fence.key, &fence.len);
        fence_key = fence.key;
        fence_len = fence.len;

        // compare current key with fence key to determine which node to insert
        if (compare_key(key, len, fence.key, fence.len) < 0) {
          assert(node_insert(to_process, key, len, val) == 1);
          next = new;
        } else {
          assert(node_insert(new, key, len, val) == 1);
          to_process = new;
          next = 0;
        }
        break;
      default:
        assert(0);
      }
    } else { // Read
      *(val_t *)&val = node_search(to_process, key, len);
    }

    pre = cn; // record previous node
  }
}
