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
#include "metric.h"

static const char *stage_descend  = "descend to leaf";
static const char *stage_sync     = "worker sync";
static const char *stage_redis    = "redistribute work";
static const char *stage_leaves   = "modify leaves";
static const char *stage_branches = "modify braches";
static const char *stage_root     = "modify root";

palm_tree* new_palm_tree(int worker_num)
{
  for (int i = 0; i < worker_num; ++i) {
    register_metric(i, stage_descend, (void *)new_clock());
    register_metric(i, stage_sync, (void *)new_clock());
    register_metric(i, stage_redis, (void *)new_clock());
    register_metric(i, stage_leaves, (void *)new_clock());
    register_metric(i, stage_branches, (void *)new_clock());
    register_metric(i, stage_root, (void *)new_clock());
  }

  palm_tree *pt = (palm_tree *)malloc(sizeof(palm_tree));
  pt->root = new_node(Root, 0);

  return pt;
}

// free the entire palm tree recursively
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

// only processed by worker 0
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

  for (uint32_t i = 0; i < number; ++i) {
    assert(node_insert(new_root, fences[i].key, fences[i].len, fences[i].ptr) == 1);
  }

  // replace old root
  pt->root = new_root;
}

// for each key in [beg, end), we descend to leaf node, and store each key's descending path
static void descend_to_leaf(palm_tree *pt, batch *b, uint32_t beg, uint32_t end, worker *w)
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
    uint32_t level = pt->root->level;
    node *cur = pt->root;
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

// this is the entrance for all the write/read operations
void palm_tree_execute(palm_tree *pt, batch *b, worker *w)
{
  // get root level to prevent dead lock bug when promoting node modifications
  uint32_t root_level = pt->root->level;

  struct clock c = clock_get();

  /*  ---  Stage 1  --- */

  // calculate [beg, end) in a batch that current thread needs to process
  // it's possible that a worker has no key to process
  uint32_t part = (uint32_t)ceilf((float)b->keys / w->total);
  uint32_t beg = w->id * part;
  uint32_t end = beg + part > b->keys ? b->keys : beg + part;

  // descend to leaf for each key that belongs to this worker in this batch
  descend_to_leaf(pt, b, beg, end, w); update_metric(w->id, stage_descend, &c);

  worker_sync(w, 0 /* level */, root_level); update_metric(w->id, stage_sync, &c);

  /*  ---  Stage 2  --- */

  // try to find overlap nodes in previoud worker and next worker,
  // if there is a previous worker owns the same leaf node in current worker,
  // it will be processed by previous worker
  worker_redistribute_work(w, 0 /* level */); update_metric(w->id, stage_redis, &c);

  // now we process all the paths that belong to this worker
  worker_execute_on_leaf_nodes(w, b); update_metric(w->id, stage_leaves, &c);

  worker_sync(w, 1 /* level */, root_level); update_metric(w->id, stage_sync, &c);

  /*  ---  Stage 3  --- */

  // fix the split level by level
  uint32_t level = 1;
  while (level <= root_level) {
    worker_redistribute_work(w, level); update_metric(w->id, stage_redis, &c);

    worker_execute_on_branch_nodes(w, level); update_metric(w->id, stage_branches, &c);

    ++level;

    worker_sync(w, level, root_level); update_metric(w->id, stage_sync, &c);

    // this is a very fucking smart and elegant optimization, we use `level` as an external
    // switch value, although `level` is on each thread's stack, it is globally equal for
    // workers in the same synchronization group at each stage, so it can be used to avoid
    // concurrency problems and save a lot of small but frequent memory allocation for
    // split information at the same time
    worker_switch_fence(w, level);
  }

  /*  ---  Stage 4  --- */

  if (w->id == 0) {
    handle_root_split(pt, w); update_metric(w->id, stage_root, &c);
  }

  // reset worker here to avoid concurrency problems
  worker_reset(w);
}
