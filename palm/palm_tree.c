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
#include "allocator.h"

static const char *stage_descend  = "descend to leaf";
static const char *stage_sync     = "worker sync";
static const char *stage_redis    = "redistribute work";
static const char *stage_leaves   = "modify leaves";
static const char *stage_branches = "modify braches";
static const char *stage_root     = "modify root";

static void do_palm_tree_execute(palm_tree *pt, batch *b, worker *w);

typedef struct thread_arg
{
  palm_tree *pt;
  worker    *wrk;
  bounded_queue *que;
}thread_arg;

static thread_arg* new_thread_arg(palm_tree *pt, worker *w, bounded_queue *q)
{
  thread_arg *j = (thread_arg *)malloc(sizeof(thread_arg));
  j->pt  = pt;
  j->wrk = w;
  j->que = q;

  return j;
}

static void free_thread_arg(thread_arg *j)
{
  free((void *)j);
}

static void* run(void *arg)
{
  thread_arg *j = (thread_arg *)arg;
  palm_tree *pt = j->pt;
  worker *w= j->wrk;
  bounded_queue *q = j->que;
  int q_idx = 0;

  while (1) {
    // TODO: optimization?
    batch *bth = bounded_queue_get_at(q, &q_idx); // q_idx will be updated in the queue

    if (likely(bth))
      do_palm_tree_execute(pt, bth, w);
    else
      break;

    // let worker 0 do the dequeue
    if (w->id == 0)
      bounded_queue_dequeue(q);
  }

  free_thread_arg(j);
  return 0;
}

palm_tree* new_palm_tree(int worker_num, int queue_size)
{
#ifdef Allocator
  init_allocator();
#endif

  if (worker_num <= 0) worker_num = 1;

  init_metric(worker_num);

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

  pt->worker_num = worker_num;
  pt->queue = new_bounded_queue(queue_size);
  pt->ids = (pthread_t *)malloc(sizeof(pthread_t) * pt->worker_num);
  pt->workers = (worker **)malloc(sizeof(worker *) * pt->worker_num);

  for (int i = 0; i < pt->worker_num; ++i) {
    pt->workers[i] = new_worker(i, pt->worker_num);
    if (i > 0)
      worker_link(pt->workers[i - 1], pt->workers[i]);
  }

  for (int i = 0; i < pt->worker_num; ++i) {
    thread_arg *arg = new_thread_arg(pt, pt->workers[i], pt->queue);
    assert(pthread_create(&pt->ids[i], 0, run, (void *)arg) == 0);
  }

  return pt;
}

void free_palm_tree(palm_tree *pt)
{
  bounded_queue_clear(pt->queue);

  // collect all the child threads
  for (int i = 0; i < pt->worker_num; ++i)
    assert(pthread_join(pt->ids[i], 0) == 0);

  free_bounded_queue(pt->queue);

  for (int i = 0; i < pt->worker_num; ++i)
    free_worker(pt->workers[i]);

  free((void *)pt->workers);
  free((void *)pt->ids);

  // free the entire palm tree recursively
  free_btree_node(pt->root);

  free((void *)pt);

  free_metric();
}

// finish all the task batch in the queue
void palm_tree_flush(palm_tree *pt)
{
  bounded_queue_wait_empty(pt->queue);
}

// put task batch in the queue
void palm_tree_execute(palm_tree *pt, batch *b)
{
  bounded_queue_enqueue(pt->queue, b);
}

#ifdef Test

void palm_tree_validate(palm_tree *pt)
{
  node *ptr = pt->root;
  uint32_t total_count = 0;
  float total_coverage = 0;
  float average_prefix = 0;
  while (ptr) {
    node *next = ptr->first;
    node *cur = ptr;
    uint32_t count = 0;
    float coverage = 0;
    uint32_t less50 = 0;
    uint32_t less60 = 0;
    uint32_t less70 = 0;
    uint32_t less80 = 0;
    while (cur) {
      btree_node_validate(cur);
      if (cur->level == 0) average_prefix += cur->pre;
      float c = node_get_coverage(cur);
      if (c < 0.5) ++less50;
      if (c < 0.6) ++less60;
      if (c < 0.7) ++less70;
      if (c < 0.8) ++less80;
      coverage += c;
      ++count;
      cur = cur->next;
    }
    printf("level %u:  count: %-4u  coverage: %.2f%%  <50%%: %-4u  <60%%: %-4u  <70%%: %-4u  <80%%: %-4u\n",
      ptr->level, count, (coverage * 100 / count), less50, less60, less70, less80);
    total_count += count;
    total_coverage += coverage;
    average_prefix /= count;
    ptr = next;
  }
  printf("average prefix length: %.2f\n", average_prefix);
  printf("total node count: %u\naverage coverage: %.2f%%\n",
    total_count, total_coverage * 100 / total_count);
}

#endif /* Test */

// only processed by worker 0
static void handle_root_split(palm_tree *pt, worker *w)
{
  uint32_t number;
  fence *fences;
  worker_get_fences(w, pt->root->level, &fences, &number);

  if (likely(number == 0)) return ;

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

#ifdef Lazy
// descend to leaf node for key at `kidx`, using path at `pidx`
static void descend_to_leaf_single(node *r, batch *b, worker *w, uint32_t kidx, uint32_t pidx)
{
  uint32_t  op;
  void    *key;
  uint32_t len;
  void    *val;
  // get kv info
  batch_read_at(b, kidx, &op, &key, &len, &val);

  path* p = worker_get_path_at(w, pidx);

  // loop until we reach level 0, push all the node to `p` along the way
  uint32_t level = r->level;
  node *cur = r;
  while (level--) {
    node *pre = cur;
    cur = node_descend(cur, key, len);
    // TODO: remove this
    assert(pre && pre->level);
    path_push_node(p, pre);
  }

  // TODO: remove this
  assert(cur && !cur->level);
  path_push_node(p, cur);
}

// this function is used for lazy descending, for key range [key_a, key_b],
// if path a and path b falls into the same leaf node, all the keys between them
// must fall into the same leaf node since they are sorted,
// so we can avoid descending for each key, this is especially useful
// when the palm tree is small or the key is close to each other
// TODO: use loop to replace recursion
static void descend_for_range(node *r, batch *b, worker *w, uint32_t kbeg, uint32_t kend, uint32_t pidx)
{
  if ((kbeg + 1) >= kend) return ;

  path *lp = worker_get_path_at(w, pidx);
  path *rp = worker_get_path_at(w, pidx + kend - kbeg);
  if (path_get_node_at_level(lp, 0) != path_get_node_at_level(rp, 0)) {
    uint32_t kmid = (kbeg + kend) / 2;
    descend_to_leaf_single(r, b, w, kmid, pidx + kmid - kbeg);
    descend_for_range(r, b, w, kbeg, kmid, pidx);
    descend_for_range(r, b, w, kmid, kend, pidx + kmid - kbeg);
  } else {
    // all the keys in [kbeg, kend] fall into the same leaf node,
    // they must all have the exact same path, so copy path for keys in (kbeg, kend)
    uint32_t between = kend - kbeg - 1;
    for (uint32_t i = 1; i <= between; ++i)
      path_copy(lp, worker_get_path_at(w, pidx + i));
  }
}
#endif /* Lazy */

// we descend to leaf node for each key in [beg, end), and store each key's descending path.
// there are 3 descending policy to choose:
//   1. lazy descend: like dfs, but with some amazing optimization, great for sequential insertion
//   2. level descend: like bfs, good for cache locality
//   3. zigzag descend: invented by myself, also good for cache locality
static void descend_to_leaf(palm_tree *pt, batch *b, uint32_t beg, uint32_t end, worker *w)
{
  if (beg == end) return ;

#ifdef Lazy  // lazy descend
  for (uint32_t i = beg; i < end; ++i) {
    path* p = worker_get_new_path(w);
    path_set_kv_id(p, i);
  }

  uint32_t pidx = 0;
  descend_to_leaf_single(pt->root, b, w, beg, pidx);
  if (--end > beg) {
    descend_to_leaf_single(pt->root, b, w, end, pidx + end - beg);
    descend_for_range(pt->root, b, w, beg, end, pidx);
  }
#elif Level  // level descend
  for (uint32_t i = beg; i < end; ++i) {
    path* p = worker_get_new_path(w);
    path_set_kv_id(p, i);
    path_push_node(p, pt->root);
  }

  for (uint32_t level = pt->root->level, idx = 0; level; --level, ++idx) {
    for (uint32_t i = beg, j = 0; i < end; ++i, ++j) {
      uint32_t  op;
      void    *key;
      uint32_t len;
      void    *val;
      // get kv info
      batch_read_at(b, i, &op, &key, &len, &val);
      path *p = worker_get_path_at(w, j);
      node *cur = path_get_node_at_index(p, idx);
      cur = node_descend(cur, key, len);
      node_prefetch(cur);
      path_push_node(p, cur);
    }
  }
#else  // zigzag descend
  for (uint32_t i = beg; i < end; ++i) {
    path* p = worker_get_new_path(w);
    path_set_kv_id(p, i);
    path_push_node(p, pt->root);
  }

  // make sure that we process each key from left to right in level 0 for better cache locality
  // 1 means left to right, -1 means right to left
  int direction = ((pt->root->level % 2) == 0) ? 1 : -1;
  for (uint32_t level = pt->root->level, idx = 0; level; --level, ++idx, direction *= -1) {
    int i, e, j;
    if (direction == 1)
      i = beg, e = end, j = 0;
    else
      i = end - 1, e = (int)beg - 1, j = end - beg - 1;
    for (; i != e; i += direction, j += direction) {
      uint32_t  op;
      void    *key;
      uint32_t len;
      void    *val;
      // get kv info
      batch_read_at(b, (uint32_t)i, &op, &key, &len, &val);
      path *p = worker_get_path_at(w, (uint32_t)j);
      node *cur = path_get_node_at_index(p, idx);
      cur = node_descend(cur, key, len);
      node_prefetch(cur);
      path_push_node(p, cur);
    }
  }
#endif
}

// Reference: Parallel Architecture-Friendly Latch-Free Modifications to B+ Trees on Many-Core Processors
// this is the entrance for all the write/read operations
static void do_palm_tree_execute(palm_tree *pt, batch *b, worker *w)
{
  worker_reset(w);

  // get root level here to prevent dead lock bug when promoting node modifications
  uint32_t root_level = pt->root->level;
  struct clock c = clock_get();

  /*  ---  Stage 1  --- */

  // calculate [beg, end) in a batch that current thread needs to process
  // it's possible that a worker has no key to process
  uint32_t part = (uint32_t)ceilf((float)b->keys / w->total);
  uint32_t beg = w->id * part > b->keys ? b->keys : w->id * part;
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

  // do a global synchronization, not really needed, but just make things consistent
  worker_sync(w, level + 1, root_level); update_metric(w->id, stage_sync, &c);
}
