/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#ifndef _worker_h_
#define _worker_h_

#include "node.h"

#define channel_size max_descend_depth + 1 // +2 is better but we want `channel_size` to be 8

/**
 *   every thread has a worker, worker does write/read operations to b+ tree,
 *   worker is chained together to form a double-linked list,
 *   every worker only execute operation on paths that are not in conflict with other nodes,
 *   if there is a conflict, worker with smaller id wins.
 *   when descended to leaf nodes, a worker will communicate with prev worker and next worker
 *   to determine which keys belongs to it.
 *
 *   for example:
 *    worker 1 descends to 3 leaf nodes, n1, n2, n3
 *    worker 2 descends to 3 leaf nodes, n3, n4, n5
 *    worker 3 descends to 3 leaf nodes, n5, n6, n7
 *
 *   in this case, worker 1 process n1 n2,
 *                 worker 2 process n3 n4,
 *                 worker 3 process n5 n6 n7,
**/
typedef struct worker
{
  uint32_t  id;        // my id
  uint32_t  total;     // total workers

  uint32_t  max_path;  // maximum path number
  uint32_t  cur_path;  // current path number
  uint32_t  beg_path;  // begin path index this worker needs to process
  uint32_t  tot_path;  // total paths that this worker needs to process
  path     *paths;     // paths for all the keys this worker has

  uint32_t  max_fence;    // maximum number of new node this worker generates
  uint32_t  cur_fence[2]; // current number of new node this worker generates
  uint32_t  beg_fence;    // begin fence index this worker needs to process
  uint32_t  tot_fence;    // total fences that this worker needs to process
  fence    *fences[2];    // to place the fence key info, there are 2 groups for switch
                          // each of them are sorted according to the key
                          // this is a very cool optimization

  struct worker *prev; // previous worker with smaller id
  struct worker *next; // next worker with bigger id

  /* point to point synchronization */
  node *last[channel_size]; // `last` & `first` both take up a cache line
  node *first[channel_size];
  node *their_last;
  node *my_first;
  node *my_last;
  node *their_first;
}worker;

worker* new_worker(uint32_t id, uint32_t total);
void free_worker(worker* w);
void worker_link(worker *a, worker *b);
path* worker_get_new_path(worker *w);
path* worker_get_path_at(worker *w, uint32_t idx);
void worker_update_fence(worker *w, uint32_t level, fence *f, uint32_t i);
void worker_switch_fence(worker *w, uint32_t level);
void worker_get_fences(worker *w, uint32_t level, fence **fences, uint32_t *number);
void worker_redistribute_work(worker *w, uint32_t level);
void worker_reset(worker *w);
void worker_sync(worker *w, uint32_t level, uint32_t root_level);
void worker_execute_on_leaf_nodes(worker *w, batch *b);
void worker_execute_on_branch_nodes(worker *w, uint32_t level);

#ifdef Test

void worker_print_path_info(worker *w);
void worker_print_fence_info(worker *w, uint32_t level);

#endif

#endif /* _worker_h_ */