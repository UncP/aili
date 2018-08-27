/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#ifndef _worker_h_
#define _worker_h_

#include "node.h"
#include "barrier.h"

typedef struct fence
{
  uint32_t  id;                    // path id
  uint32_t  len;                   // key length
  char      key[max_key_size + 1]; // key data, +1 for alignment
  node     *ptr;                   // new node pointer
}fence;

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
  barrier  *bar;       // barrier of the worker pool

  uint32_t  max_path;  // maximum path number
  uint32_t  cur_path;  // current path number
  uint32_t  beg_path;  // begin path index this worker needs to process
  uint32_t  end_path;  // end path index this worker needs to process
  path     *paths;     // paths for all the keys this worker has

  uint32_t  max_fence; // maximum number of new node this worker generates
  uint32_t  cur_fence; // current number of new node this worker generates
  fence    *fences;    // to place the fence key info, works like

  worker   *prev;      // previous worker with smaller id
  worker   *next;      // next worker with bigger id
}worker;

worker* new_worker(uint32_t id, uint32_t total, barrier *b);
void free_worker(worker* w);
path* worker_get_new_path(worker *w);
path* worker_get_path_at(worker *w, uint32_t idx);
uint32_t worker_get_path_beg(worker *w);
uint32_t worker_get_path_end(worker *w);
fence* worker_get_new_fence(worker *w);
fence* worker_get_fence_at(worker *w, uint32_t idx);
uint32_t worker_get_fence_count(worker *w);
void worker_resolve_hazards(worker *w);
void worker_clear(worker *w);
void worker_link(worker *a, worker *b);

#endif /* _worker_h_ */
