/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#include <assert.h>

#include "worker.h"

worker* new_worker(uint32_t id, uint32_t total, barrier *b)
{
  assert(id < total);

  worker *w = (worker *)malloc(sizeof(worker));
  w->id = id;
  w->total = total;
  w->bar = b;

  // TODO: is there a better value?
  w->max_path = 64;
  w->cur_path = 0;
  w->paths = (path *)malloc(sizeof(path) * w->max_path);

  // TODO: change this to `max_descend_depth`?
  // 4 is enough even in extreme situations, so it's not likely
  // more memory will be required
  w->max_fence = 4;
  w->cur_fence = 0;
  w->fences = (fence *)malloc(sizeof(fence) * w->max_fence);

  w->prev = 0;
  w->next = 0;

  return w;
}

void free_worker(worker* w)
{
  free((void *)w->fences);
  free((void *)w->paths);
  free((void *)w);
}

path* worker_get_new_path(worker *w)
{
  // TODO: optimize memory allocation?
  if (w->cur_path == w->max_path) {
    w->max_path = (uint32_t)((float)w->max_path * 1.5);
    w->paths = (path *)realloc(w->paths, sizeof(path) * w->max_path);
  }
  // TODO: remove this
  assert(w->cur_path < w->max_path);
  return &w->paths[w->cur_path++];
}

path* worker_get_path_at(worker *w, uint32_t idx)
{
  // TODO: remove this
  assert(w->beg_path > idx);
  return &w->paths[idx];
}

uint32_t worker_get_path_beg(worker *w)
{
  return w->beg_path;
}

fence* worker_get_new_fence(worker *w)
{
  // TODO: optimize memory allocation?
  if (w->cur_fence == w->max_fence) {
    w->max_fence = (uint32_t)((float)w->max_fence * 1.5);
    w->fences = (fence *)realloc(w->fences, sizeof(fence) * w->max_fence);
  }
  assert(w->cur_path < w->max_path);
  return &w->fences[w->cur_fence++];
}

fence* worker_get_fence_at(worker *w, uint32_t idx)
{
  assert(w->cur_fence > idx);
  return &w->fences[idx];
}

uint32_t worker_get_fence_count(worker *w)
{
  return w->cur_fence;
}

/**
 *  since two paths may descend to the same leaf node, but only one worker
 *  can write that node, so this function is to find out the paths that
 *  this worker should process. there are modifications to worker's path that
 *  may cause concurrency problems since this function is called by each worker
 *  at the same time, but we use some trick to avoid it.
 *  there is a chance that this worker will not have any path to process,
 *  especially when sequential insertion happens
 *
 *  this function is actually very neat :), it does not only solve the concurrency
 *  problem, but also avoids the memory allocation problem
**/
void worker_resolve_hazards(worker *w)
{
  // this worker does not have any path
  if (w->cur_path == 0) {
    w->tot_path = 0;
    return ;
  }

  // we don't modify `cur_path` in this function because this is visible to other workers
  // but we can safely modify `beg_path`

  // decide which path we start to process in this worker
  if (w->prev) {
    // we set `beg_path` to `cur_path` because there is a chance that no path in this worker
    // will be processed by this worker
    w->beg_path = w->cur_path;

    path *lp = &w->prev->paths[w->prev->cur_path - 1];
    node *ln = path_get_leaf_node(lp);
    for (uint32_t i = 0; i < w->cur_path; ++i) {
      path *cp = &w->paths[i];
      node *cn = path_get_leaf_node(cp);
      if (ln != cn) {
        w->beg_path = i;
        break;
      }
    }
  } else {
    // since there is no previous worker, all the paths in this worker are processed by this worker
    w->beg_path = 0;
  }

  // calculate the paths needs to process in this worker
  w->tot_path = w->cur_path - w->beg_path;

  path *lp = &w->paths[w->cur_path - 1];
  node *ln = path_get_leaf_node(lp);
  // calculate the paths needs to process in other workers, it may cover several workers
  // escpecially when sequential insertion happens
  worker *next = w->next;
  while (next && next->cur_path) {
    for (uint32_t i = 0; i < next->cur_path; ++i) {
      path *np = &next->paths[i];
      node *nn = path_get_leaf_node(np);
      if (nn != ln) {
        // once there is a path leaf node not the same with this worker's last path leaf node, return
        return;
      } else if (w->tot_path) {
        // it's possible that this worker does not have any path to process,
        // so we need to test `tot_path` to determine whether this path in next worker belongs to
        // previous worker
        ++w->tot_path;
      }
    }
    next = next->next;
  }
}

void worker_clear(worker *w)
{
  // TODO: set max_path to cur_path?
  for (uint32_t i = 0; i < w->max_path; ++i)
    path_clear(&w->paths[i]);
  w->cur_path = 0;

  for (uint32_t i = 0; i < w->max_fence; ++i)
    w->fences[i].len = 0;
  w->cur_fence = 0;
}

void worker_link(worker *a, worker *b)
{
  a->next = b;
  b->prev = a;
}

void init_path_iter(path_iter *iter, worker *w)
{
  assert(w);
  iter->current = 0;
  iter->total   = w->tot_path;
  iter->offset  = w->beg_path;
  iter->owner   = w;
}

path* next_path(path_iter *iter)
{
  if (iter->current++ == iter->total)
    return 0;

  if (iter->offset == w->cur_path) {
    iter->owner = iter->owner->next;
    assert(iter->owner && iter->owner->cur_path);
    iter->offset = 0;
  }

  return &iter->owner->paths[iter->offset++];
}
