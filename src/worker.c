/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#include <assert.h>
#include <stdlib.h>

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
  w->beg_path = 0;
  w->tot_path = 0;
  w->paths = (path *)malloc(sizeof(path) * w->max_path);

  // TODO: change this to `max_descend_depth`?
  // 4 is enough even in extreme situations, so it's not likely
  // more memory will be required
  w->max_fence = 4;
  w->cur_fence[0] = 0;
  w->cur_fence[1] = 0;
  w->fences[0] = (fence *)malloc(sizeof(fence) * w->max_fence);
  w->fences[1] = (fence *)malloc(sizeof(fence) * w->max_fence);

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

// this is a FUCKING genius optimization!!!
void worker_switch_fence(worker *w, uint32_t level)
{
  w->cur_fence[level % 2] = 0;
}

fence* worker_get_new_fence(worker *w, uint32_t level)
{
  uint32_t idx = level % 2;
  uint32_t *cur_fence = &w->cur_fence[idx];
  // TODO: optimize memory allocation?
  if (*cur_fence == w->max_fence) {
    w->max_fence = (uint32_t)((float)w->max_fence * 1.5);
    w->fences[idx] = (fence *)realloc(w->fences[idx], sizeof(fence) * w->max_fence);
  }
  // TODO: remove this
  assert(*cur_fence < w->max_fence);
  return &w->fences[idx][*cur_fence++];
}

void worker_get_fences(worker *w, uint32_t level, fence **fences, uint32_t *number)
{
  *number = w->cur_fence[level % 2];
  *fences = w->fences[level % 2];
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
 *  this function is actually very neat :), it does not only avoid the concurrency
 *  problem, but also avoids the memory allocation problem
**/
void worker_redistribute_work(worker *w)
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

    // `w->prev->cur_path` can't be 0 if this worker has non-zero path
    path *lp = &w->prev->paths[w->prev->cur_path - 1];
    node *ln = path_get_node_at_level(lp, 0);
    for (uint32_t i = 0; i < w->cur_path; ++i) {
      path *cp = &w->paths[i];
      node *cn = path_get_node_at_level(cp, 0);
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

  // if we don't have any path for this worker, we can return directly since
  // there will not be any path for this worker in next workers
  if (w->tot_path == 0)
    return ;

  path *lp = &w->paths[w->cur_path - 1];
  node *ln = path_get_node_at_level(lp, 0);
  // calculate the paths needs to process in other workers, it may cover several workers
  // escpecially when sequential insertion happens
  worker *next = w->next;
  while (next && next->cur_path) {
    for (uint32_t i = 0; i < next->cur_path; ++i) {
      path *np = &next->paths[i];
      node *nn = path_get_node_at_level(np, 0);
      // once there is a path leaf node not the same with this worker's last path leaf node, return
      if (nn != ln)
        return ;
      else
        ++w->tot_path;
    }
    next = next->next;
  }
}

// this function does exactly the same work as `worker_redistribute_work`
// but with some critical difference
void worker_redistribute_split_work(worker *w, uint32_t level)
{
  uint32_t idx = (level - 1) % 2;
  uint32_t cur_fence = w->cur_fence[idx];
  // no split, return directly
  if (cur_fence == 0) {
    w->tot_fence = 0;
    return ;
  }

  // make sure there is a previous worker and previous worker has split
  if (w->prev && w->prev->cur_fence[idx]) {
    w->beg_fence = cur_fence;

    path *lp = w->prev->fences[idx][w->prev->cur_fence[idx] - 1].pth;
    node *ln = path_get_node_at_level(lp, level);

    for (uint32_t i = 0; i < cur_fence; ++i) {
      path *cp = w->fences[idx][i].pth;
      node *cn = path_get_node_at_level(cp, level);
      if (ln != cn) {
        w->beg_fence = i;
        break;
      }
    }
  } else {
    w->beg_fence = 0;
  }

  w->tot_fence = cur_fence - w->beg_fence;

  // we don't return here if `w->tot_fence == 0` because there might be
  // a split in next worker but belongs to this worker

  path *lp = w->fences[idx][cur_fence - 1].pth;
  node *ln = path_get_node_at_level(lp, level);

  worker *next = w->next;
  // we can't do an early termination like the function above because
  // it's possible that next worker does not has split, but next next worker
  // does, they might land on the same internal node
  while (next) {
    uint32_t next_fence = next->cur_fence[idx];
    fence *fences = next->fences[idx];
    for (uint32_t i = 0; i < next_fence; ++i) {
      path *np = fences[i].pth;
      node *nn = path_get_node_at_level(np, level);
      if (nn != ln)
        return ;
      else
        ++w->tot_fence;
    }
    next = next->next;
  }
}

void worker_reset(worker *w)
{
  // TODO: set max_path to cur_path?
  for (uint32_t i = 0; i < w->max_path; ++i)
    path_clear(&w->paths[i]);
  w->cur_path = 0;

  w->cur_fence[0] = 0;
  w->cur_fence[1] = 0;
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

  if (iter->offset == iter->owner->cur_path) {
    iter->owner = iter->owner->next;
    assert(iter->owner && iter->owner->cur_path);
    iter->offset = 0;
  }

  return &iter->owner->paths[iter->offset++];
}

void init_fence_iter(fence_iter *iter, worker *w, uint32_t level)
{
  assert(w && level);
  iter->level   = (level - 1) % 2;
  iter->current = 0;
  iter->total   = w->tot_fence;
  iter->offset  = w->beg_fence;
  iter->owner   = w;
}

fence* next_fence(fence_iter *iter)
{
  if (iter->current++ == iter->total)
    return 0;

  // loop until we find a worker has split, it's impossible this is a dead loop
  while (iter->offset == iter->owner->cur_fence[iter->level]) {
    iter->owner = iter->owner->next;
    // current owner may not have any split, remove assert `cur_fence`
    assert(iter->owner);
    iter->offset = 0;
  }

  return &iter->owner->fences[iter->level][iter->offset++];
}
