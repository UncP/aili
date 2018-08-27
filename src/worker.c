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

  // TODO: is there a better value?
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

uint32_t worker_get_path_end(worker *w)
{
  return w->end_path;
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
 *  at the same time, but we use some trick to avoid it
 *
 *  after this function, the path and index should be like this:
 *
 *            beg_path                 cur_path          end_path            max_path
 *  |   0   |   1   |   2   |   ...   |   N   |   N+1   |   N+2   |   ...   |   M   |
 *
 *  [cur_path, endpath) is from next worker, after this function,
 *  [0, beg_path) belong to previous worker,
 *  we need to process [beg_path, end_path) for this worker
 *
**/
void worker_resolve_hazards(worker *w)
{
  // we don't modify `cur_path` in this function because this is visible to other workers
  // but we can safely modify `beg_path` and `end_path`

  w->beg_path = 0;

  if (w->prev) {
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
  }
  // now `w->beg_path` will be the index that this worker is responsible to start to process

  w->end_path = w->cur_path;
  if (w->next) {
    path *cp = &w->paths[w->cur_path - 1];
    node *cn = path_get_leaf_node(cp);
    // after this loop, `j` will be the path count that this worker is responsible to process for `w->next`
    for (uint32_t j = 0; j < w->next->cur_path; ++j) {
      path *np = &w->next->paths[i];
      node *nn = path_get_leaf_node(np);
      if (nn != cn) {
        break;
      } else {
        // this path is now owned by this worker
        // TODO: optimization? sizeof(path) currently is 64 bytes, so not a very heavy memcpy
        // TODO: deal memory allocation or just refactor `paths`
        memcpy(&w->paths[w->end_path++], np, sizeof(path));
      }
    }
  }
  // now `w->end_path` will be the index that this worker is responsible to stop to process
}

void worker_clear(worker *w)
{
  // TODO: set max_path to end_path?
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
