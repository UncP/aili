/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#include <assert.h>
#include <stdlib.h>
#include <string.h>
// TODO: remove this
#include <stdio.h>

#include "worker.h"

// a magic number for pointer, no pointer will be equal with it
#define magic_pointer (node *)913

// a channel specially tailored for palm tree algorithm, not the channel you see in Go or Rust
struct _channel
{
  int    total;
  void **last;
  void **first;
};

channel* new_channel(int total)
{
  channel *c = (channel *)malloc(sizeof(channel));

  c->total = total;
  c->first = (void **)calloc(c->total, sizeof(void *));
  c->last  = (void **)calloc(c->total, sizeof(void *));

  return c;
}

void free_channel(channel *c)
{
  free((void *)c->last);
  free((void *)c->first);

  free((void *)c);
}

void channel_reset(channel *c)
{
  memset(c->first, 0, c->total * sizeof(void *));
  memset(c->last,  0, c->total * sizeof(void *));
}

void channel_set_first(channel *c, uint32_t idx, void *ptr)
{
  assert(idx < c->total);
  __sync_val_compare_and_swap((uint64_t *)c->first[idx], 0, (uint64_t)ptr);
}

void channel_set_last(channel *c, uint32_t idx, void *ptr)
{
  assert(idx < c->total);
  __sync_val_compare_and_swap((uint64_t *)c->last[idx], 0, (uint64_t)ptr);
}

void* channel_get_first(channel *c, uint32_t idx)
{
  assert(idx < c->total);
  return (void *)__sync_val_compare_and_swap((uint64_t *)c->first[idx], 0, 0);
}

void* channel_get_last(channel *c, uint32_t idx)
{
  assert(idx < c->total);
  return (void *)__sync_val_compare_and_swap((uint64_t *)c->last[idx], 0, 0);
}

worker* new_worker(uint32_t id, uint32_t total, barrier *b)
{
  assert(id < total);

  worker *w = (worker *)malloc(sizeof(worker));
  w->id = id;
  w->total = total;
  w->bar = b;

  // for a 4kb node, 16 bytes key, there will be about 256 keys,
  // if there are 4 workers, 64 should be enough
  // TODO: dynamically change this in account of batch size and worker number
  w->max_path = 64;
  w->cur_path = 0;
  w->beg_path = 0;
  w->tot_path = 0;
  w->paths = (path *)malloc(sizeof(path) * w->max_path);

  // 4 is enough even in extreme situations, so it's not likely
  // more memory will be required
  // TODO: change this to `max_descend_depth`?
  w->max_fence = 4;
  w->cur_fence[0] = 0;
  w->cur_fence[1] = 0;
  w->fences[0] = (fence *)malloc(sizeof(fence) * w->max_fence);
  w->fences[1] = (fence *)malloc(sizeof(fence) * w->max_fence);

  w->prev = 0;
  w->next = 0;

  w->ch = new_channel(max_descend_depth);
  w->their_last  = 0;
  w->my_first    = 0;
  w->my_last     = 0;
  w->their_first = 0;

  return w;
}

void free_worker(worker* w)
{
  free_channel(w->ch);

  free((void *)w->fences[0]);
  free((void *)w->fences[1]);
  free((void *)w->paths);
  free((void *)w);
}

void worker_link(worker *a, worker *b)
{
  a->next = b;
  b->prev = a;
}

void worker_reset(worker *w)
{
  for (uint32_t i = 0; i < w->max_path; ++i)
    path_clear(&w->paths[i]);
  w->cur_path = 0;

  w->cur_fence[0] = 0;
  w->cur_fence[1] = 0;

  channel_reset(w->ch);
}

path* worker_get_new_path(worker *w)
{
  // TODO: optimize memory allocation?
  if (w->cur_path == w->max_path) {
    w->max_path = w->max_path * 2;
    assert(w->paths = (path *)realloc(w->paths, sizeof(path) * w->max_path));
  }
  // TODO: remove this
  assert(w->cur_path < w->max_path);
  return &w->paths[w->cur_path++];
}

void worker_switch_fence(worker *w, uint32_t level)
{
  w->cur_fence[level % 2] = 0;
}

fence* worker_get_new_fence(worker *w, uint32_t level)
{
  uint32_t idx = level % 2;
  // TODO: optimize memory allocation?
  if (w->cur_fence[idx] == w->max_fence) {
    w->max_fence = w->max_fence * 2;
    assert(w->fences[idx] = (fence *)realloc(w->fences[idx], sizeof(fence) * w->max_fence));
  }
  // TODO: remove this
  assert(w->cur_fence[idx] < w->max_fence);
  return &w->fences[idx][w->cur_fence[idx]++];
}

// insert fence info in fence key order for later promotion
// return insert position for later fence info update
uint32_t worker_insert_fence(worker *w, uint32_t level, fence *f)
{
  uint32_t idx = level % 2;
  uint32_t cur = w->cur_fence[idx];
  if (cur == w->max_fence) {
    w->max_fence = w->max_fence * 2;
    assert(w->fences[idx] = (fence *)realloc(w->fences[idx], sizeof(fence) * w->max_fence));
  }
  // TODO: remove this
  assert(cur < w->max_fence);

  // find which index to insert new fence
  uint32_t i = 0;
  fence *fences = w->fences[idx];
  for (; i < cur; ++i)
    if (compare_key(fences[i].key, fences[i].len, f->key, f->len) > 0)
      break;

  // save the space for new fence
  uint32_t l = cur, j = i;
  for (; i < cur; ++i, --l)
    memcpy(&fences[l], &fences[l - 1], sizeof(fence));

  memcpy(&fences[j], f, sizeof(fence));
  ++w->cur_fence[idx];

  return j;
}

void worker_update_fence(worker *w, uint32_t level, fence *f, uint32_t i)
{
  uint32_t idx = level % 2;
  uint32_t cur = w->cur_fence[idx];
  // TODO: remove this
  assert(cur > 0 && i < cur);

  if (i == cur - 1)
    f->ptr = 0;
  else
    memcpy(f, &w->fences[idx][i + 1], sizeof(fence));
}

// loop all the workers to find the fence info for root split, this function should
// only be called by worker 0
// TODO: there is only one root split under any circumstances, but looping all the workers
//       may be an expensive job, especially if this machine supports dozens of threads
void worker_get_fences(worker *w, uint32_t level, fence **fences, uint32_t *number)
{
  assert(w->id == 0);
  uint32_t idx = level % 2;
  worker *next = w->next;
  while (next) {
    for (uint32_t i = 0; i < next->cur_fence[idx]; ++i)
      worker_insert_fence(w, level, &next->fences[idx][i]);
    next = next->next;
  }
  *number = w->cur_fence[idx];
  *fences = w->fences[idx];
}

/**
 *   point to point synchronization pseudo code
 *
 *   sent-first,  sent-last  = false
 *   their-first, their-last = NULL
 *
 *   initialize my-first, my-last
 *
 *   while ¬ ∧ {their-first, their-last, sent-first, sent-last} {
 *       if my-first ∧ ¬sent-first
 *           SEND-FIRST(i − 1, d, my-first)
 *           sent-first = true
 *
 *       if my-last ∧ ¬sent-last
 *           SEND-LAST(i + 1, d, my-last)
 *           sent-last = true
 *
 *       if ¬their-first
 *           their-first = TRY-RECV-FIRST(i + 1, d)
 *
 *       if their-first ∧ ¬my-first
 *           my-first = their-first
 *
 *       if ¬their-last
 *           their-last = TRY-RECV-LAST(i − 1, d)
 *
 *       if their-last ∧ ¬my-last
 *           my-last = their-last
 *   }
 *
 *
 *   node sequence:
 *     their-last  my-first  my-last  their-first
 *   they can be overlapped
**/
// TODO: save more info for later redistribute work
void worker_sync(worker *w, uint32_t level)
{
  int set_first = 0, set_last = 0;
  node *their_first = 0, *their_last = 0;

  node *my_first = 0, *my_last = 0;

  // handle worker boundary case
  if (w->id == 0) {
    set_first = 1;
    their_last = magic_pointer; // as long as it's not zero
  }
  if (w->id == w->total - 1) {
    set_last = 1;
    their_first = magic_pointer; // as long as it's not zero
  }

  // initialize `my` node info
  if (level == 0) { // we are descending to leaf
    if (w->cur_path) {
      my_first = path_get_node_at_level(&w->paths[0], level);
      my_last  = path_get_node_at_level(&w->paths[w->cur_path - 1], level);
    }
  } else { // we are promoting split
    uint32_t idx = (level - 1) % 2;
    if (w->cur_fence[idx]) {
      my_first = path_get_node_at_level(w->fences[idx][0].pth, level);
      my_last  = path_get_node_at_level(w->fences[idx][w->cur_fence[idx] - 1].pth, level);
    }
  }

  // we don't use sched_yield() or functions like that,
  // we just keep each thread busy
  while (!(set_first && set_last && their_first && their_last)) {
    if (my_first && !set_first) {
      channel_set_first(w->prev->ch, level, (void *)my_first); // `w->prev` can't be NULL
      set_first = 1;
    }

    if (my_last && !set_last) {
      channel_set_last(w->next->ch, level, (void *)my_last); // `w->next` can't be NULL
      set_last = 1;
    }

    if (!their_first)
      their_first = (node *)channel_get_first(w->ch, level);

    if (their_first && !my_first)
      my_first = their_first;

    if (!their_last)
      their_last = (node *)channel_get_last(w->ch, level);

    if (their_last && !my_last)
      my_last = their_last;
  }

  // record boundary nodes for later work redistribution
  w->their_last  = their_last;
  w->my_first    = my_first;
  w->my_last     = my_last;
  w->their_first = their_first;
}

void worker_redistribute_work(worker *w, uint32_t level)
{
  if (level == 0) {
    if (w->cur_path == 0) {
      w->tot_path = 0;
      return ;
    }
    if (w->their_last != magic_pointer && w->my_first == w->their_last) {
      w->beg_path = w->cur_path;
      for (uint32_t i = 0; i < w->cur_path; ++i) {
        path *cp = &w->paths[i];
        node *cn = path_get_node_at_level(cp, level);
        if (cn != w->their_last) {
          w->beg_path = i;
          break;
        }
      }
    } else {
      w->beg_path = 0;
    }

    w->tot_path = w->cur_path - w->beg_path;

    if (w->tot_path == 0 || w->their_first == magic_pointer || w->my_last != w->their_first) return ;

    worker *next = w->next;
    while (next && next->cur_path) {
      for (uint32_t i = 0; i < next->cur_path; ++i) {
        path *np = &next->paths[i];
        node *nn = path_get_node_at_level(np, level);
        if (nn != w->my_last)
          return ;
        else
          ++w->tot_path;
      }
      next = next->next;
    }
  } else {
    uint32_t idx = (level - 1) % 2;
    uint32_t cur_fence = w->cur_fence[idx];
    if (cur_fence == 0) {
      w->tot_fence = 0;
      return ;
    }
    if (w->their_last != magic_pointer && w->my_first == w->their_last) {
      w->beg_fence = cur_fence;
      for (uint32_t i = 0; i < cur_fence; ++i) {
        path *cp = w->fences[idx][i].pth;
        node *cn = path_get_node_at_level(cp, level);
        if (cn != w->their_last) {
          w->beg_fence = i;
          break;
        }
      }
    } else {
      w->beg_fence = 0;
    }

    w->tot_fence = cur_fence - w->beg_fence;

    if (w->tot_fence == 0 || w->their_first == magic_pointer || w->my_last != w->their_first) return ;

    worker *next = w->next;
    while (next) {
      uint32_t next_fence = next->cur_fence[idx];
      fence *fences = next->fences[idx];
      for (uint32_t i = 0; i < next_fence; ++i) {
        path *np = fences[i].pth;
        node *nn = path_get_node_at_level(np, level);
        if (nn != w->my_last)
          return ;
        else
          ++w->tot_fence;
      }
      next = next->next;
    }
  }
}

// process keys assigned to this worker in leaf nodes, worker has already obtained the path information
void worker_execute_on_leaf_nodes(worker *w, batch *b)
{
  fence fnc;
  node *pn   = 0; // previous path node
  node *curr = 0; // node actually to process the key

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

    if (cn != pn) {
      curr = cn;
      fnc.ptr = 0; // previous split has no influence on current key
    } else if (fnc.ptr && compare_key(key, len, fnc.key, fnc.len) >= 0) { // equal is possible
      curr = fnc.ptr;
      fnc.ptr = 0;
    }

    if (op == Write) {
      switch (node_insert(curr, key, len, (const void *)*(val_t *)val)) {
        case 1:  // key insert succeed, we set value to 1
          set_val(val, 1);
          break;
        case 0:  // key already inserted, we set value to 0
          set_val(val, 0);
          break;
        case -1: { // node does not have enough space, needs to split
          node *nn = new_node(Leaf, curr->level);
          node_split(curr, nn, fnc.key, &fnc.len);
          fnc.pth = cp;
          fnc.ptr = nn;
          uint32_t idx = worker_insert_fence(w, 0, &fnc);

          // compare current key with fence key to determine which node to insert
          if (compare_key(key, len, fnc.key, fnc.len) > 0) { // equal is not possible
            curr = nn;
            // we need to update fence because the next key may fall into the next split node
            worker_update_fence(w, 0, &fnc, idx);
          }
          assert(node_insert(curr, key, len, (const void *)*(val_t *)val) == 1);
          set_val(val, 1);
          break;
        }
        default:
          assert(0);
      }
    } else { // Read
      set_val(val, (val_t)node_search(curr, key, len));
    }

    pn = cn; // record previous node
  }
}

// this function does exactly the same work as `execute_on_leaf_nodes`,
// but with some critical difference
// TODO: maybe they can be combined?
void worker_execute_on_branch_nodes(worker *w, uint32_t level)
{
  fence fnc;
  node *pn   = 0; // previous path node
  node *curr = 0; // node actually to process the key

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

    if (cn != pn) {
      curr = cn;
      fnc.ptr = 0; // previous split has no influence on current key
    } else if (fnc.ptr && compare_key(key, len, fnc.key, fnc.len) >= 0) {  // equal is possible
      curr = fnc.ptr;
      fnc.ptr = 0;
    }

    switch (node_insert(curr, key, len, val)) {
      case 1:  // key insert succeed
        break;
      case 0:  // key already inserted, it's not possible
        assert(0);
        break;
      case -1: { // node does not have enough space, needs to split
        node *nn = new_node(Branch, curr->level);
        node_split(curr, nn, fnc.key, &fnc.len);
        fnc.pth = cp;
        fnc.ptr = nn;
        uint32_t idx = worker_insert_fence(w, level, &fnc);

        // compare current key with fence key to determine which node to insert
        if (compare_key(key, len, fnc.key, fnc.len) > 0) { // equal is not possible
          curr = nn;
          // we need to update fence because next key may fall into the next split node
          worker_update_fence(w, level, &fnc, idx);
        }
        assert(node_insert(curr, key, len, val) == 1);
        break;
      }
      default:
        assert(0);
    }

    pn = cn; // record previous node
  }
}

void init_path_iter(path_iter *iter, worker *w)
{
  // TODO: remove this
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
  // TODO: remove this
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

#ifdef Test

void worker_print_path_info(worker *w)
{
  printf("worker %u path info\n", w->id);
  path *p;
  path_iter pi;
  init_path_iter(&pi, w);
  while ((p = next_path(&pi))) {
    node* n = path_get_node_at_level(p, 0);
    printf("%u ", n->id);
  }
  printf("\n");
}

void worker_print_fence_info(worker *w, uint32_t level)
{
  printf("worker %u split info at level %u\n", w->id, level);
  printf("myself:%u total:%u\n", w->cur_fence[(level - 1) % 2], w->tot_fence - w->beg_fence);
  fence *f;
  fence_iter fi;
  init_fence_iter(&fi, w, level);
  while ((f = next_fence(&fi))) {
    node *n = path_get_node_at_level(f->pth, level);
    f->key[f->len] = '\0';
    printf("%u %s  parent:%u\n", f->ptr->id, f->key, n->id);
  }
}

#endif
