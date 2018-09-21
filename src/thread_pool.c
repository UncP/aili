/**
 *    author:     UncP
 *    date:    2018-09-11
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>
// TODO: remove this
#include <stdio.h>

#include "thread_pool.h"

typedef struct thread_arg
{
  palm_tree *pt;
  worker    *wrk;
  bounded_queue *que;
  barrier   *bar;
}thread_arg;

static thread_arg* new_thread_arg(palm_tree *pt, worker *w, bounded_queue *q, barrier *b)
{
  thread_arg *j = (thread_arg *)malloc(sizeof(thread_arg));
  j->pt  = pt;
  j->wrk = w;
  j->que = q;
  j->bar = b;

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
  barrier *bar = j->bar;

  while (1) {
    // TODO: optimization?
    batch *bth = bounded_queue_top(q);

    if (bth)
      palm_tree_execute(pt, bth, w);
    else
      break;

    // let worker 0 do the popping
    if (w->id == 0)
      bounded_queue_pop(q);

    // TODO: optimization?
    // sync here to prevent a worker executing the same batch twice
    if (bar) barrier_wait(bar);
  }

  free_thread_arg(j);
  return 0;
}

thread_pool* new_thread_pool(int num, palm_tree *pt, bounded_queue *queue)
{
  thread_pool *tp = (thread_pool *)malloc(sizeof(thread_pool));

  if (num <= 0) num = 1;

  tp->num = num;
  tp->queue = queue;
  tp->ids = (pthread_t *)malloc(sizeof(pthread_t) * tp->num);
  tp->workers = (worker **)malloc(sizeof(worker *) * tp->num);

  tp->bar = tp->num > 1 ? new_barrier(tp->num) : 0;

  for (int i = 0; i < tp->num; ++i) {
    tp->workers[i] = new_worker(i, tp->num);
    if (i > 0)
      worker_link(tp->workers[i - 1], tp->workers[i]);
  }

  for (int i = 0; i < tp->num; ++i) {
    thread_arg *j = new_thread_arg(pt, tp->workers[i], tp->queue, tp->bar);
    assert(pthread_create(&tp->ids[i], 0, run, (void *)j) == 0);
  }

  return tp;
}

void thread_pool_stop(thread_pool *tp)
{
  bounded_queue_clear(tp->queue);

  // collect all the child threads
  for (int i = 0; i < tp->num; ++i)
    assert(pthread_join(tp->ids[i], 0) == 0);
}

// `thread_pool_stop` must be called before `free_thread_pool`
void free_thread_pool(thread_pool *tp)
{
  for (int i = 0; i < tp->num; ++i)
    free_worker(tp->workers[i]);

  free((void *)tp->bar);
  free((void *)tp->workers);
  free((void *)tp->ids);
  free((void *)tp);
}
