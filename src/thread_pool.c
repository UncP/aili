/**
 *    author:     UncP
 *    date:    2018-09-11
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>

#include "thread_pool.h"

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

  while (1) {
    batch *b = bounded_queue_top(q);

    if (b)
      palm_tree_execute(pt, b, w);
    else
      break;

    if (w->id == 0)
      bounded_queue_pop(q);
  }

  free_thread_arg(j);
  return 0;
}

thread_pool* new_thread_pool(int num, palm_tree *pt, bounded_queue *queue)
{
  thread_pool *tp = (thread_pool *)malloc(sizeof(thread_pool));

  if (num <= 0) num = 1;
  // before having a machine that has more cores, 4 is enough for my mbp 13'
  if (num >= 4) num = 4;

  tp->num = num;
  tp->queue = queue;
  tp->pt = pt;
  tp->ids = (pthread_t *)malloc(sizeof(pthread_t) * tp->num);
  tp->workers = (worker **)malloc(sizeof(worker *) * tp->num);

  tp->bar = tp->num > 1 ? new_barrier(tp->num) : 0;

  for (int i = 0; i < tp->num; ++i) {
    tp->workers[i] = new_worker(i, tp->num, tp->bar);

    thread_arg *j = new_thread_arg(tp->pt, tp->workers[i], tp->queue);

    assert(pthread_create(&tp->ids[i], 0, run, (void *)j) == 0);

    if (i > 0)
      worker_link(tp->workers[i - 1], tp->workers[i]);
  }

  return tp;
}

void thread_pool_stop(thread_pool *tp)
{
  bounded_queue_clear(tp->queue);
}

// `thread_pool_stop` must be called before `free_thread_pool`
void free_thread_pool(thread_pool *tp)
{
  for (int i = 0; i < tp->num; ++i)
    assert(pthread_join(tp->ids[i], 0) == 0);

  for (int i = 0; i < tp->num; ++i)
    free_worker(tp->workers[i]);

  free((void *)tp->bar);
  free((void *)tp->workers);
  free((void *)tp->ids);
  free((void *)tp);
}
