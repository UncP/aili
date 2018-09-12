/**
 *    author:     UncP
 *    date:    2018-09-11
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>

#include "thread_pool.h"

typedef struct job
{
  palm_tree *pt;
  worker    *w;
}

static job* new_job(palm_tree *pt, worker *w)
{
  job *j = (job *)malloc(sizeof(job));
  j->pt = pt;
  j->w = w;

  return j;
}

static void free_job(job *j)
{
  free((void *)j);
}

static void* run(void *arg)
{
  job *j = (job *)arg;
  palm_tree *pt = j->pt;
  worker    *w  = j->w;

  while (1) {

  }

  free_job(j);
}

thread_pool* new_thread_pool(int num, palm_tree *pt)
{
  thread_pool *tp = (thread_pool *)malloc(sizeof(thread_pool));

  if (num <= 0) num = 1;
  if (num >= 4) num = 4;

  tp->num = num;
  tp->run = 1;
  tp->ids = (pthread_t *)malloc(sizeof(pthread_t) * tp->num);
  tp->workers = (worker **)malloc(sizeof(worker *) * tp->num);

  barrier *bar = tp->num > 1 ? new_barrier(tp->num) : 0;

  for (int i = 0; i < tp->num; ++i) {
    tp->workers[i] = new_worker(i, tp->num, bar);

    job *j = new_job(pt, tp->workers[i]);

    assert(pthread_create(&tp->ids[i], 0, run, (void *)j) == 0);

    if (i > 0)
      worker_link(tp->workers[i - 1], tp->workers[i]);
  }

  return tp;
}

void thread_pool_stop(thread_pool *tp)
{
  pthread_mutex_lock(&tp->lock);
  tp->run = 0;
  pthread_mutex_unlock(&tp->lock);
}

// `thread_pool_stop` must be called before `free_thread_pool`
void free_thread_pool(thread_pool *tp)
{
  for (int i = 0; i < tp->num; ++i)
    free_worker(tp->workers[i]);

  free((void *)tp->workers);
  free((void *)tp->ids);
  free((void *)tp);
}
