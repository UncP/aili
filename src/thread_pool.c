/**
 *    author:     UncP
 *    date:    2018-09-11
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>

#include "thread_pool.h"

static void* run(void *args)
{

  while (1) {
  }
}

thread_pool* new_thread_pool(int num)
{
  thread_pool *pt = (thread_pool *)malloc(sizeof(thread_pool));

  if (num <= 0) num = 1;
  if (num >= 4) num = 4;

  pt->num = num;
  pt->run = 1;
  pt->ids = (pthread_t *)malloc(sizeof(pthread_t) * pt->num);

  for (int i = 0; i < pt->num; ++i) {
    assert(pthread_create(&pt->ids[i], 0, run, 0) == 0);

  }
}

void thread_pool_stop(thread_pool *pt)
{
  pthread_mutex_lock(&pt->lock);
  pt->run = 0;
  pthread_mutex_unlock(&pt->lock);
}

void free_thread_pool(thread_pool *tp)
{

}
