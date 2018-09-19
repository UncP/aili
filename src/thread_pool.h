/**
 *    author:     UncP
 *    date:    2018-09-11
 *    license:    BSD-3
**/

#ifndef _thread_pool_h_
#define _thread_pool_h_

#include <pthread.h>

#include "worker.h"
#include "barrier.h"
#include "bounded_queue.h"
#include "palm_tree.h"

typedef struct thread_pool
{
  int        num;
  int        run;
  pthread_t *ids;

  bounded_queue *queue;

  worker **workers;
  barrier *bar;

}thread_pool;

thread_pool* new_thread_pool(int num, palm_tree *pt, bounded_queue *queue);
void thread_pool_stop(thread_pool *tp);
void free_thread_pool(thread_pool *tp);

#endif /* _thread_pool_h_ */