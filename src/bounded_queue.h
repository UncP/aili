/**
 *    author:     UncP
 *    date:    2018-09-13
 *    license:    BSD-3
**/

#ifndef _bounded_queue_h_
#define _bounded_queue_h_

#include <pthread.h>

// a bounded queue designed for palm tree
typedef struct bounded_queue
{
  int total;
  int head;
  int tail;
  int clear;

  void **array;

  pthread_mutex_t mutex;
  pthread_cond_t  cond;
}bounded_queue;

bounded_queue* new_bounded_queue(int total);
void free_bounded_queue(bounded_queue *q);

#endif /* _bounded_queue_h_ */