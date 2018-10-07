/**
 *    author:     UncP
 *    date:    2018-09-13
 *    license:    BSD-3
**/

#ifndef _bounded_queue_h_
#define _bounded_queue_h_

#include <pthread.h>

typedef struct bounded_queue
{
  int total;
  int head; // point to the slot which to enqueue
  int tail; // point to the slot which to dequeue
  int size;
  int clear;

  void **array;

  pthread_mutex_t mutex;

  pthread_cond_t  cond;
}bounded_queue;

bounded_queue* new_bounded_queue(int total);
void free_bounded_queue(bounded_queue *q);
void bounded_queue_wait_empty(bounded_queue *q);
void bounded_queue_clear(bounded_queue *q);
void bounded_queue_enqueue(bounded_queue *q, void *element);
void* bounded_queue_get_at(bounded_queue *q, int *idx);
void bounded_queue_dequeue(bounded_queue *q);

#endif /* _bounded_queue_h_ */