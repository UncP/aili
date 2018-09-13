/**
 *    author:     UncP
 *    date:    2018-09-13
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>

#include "bounded_queue.h"

bounded_queue* new_bounded_queue(int total)
{
  if (total <= 0) total = 4;
  // we don't use a big queue to avoid too much batch memory
  if (total >= 8) total = 8;

  bounded_queue *q = (bounded_queue *)malloc(sizeof(bounded_queue));

  q->total = total;
  q->head = 0;
  q->tail = 0;
  q->clear = 0;

  q->array = (void **)malloc(sizeof(void *) * q->total);

  assert(pthread_mutex_init(&q->mutex, 0) == 0);
  assert(pthread_cond_init(&q->cond, 0) == 0);

  return q;
}

void bounded_queue_clear(bounded_queue *q)
{
  pthread_mutex_lock(&q->mutex);

  q->clear = 1;

  // wait until all the queue elements have been processed
  while (q->head != q->tail)
    pthread_cond_wait(&q->cond, &q->mutex);

  pthread_mutex_unlock(&q->mutex);
}

void free_bounded_queue(bounded_queue *q)
{
  pthread_mutex_destroy(&q->mutex);
  pthread_mutex_destroy(&q->cond);

  free((void *)q->array);

  free((void *)q);
}

void bounded_queue_push(bounded_queue *q, void *element)
{
  pthread_mutex_lock(&q->mutex);
  while (((q->tail + 1) % q->total) == q->head)
    pthread_cond_wait(&q->cond, &q->mutex);

  q->array[q->tail++] = element;
  if (q->tail == q->total)
    q->tail = 0;

  // wake up all the workers
  pthread_cond_broadcast(&q->cond);

  pthread_mutex_unlock(&q->mutex);
}

// return the element but don't proceed `q->head`
void* bounded_queue_top(bounded_queue *q)
{
  pthread_mutex_lock(&q->mutex);

  while (q->head == q->tail && !q->clear)
    pthread_cond_wait(&q->cond, &q->mutex);

  void *r;
  if (!q->clear)
  	r = q->array[q->head];
  else
  	r = 0;

  pthread_mutex_unlock(&q->mutex);
  return r;
}

void bounded_queue_pop(bounded_queue *q)
{
  pthread_mutex_lock(&q->mutex);

  if (++q->head == q->total)
  	q->head = 0;

  pthread_cond_signal(&q->cond);

  pthread_mutex_unlock(&q->mutex);
}
