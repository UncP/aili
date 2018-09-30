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
  if (total <= 0) total = 1;
  // we don't use a big queue to avoid too much batch memory
  if (total >= 8) total = 8;

  bounded_queue *q = (bounded_queue *)malloc(sizeof(bounded_queue));

  q->total = total;
  q->head  = 0;
  q->tail  = 0;
  q->clear = 0;

  q->array = (void **)calloc(q->total, sizeof(void *));

  assert(pthread_mutex_init(&q->mutex, 0) == 0);
  assert(pthread_cond_init(&q->cond, 0) == 0);

  return q;
}

void free_bounded_queue(bounded_queue *q)
{
  pthread_mutex_destroy(&q->mutex);
  pthread_cond_destroy(&q->cond);

  free((void *)q->array);

  free((void *)q);
}

void bounded_queue_clear(bounded_queue *q)
{
  pthread_mutex_lock(&q->mutex);

  // wait until all the queue elements have been processed
  while (q->array[q->head])
    pthread_cond_wait(&q->cond, &q->mutex);

  q->clear = 1;
  pthread_cond_broadcast(&q->cond);

  pthread_mutex_unlock(&q->mutex);
}

void bounded_queue_enqueue(bounded_queue *q, void *element)
{
  assert(element);

  pthread_mutex_lock(&q->mutex);

  while (q->array[q->tail] && !q->clear)
    pthread_cond_wait(&q->cond, &q->mutex);

  if (!q->clear) {
    q->array[q->tail++] = element;
    if (q->tail == q->total)
      q->tail = 0;
    // wake up all the workers
    pthread_cond_broadcast(&q->cond);
  }

  pthread_mutex_unlock(&q->mutex);
}

// return the element at `idx` but don't proceed `q->head`
void* bounded_queue_get_at(bounded_queue *q, int *idx)
{
  pthread_mutex_lock(&q->mutex);

  while (!q->array[*idx] && !q->clear)
    pthread_cond_wait(&q->cond, &q->mutex);

  void *r;
  if (!q->clear) {
    r = q->array[*idx];
    if (++(*idx) == q->total)
      *idx = 0;
  } else {
    r = 0;
  }

  pthread_mutex_unlock(&q->mutex);
  return r;
}

void bounded_queue_dequeue(bounded_queue *q)
{
  pthread_mutex_lock(&q->mutex);

  assert(q->array[q->head]);

  q->array[q->head] = 0;

  if (++q->head == q->total)
    q->head = 0;

  pthread_cond_signal(&q->cond);

  pthread_mutex_unlock(&q->mutex);
}
