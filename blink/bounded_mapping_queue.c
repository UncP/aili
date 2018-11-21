/**
 *    author:     UncP
 *    date:    2018-11-21
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>

#include "bounded_mapping_queue.h"

// find first zero bit, ported from linux kernal, `word` must not be ~0UL
static inline unsigned long ffz(unsigned long word)
{
  __asm__ ("rep; bsf %1,%0"
    : "=r" (word)
    : "r" (~word));
  return word;
}

// find first set bit, ported from linux kernal, `word` must not be 0UL
static inline unsigned long ffs(unsigned long word)
{
  __asm__ ("rep; bsf %1,%0"
    : "=r" (word)
    : "rm" (word));
  return word;
}

bounded_mapping_queue* new_bounded_mapping_queue(size_t element_bytes)
{
  bounded_mapping_queue *q = (bounded_mapping_queue *)malloc(sizeof(bounded_mapping_queue));

  q->clear = 0;
  q->free  = (uint64_t)-1;
  q->busy  = 0;

  size_t aligned_element_bytes = (element_bytes + 63) & (~((size_t)63));
  void *ptr = valloc(max_queue_size * aligned_element_bytes);
  for (size_t i = 0; i < max_queue_size; ++i)
    q->elements[i] = (void *)((char *)ptr + i * aligned_element_bytes);

  pthread_mutex_init(q->mutex, 0);
  pthread_cond_init(q->free_cond, 0);
  pthread_cond_init(q->busy_cond, 0);

  return q;
}

void free_bounded_mapping_queue(bounded_mapping_queue *q)
{
	bounded_mapping_queue_clear(q);

  pthread_mutex_destroy(q->mutex);
  pthread_cond_destroy(q->free_cond);
  pthread_cond_destroy(q->busy_cond);

  free((void *)q);
}

void bounded_mapping_queue_clear(bounded_mapping_queue *q)
{
  pthread_mutex_lock(q->mutex);

  if (q->clear) {
    pthread_mutex_unlock(q->mutex);
    return ;
  }

  while (q->free != (uint64_t)-1 || q->busy != 0)
    pthread_cond_wait(q->free_cond, q->mutex);

  q->clear = 1;

  pthread_mutex_unlock(q->mutex);
}

void bounded_mapping_queue_wait_empty(bounded_mapping_queue *q)
{
  pthread_mutex_lock(q->mutex);

  assert(q->clear == 0);

  while (q->free != (uint64_t)-1 || q->busy != 0)
    pthread_cond_wait(q->free_cond, q->mutex);

  pthread_mutex_unlock(q->mutex);
}

void* bounded_mapping_queue_get_free(bounded_mapping_queue *q, int *idx)
{
  pthread_mutex_lock(q->mutex);

  assert(q->clear == 0);

  while (q->free == 0)
  	pthread_cond_wait(q->free_cond, q->mutex);

  *idx = ffz(q->free);

  return q->elements[*idx];
}

void bounded_mapping_queue_put_free(bounded_mapping_queue *q, int idx)
{
  assert(idx >= 0 && idx <= 63);
  uint64_t mask = ((uint64_t)1) << idx;
  q->busy |= mask;
  q->free &= ~mask;

  pthread_mutex_unlock(q->mutex);
  pthread_cond_signal(q->busy_cond);
}

void* bounded_mapping_queue_get_busy(bounded_mapping_queue *q, int *idx)
{
  pthread_mutex_lock(q->mutex);

  assert(q->clear == 0);

  while (q->busy == 0 && q->clear == 0)
    pthread_cond_wait(q->busy_cond, q->mutex);

  if (q->clear) {
    pthread_mutex_unlock(q->mutex);
    *idx = -1;
    return (void *)0;
  }

  *idx = ffs(q->busy);

  q->busy &= ~(((uint64_t)1) << *idx);

  return q->elements[*idx];
}

void bounded_mapping_queue_put_busy(bounded_mapping_queue *q, int idx)
{
  assert(idx >= 0 && idx <= 63);

  pthread_mutex_lock(q->mutex);

  assert(q->clear == 0);

  q->free |= ((uint64_t)1) << idx;

  pthread_mutex_unlock(q->mutex);
  pthread_cond_signal(q->free_cond);
}
