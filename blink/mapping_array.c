/**
 *    author:     UncP
 *    date:    2018-11-21
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>
// TODO: remove this
#include <stdio.h>

#include "mapping_array.h"

// find first set bit, ported from linux kernal, `word` must not be 0UL
static inline unsigned long ffs_(unsigned long word)
{
  __asm__ ("rep; bsf %1,%0"
    : "=r" (word)
    : "rm" (word));
  return word;
}

mapping_array* new_mapping_array(size_t element_bytes)
{
  mapping_array *q = (mapping_array *)malloc(sizeof(mapping_array));

  q->clear = 0;
  q->free  = (uint64_t)-1;
  q->busy  = 0;

  size_t aligned_element_bytes = (element_bytes + 63) & (~((size_t)63));
  void *ptr = valloc(max_array_size * aligned_element_bytes);
  for (size_t i = 0; i < max_array_size; ++i)
    q->elements[i] = (void *)((char *)ptr + i * aligned_element_bytes);

  pthread_mutex_init(q->mutex, 0);
  pthread_cond_init(q->free_cond, 0);
  pthread_cond_init(q->busy_cond, 0);

  return q;
}

void free_mapping_array(mapping_array *q)
{
	mapping_array_clear(q);

  pthread_mutex_destroy(q->mutex);
  pthread_cond_destroy(q->free_cond);
  pthread_cond_destroy(q->busy_cond);

  free((void *)q);
}

void mapping_array_clear(mapping_array *q)
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
  pthread_cond_broadcast(q->busy_cond);
}

void mapping_array_wait_empty(mapping_array *q)
{
  pthread_mutex_lock(q->mutex);

  assert(q->clear == 0);

  while (q->free != (uint64_t)-1 || q->busy != 0)
    pthread_cond_wait(q->free_cond, q->mutex);

  pthread_mutex_unlock(q->mutex);
}

void* mapping_array_get_free(mapping_array *q, int *idx)
{
  pthread_mutex_lock(q->mutex);

  assert(q->clear == 0);

  while (q->free == 0)
  	pthread_cond_wait(q->free_cond, q->mutex);

  *idx = ffs_(q->free);

  return q->elements[*idx];
}

void mapping_array_put_free(mapping_array *q, int idx)
{
  assert(idx >= 0 && idx <= 63);
  uint64_t mask = ((uint64_t)1) << idx;
  q->busy |= mask;
  q->free &= ~mask;

  pthread_mutex_unlock(q->mutex);
  pthread_cond_signal(q->busy_cond);
}

void* mapping_array_get_busy(mapping_array *q, int *idx)
{
  pthread_mutex_lock(q->mutex);

  while (q->busy == 0 && q->clear == 0)
    pthread_cond_wait(q->busy_cond, q->mutex);

  if (q->clear) {
    pthread_mutex_unlock(q->mutex);
    *idx = -1;
    return (void *)0;
  }

  *idx = ffs_(q->busy);

  q->busy &= ~(((uint64_t)1) << *idx);

  pthread_mutex_unlock(q->mutex);
  return q->elements[*idx];
}

void mapping_array_put_busy(mapping_array *q, int idx)
{
  assert(idx >= 0 && idx <= 63);

  pthread_mutex_lock(q->mutex);

  assert(q->clear == 0);

  q->free |= ((uint64_t)1) << idx;

  pthread_mutex_unlock(q->mutex);
  pthread_cond_signal(q->free_cond);
}
