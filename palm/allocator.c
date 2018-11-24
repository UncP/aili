/**
 *    author:     UncP
 *    date:    2018-11-23
 *    license:    BSD-3
**/

#ifdef Allocator

#include <stdlib.h>
#include <assert.h>
// TODO: remove this
#include <stdio.h>
#include <pthread.h>

#include "allocator.h"

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))

static pthread_key_t key;
static int initialized = 0;

static inline block* new_block()
{
  block *b = (block *)malloc(sizeof(block));
  b->buffer = malloc(block_size);
  b->now = 0;
  b->next = 0;
  return b;
}

static void free_block(block *b)
{
  free(b->buffer);
  free((void *)b);
}

static inline void* block_alloc(block *b, size_t size, int *success)
{
  if (likely((b->now + size) <= block_size)) {
    *success = 1;
    void *ptr = (char *)b->buffer + b->now;
    b->now += size;
    return ptr;
  } else {
    *success = 0;
    return 0;
  }
}

void destroy_allocator(void *arg)
{
  allocator *a = (allocator *)arg;

  while (a->head) {
    block *head = a->head->next;
    free_block(a->head);
    a->head = head;
  }

  free((void *)a);
}

void init_allocator()
{
  if (initialized == 0) {
    assert(pthread_key_create(&key, destroy_allocator) == 0);
    initialized = 1;
  }

  allocator *a = (allocator *)malloc(sizeof(allocator));
  block *head = new_block();
  a->head = head;
  a->curr = head;

  assert(pthread_setspecific(key, (void *)a) == 0);

#ifdef __linux__
  void *ptr;
  pthread_getspecific(key, &ptr);
  assert(ptr == (void *)a);
#else
  assert(pthread_getspecific(key) == (void *)a);
#endif
}

void* allocator_alloc(size_t size)
{
  allocator *a;

#ifdef __linux__
  pthread_getspecific(key, &a);
  assert(a);
#else
  assert((a = pthread_getspecific(key)));
#endif

  int success;
  void *ptr = block_alloc(a->curr, size, &success);
  if (unlikely(success == 0)) {
    a->curr = new_block();
    block *head = a->head;
    a->head = a->curr;
    a->head->next = head;
    ptr = block_alloc(a->curr, size, &success);
  }
  assert(success);
  return ptr;
}

void allocator_free(void *ptr)
{
  // do nothing
  (void)ptr;
}

#endif /* Allocator */
