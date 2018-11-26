/**
 *    author:     UncP
 *    date:    2018-11-23
 *    license:    BSD-3
**/

#ifdef Allocator

#include <stdlib.h>
#include <assert.h>
#include <sys/mman.h>
#include <pthread.h>

#include "allocator.h"

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))

static pthread_key_t key;
static int initialized = 0;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static inline void* block_alloc(block *b, size_t size, int *success)
{
  if (likely((b->now + size) <= b->tot)) {
    *success = 1;
    void *ptr = (char *)b->buffer + b->now;
    b->now += size;
    return ptr;
  } else {
    *success = 0;
    return 0;
  }
}

static inline block* new_block(block *meta)
{
  size_t s = (sizeof(block) + 63) & (~((size_t)63));
  int success;
  block *b = block_alloc(meta, s, &success);
  if (likely(success)) {
    b->buffer = mmap(0, block_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    assert(b->buffer != MAP_FAILED);
    b->now = 0;
    b->tot = block_size;
    b->next = 0;
    return b;
  }
  return 0;
}

static inline block* new_meta_block()
{
  char *buf = (char *)mmap(0, meta_block_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
  assert(buf != MAP_FAILED);
  block *m = (block *)buf;
  m->buffer = (void *)buf;
  m->now = (sizeof(block) + 63) & (~((size_t)63));
  m->tot = meta_block_size;
  m->next = 0;
  return m;
}

static void free_block(block *b)
{
  munmap(b->buffer, b->tot);
}

void destroy_allocator(void *arg)
{
  allocator *a = (allocator *)arg;

  block *curr = a->curr;
  while (curr) {
    block *next = curr->next;
    free_block(curr);
    curr = next;
  }

  curr = a->meta_curr;
  while (curr) {
    block *next = curr->next;
    free_block(curr);
    curr = next;
  }

  free((void *)a);
}

void init_allocator()
{
  pthread_mutex_lock(&mutex);
  if (initialized == 0) {
    assert(pthread_key_create(&key, destroy_allocator) == 0);
    initialized = 1;
  }
  pthread_mutex_unlock(&mutex);

  allocator *a;
  assert(posix_memalign((void **)&a, 64, sizeof(allocator)) == 0);
  block *meta = new_meta_block();
  a->meta_curr = meta;

  block *curr = new_block(a->meta_curr);
  assert(curr);
  a->curr = curr;

  assert(pthread_setspecific(key, (void *)a) == 0);
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
    block *new = new_block(a->meta_curr);
    if (new == 0) {
      block *meta = new_meta_block();
      meta->next = a->meta_curr;
      a->meta_curr = meta;
      new = new_block(a->meta_curr);
      assert(new);
    }
    new->next = a->curr;
    a->curr = new;
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
