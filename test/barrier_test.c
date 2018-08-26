/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <stdlib.h>

#include "../src/barrier.h"

static const int threads = 4;
static barrier b;

void* f(void *v)
{
  sleep(1 + (rand() % 5));
  printf("hello world %llu\n", (uint64_t)v);
  barrier_wait(&b);
  return (void *)0;
}

int main()
{
  init_barrier(&b, threads);
  pthread_t ids[threads];
  for (int i = 0; i < threads; ++i) {
    assert(pthread_create(&ids[i], 0, f, (void *)(uint64_t)i) == 0);
  }
  for (int i = 0; i != threads; ++i)
    assert(pthread_join(ids[i], 0) == 0);

  return 0;
}
