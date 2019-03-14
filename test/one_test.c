/**
 *    author:     UncP
 *    date:    2019-03-14
 *    license:    BSD-3
**/

/* one benchfuck to rule them all! */

#include <stdio.h>
#include <strings.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <sys/time.h>

#include "../util/rng.h"
#include "../mass/mass_tree.h"
#include "../art/art.h"

static long long mstime()
{
  struct timeval tv;
  long long ust;

  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec)*1000000;
  ust += tv.tv_usec;
  return ust / 1000;
}

struct thread_arg
{
  void *tree;
  int  keys;
  int  is_put;
};

typedef enum tree_type
{
  PALM  = 0,
  BLINK = 1,
  MASS  = 2,
  ART   = 3,
}tree_type;

static void* run(void *arg)
{
  struct thread_arg *ta = (struct thread_arg *)arg;
  void *tree = ta->tree;
  int keys = ta->keys;
  int is_put = ta->is_put;

  rng r;
  rng_init(&r, rand(), rand());

  long long before = mstime();

  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mtime: %.4f  s\033[0m\n", keys, (float)(after - before) / 1000);

  return (void *)ta;
}

void benchfuck(tree_type tp, int thread_number, int thread_key_num)
{
  void *tree;
  if (tp == PALM)
    ;
  if (tp == BLINK)
    ;
  if (tp == MASS)
    tree = (void *)new_mass_tree();
  if (tp == ART)
    tree = (void *)new_adaptive_radix_tree();

  srand(time(0));

  pthread_t ids[thread_number];
  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta = malloc(sizeof(struct thread_arg));
    ta->tree = tree;
    ta->keys = thread_key_num;
    ta->is_put = 1;
    assert(pthread_create(&ids[i], 0, run, (void *)ta) == 0);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta;
    assert(pthread_join(ids[i], (void **)&ta) == 0);
    free(ta);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta = malloc(sizeof(struct thread_arg));
    ta->tree = tree;
    ta->keys = thread_key_num;
    ta->is_put = 0;
    assert(pthread_create(&ids[i], 0, run, (void *)ta) == 0);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta;
    assert(pthread_join(ids[i], (void **)&ta) == 0);
    free(ta);
  }
}

int main(int argc, char **argv)
{
  if (argc < 4) {
    printf("tree_type thread_num thread_key_num\n");
    return 0;
  }

  tree_type tp;
  if (strcasecmp(argv[1], "palm") == 0)
    tp = PALM;
  if (strcasecmp(argv[1], "blink") == 0)
    tp = BLINK;
  if (strcasecmp(argv[1], "mass") == 0)
    tp = MASS;
  if (strcasecmp(argv[1], "art") == 0)
    tp = ART;

  int thread_num = atoi(argv[2]);
  if (thread_num < 1) thread_num = 1;
  int thread_key_num = atoi(argv[3]);
  if (thread_key_num < 1) thread_key_num = 1;

  benchfuck(tp, thread_num, thread_key_num);

  return 0;
}
