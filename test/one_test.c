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

#ifdef Allocator
#include "../palm/allocator.h"
#endif
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

typedef enum tree_type
{
  NONE  = 0,
  PALM  = 1,
  BLINK = 2,
  MASS  = 3,
  ART   = 4,
}tree_type;

struct thread_arg
{
  tree_type tp;
  int id;
  int total;
  union {
    mass_tree *mt;
    adaptive_radix_tree *art;
  }tree;
  int  keys;
  int  is_put;
  union {
    int (*mass_put)(mass_tree *tree, const void *key, uint32_t len, const void *val);
    int (*art_put)(adaptive_radix_tree *tree, const void *key, size_t len, const void *val);
  }put_func;
  union {
    void* (*mass_get)(mass_tree *tree, const void *key, uint32_t len);
    void* (*art_get)(adaptive_radix_tree *tree, const void *key, size_t len);
  }get_func;
};

static void* run(void *arg)
{
  void* (*alloc)(size_t);
  #ifdef Allocator
    alloc = &allocator_alloc_small;
  #else
    alloc = &malloc;
  #endif // Allocator
  struct thread_arg *ta = (struct thread_arg *)arg;
  int keys = ta->keys;

  rng r;
  rng_init(&r, ta->id, ta->id + ta->total);

  long long before = mstime();

  if (ta->is_put) {
    switch (ta->tp) {
    case PALM:
    break;
    case BLINK:
    break;
    case MASS: {
      for (int i = 0; i < keys; ++i) {
        uint64_t *key = (*alloc)(8);
        *key = rng_next(&r);
        (*(ta->put_func.mass_put))(ta->tree.mt, key, 8, 0);
      }
    }
    break;
    case ART: {
      for (int i = 0; i < keys; ++i) {
        char *key = (*alloc)(16);
        key[0] = 8;
        *(uint64_t *)(key + 1)= rng_next(&r);
        (*(ta->put_func.art_put))(ta->tree.art, (const void *)(key + 1), 8, 0);
      }
    }
    break;
    default:
      assert(0);
    }
  } else {
    switch (ta->tp) {
    case PALM:
    break;
    case BLINK:
    break;
    case MASS: {
      for (int i = 0; i < keys; ++i) {
        uint64_t key = rng_next(&r);
        void *val = (*(ta->get_func.mass_get))(ta->tree.mt, &key, 8);
        assert(val);
      }
    }
    break;
    case ART: {
      for (int i = 0; i < keys; ++i) {
        uint64_t key = rng_next(&r);
        void *val = (*(ta->get_func.art_get))(ta->tree.art, &key, 8);
        if (val == 0) {
          unsigned char *n = (unsigned char *)key;
          for (int i = 0; i < 8; ++i) {
            printf("%d ", n[i]);
          }
          printf("\n");
        }
        assert(val);
      }
    }
    break;
    default:
      assert(0);
    }
  }

  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mtime: %.4f  s\033[0m\n", keys, (float)(after - before) / 1000);

  return (void *)ta;
}

void benchfuck(tree_type tp, int thread_number, int thread_key_num)
{
  struct thread_arg ta;
  ta.tp = tp;
  ta.total = thread_number;
  ta.keys = thread_key_num;
  if (tp == PALM) {
  }
  if (tp == BLINK) {
  }
  if (tp == MASS) {
    ta.tree.mt = new_mass_tree();
    ta.put_func.mass_put = &mass_tree_put;
    ta.get_func.mass_get = &mass_tree_get;
  }
  if (tp == ART) {
    ta.tree.art = new_adaptive_radix_tree();
    ta.put_func.art_put = &adaptive_radix_tree_put;
    ta.get_func.art_get = &adaptive_radix_tree_get;
  }

  //unsigned int seed = time(0);
  //printf("seed:%u\n", seed);
  //srand(seed);

  pthread_t ids[thread_number];
  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *t = malloc(sizeof(struct thread_arg));
    *t = ta;
    t->is_put = 1;
    t->id = i + 1;
    assert(pthread_create(&ids[i], 0, run, (void *)t) == 0);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *t;
    assert(pthread_join(ids[i], (void **)&t) == 0);
    free(t);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *t = malloc(sizeof(struct thread_arg));
    *t = ta;
    t->is_put = 0;
    t->id = i + 1;
    assert(pthread_create(&ids[i], 0, run, (void *)t) == 0);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *t;
    assert(pthread_join(ids[i], (void **)&t) == 0);
    free(t);
  }
}

int main(int argc, char **argv)
{
  if (argc < 4) {
    printf("tree_type thread_num thread_key_num\n");
    return 0;
  }

  tree_type tp = NONE;
  if (strcasecmp(argv[1], "palm") == 0)
    tp = PALM;
  if (strcasecmp(argv[1], "blink") == 0)
    tp = BLINK;
  if (strcasecmp(argv[1], "mass") == 0)
    tp = MASS;
  if (strcasecmp(argv[1], "art") == 0)
    tp = ART;

  if (tp == NONE) {
    printf("tree_type thread_num thread_key_num\n");
    return 0;
  }

  int thread_num = atoi(argv[2]);
  if (thread_num < 1) thread_num = 1;
  int thread_key_num = atoi(argv[3]);
  if (thread_key_num < 1) thread_key_num = 1;

  benchfuck(tp, thread_num, thread_key_num);

  return 0;
}
