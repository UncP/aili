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

#include "../palm/palm_tree.h"
#include "../mass/mass_tree.h"
#include "../art/art.h"
#include "../util/rng.h"
#ifdef Allocator
#include "../palm/allocator.h"
#endif

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
    palm_tree *pt;
    mass_tree *mt;
    adaptive_radix_tree *art;
  }tree;
  int  keys;
  int  is_put;
  union {
    void (*palm_execute)(palm_tree *pt, batch *b);
    int (*mass_put)(mass_tree *mt, const void *key, uint32_t len, const void *val);
    int (*art_put)(adaptive_radix_tree *art, const void *key, size_t len, const void *val);
  }put_func;
  union {
    void  (*palm_execute)(palm_tree *pt, batch *b);
    void* (*mass_get)(mass_tree *mt, const void *key, uint32_t len);
    void* (*art_get)(adaptive_radix_tree *art, const void *key, size_t len);
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
    case PALM: {
      batch *batches[8 /* queue_size */ + 1];
      for (int i = 0; i < 9; ++i)
        batches[i] = new_batch();
      int idx = 0;
      batch *cb = batches[idx];
      for (int i = 0; i < keys; ++i) {
        uint64_t key = rng_next(&r);
        if (batch_add_write(cb, &key, 8, (void *)3190) == -1) {
          (*(ta->put_func.palm_execute))(ta->tree.pt, cb);
          idx = idx == 8 ? 0 : idx + 1;
          cb = batches[idx];
          batch_clear(cb);
          assert(batch_add_write(cb, &key, 8, (void *)3190) == 1);
        }
      }

      // finish remained work
      (*(ta->put_func.palm_execute))(ta->tree.pt, cb);
      palm_tree_flush(ta->tree.pt);

      for (int i = 0; i < 9; ++i)
        free_batch(batches[i]);
    }
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
    case PALM: {
      batch *batches[8 /* queue_size */ + 1];
      for (int i = 0; i < 9; ++i)
        batches[i] = new_batch();
      int idx = 0;
      batch *cb = batches[idx];
      for (int i = 0; i < keys; ++i) {
        uint64_t key = rng_next(&r);
        if (batch_add_read(cb, &key, 8) == -1) {
          (*(ta->put_func.palm_execute))(ta->tree.pt, cb);
          idx = idx == 8 ? 0 : idx + 1;
          cb = batches[idx];
          for (uint32_t j = 0; j < cb->keys; ++j)
            assert((uint64_t)batch_get_value_at(cb, j) == 3190);
          batch_clear(cb);
          assert(batch_add_read(cb, &key, 8) == 1);
        }
      }

      // finish remained work
      (*(ta->put_func.palm_execute))(ta->tree.pt, cb);
      palm_tree_flush(ta->tree.pt);
      for (int i = 0; i < 9; ++i) {
        cb = batches[i];
        for (uint32_t j = 0; j < cb->keys; ++j)
          assert((uint64_t)batch_get_value_at(cb, j) == 3190);
      }
      for (int i = 0; i < 9; ++i)
        free_batch(batches[i]);
    }
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
          unsigned char *n = (unsigned char *)&key;
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
    ta.tree.pt = new_palm_tree(thread_number, 8 /* queue_size */);
    ta.put_func.palm_execute = &palm_tree_execute;
    ta.get_func.palm_execute = &palm_tree_execute;
    ta.keys *= thread_number;
    thread_number = 1;
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
  printf("-- write start --\n");

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

  printf("-- read start --\n");

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
    printf("tree_type(palm|blink|mass|art) thread_num thread_key_num\n");
    return 0;
  }

#ifdef Allocator
  init_allocator();
#endif

  int thread_num = atoi(argv[2]);
  if (thread_num < 1) thread_num = 1;
  int thread_key_num = atoi(argv[3]);
  if (thread_key_num < 1) thread_key_num = 1;

  benchfuck(tp, thread_num, thread_key_num);

  return 0;
}
