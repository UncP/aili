/**
 *    author:     UncP
 *    date:    2018-10-07
 *    license:    BSD-3
**/

#include <stdio.h>
#include <assert.h>
#include <sys/time.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#include "../mass/node.h"

#define magic_number 3190

static long long mstime()
{
  struct timeval tv;
  long long ust;

  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec)*1000000;
  ust += tv.tv_usec;
  return ust / 1000;
}

void test_node_marco()
{
  printf("test node marco\n");

  node* n1 = new_node(Border);
  assert(is_border(n1->version));

  assert(!is_locked(n1->version));
  n1->version = set_lock(n1->version);
  assert(is_locked(n1->version));
  n1->version = unset_lock(n1->version);
  assert(!is_locked(n1->version));

  assert(!is_inserting(n1->version));
  n1->version = set_insert(n1->version);
  assert(is_inserting(n1->version));
  n1->version = unset_insert(n1->version);
  assert(!is_inserting(n1->version));

  assert(!is_spliting(n1->version));
  n1->version = set_split(n1->version);
  assert(is_spliting(n1->version));
  n1->version = unset_split(n1->version);
  assert(!is_spliting(n1->version));

  assert(!is_root(n1->version));
  n1->version = set_root(n1->version);
  assert(is_root(n1->version));
  n1->version = unset_root(n1->version);
  assert(!is_root(n1->version));

  assert(!is_deleted(n1->version));
  n1->version = set_delete(n1->version);
  assert(is_deleted(n1->version));

  node *n2 = new_node(Interior);
  assert(is_interior(n2->version));

  assert(get_vinsert(n2->version) == 0);
  for (int i = 0; i < 255; ++i) {
    n2->version = incr_vinsert(n2->version);
    assert((int)get_vinsert(n2->version) == (i+1));
  }
  n2->version = incr_vinsert(n2->version);
  assert(n2->version == 0);

  assert(get_vsplit(n2->version) == 0);
  for (int i = 0; i < 65535; ++i) {
    n2->version = incr_vsplit(n2->version);
    assert((int)get_vsplit(n2->version) == (i+1));
  }
  n2->version = incr_vsplit(n2->version);
  assert(n2->version == 0);

  free_node(n1);
  free_node(n2);
}

void test_node_utility_functions()
{
  printf("test node utility functions\n");

  node *n = new_node(Border);

  node_set_root(n);
  node_unset_root(n);

  node_set_version(n, (uint32_t)magic_number);
  assert(node_get_version(n) == magic_number);

  node_set_parent(n, (node *)magic_number);
  assert((int)node_get_parent(n) == magic_number);

  assert(node_get_next(n) == 0);

  free_node(n);
}

struct node_arg
{
  node *n;
  int milliseconds;
  int *beg;
  int *end;
  int idx;
  int time;
  pthread_mutex_t *mutex;
};

static void* _run(void *arg)
{
  struct node_arg *na = (struct node_arg *)arg;

  long long now = mstime();
  while ((mstime() - now) < na->milliseconds) {
    node_lock(na->n);
    // pthread_mutex_lock(na->mutex);
    *na->beg += na->idx;
    *na->end += na->idx;
    ++na->time;
    // pthread_mutex_unlock(na->mutex);
    node_unlock(na->n);
  }
  return 0;
}

void test_node_lock()
{
  printf("test node lock\n");

  node *n = new_node(Border);

  int threads = 4, beg = 0, end = 0, milliseconds = 3000;
  pthread_t ids[threads];
  struct node_arg *args[threads];
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, 0);
  for (int i = 0; i < threads; ++i) {
    args[i] = (struct node_arg *)malloc(sizeof(struct node_arg));
    args[i]->n = n;
    args[i]->milliseconds = milliseconds;
    args[i]->beg = &beg;
    args[i]->end = &end;
    args[i]->idx = i + 1;
    args[i]->time = 0;
    args[i]->mutex = &mutex;
    assert(pthread_create(&ids[i], NULL, _run, (void *)args[i]) == 0);
  }

  usleep((milliseconds + 5) * 1000);

  for (int i = 0; i < threads; ++i)
    assert(pthread_join(ids[i], 0) == 0);

  assert(pthread_mutex_destroy(&mutex) == 0);

  assert(beg == end);

  printf("%d\n", beg);
  for (int i = 0; i < threads; ++i) {
    beg -= args[i]->idx * args[i]->time;
    printf("%d\n", beg);
  }

  assert(beg == 0);

  for (int i = 0; i < threads; ++i)
    free(args[i]);

  free_node(n);
}

void test_border_node_insert_and_search()
{
  printf("test border node insert and search\n");

  node *n = new_node(Border);
  node_lock(n);

  srand(time(0));
  // each key should be unique
  uint64_t key[15];
  for (int i = 0; i < 15; ++i) {
    key[i] = ((uint64_t)rand() << 32) + i;
    uint32_t tmp = 0;
    assert((int)node_insert(n, &key[i], sizeof(uint64_t), &tmp, &key[i], (i % 2) ? 1 : 0 /* link */) == 1);
  }

  // each key is already inserted
  for (int i = 0; i < 15; ++i) {
    uint32_t tmp = 0;
    if ((i % 2))
      assert((uint64_t *)node_insert(n, &key[i], sizeof(uint64_t), &tmp, &key[i], 0 /* link */) == &key[i]);
    else
      assert(node_insert(n, &key[i], sizeof(uint64_t), &tmp, &key[i], 0 /* link */) == 0);
  }

  assert(node_is_full(n));
  node_unlock(n);

  node_print(n, 1);

  for (int i = 0; i < 15; ++i) {
    void *suffix;
    uint32_t tmp = 0;
    if ((i % 2)) {
      assert((uint64_t *)node_search(n, &key[i], sizeof(uint64_t), &tmp, &suffix) == &key[i]);
    } else {
      assert(node_search(n, &key[i], sizeof(uint64_t), &tmp, &suffix) == 0);
      assert((uint64_t *)suffix == &key[i]);
    }
  }

  free_node_raw(n);
}

void test_interior_node_insert_and_search()
{
  printf("test interior node insert and search\n");

  node *n = new_node(Interior);
  node_lock(n);

  srand(time(0));
  uint64_t key[15];

  node_set_first_child(n, (void *)-1);
  for (int i = 0; i < 15; ++i) {
    key[i] = i * 2 + 1;
    uint32_t tmp = 0;
    assert((int)node_insert(n, &key[i], sizeof(uint64_t), &tmp, &key[i], 1 /* link */) == 1);
  }

  node_unlock(n);

  node_print(n, 1);

  for (int i = 0; i < 15; ++i) {
    uint64_t k = i * 2;
    uint32_t tmp = 0;
    if (i == 0)
      assert((int)node_locate_child(n, &k, sizeof(uint64_t), &tmp) == -1);
    else
      assert((uint64_t *)node_locate_child(n, &k, sizeof(uint64_t), &tmp) == &key[i - 1]);
  }
  uint64_t k = 15 * 2;
  uint32_t tmp = 0;
  assert((uint64_t *)node_locate_child(n, &k, sizeof(uint64_t), &tmp) == &key[14]);

  free_node_raw(n);
}

void test_border_node_split()
{
  printf("test border node split\n");

  node *n = new_node(Border);
  node_lock(n);

  srand(time(0));
  // each key should be unique
  uint64_t key[15];
  for (int i = 0; i < 15; ++i) {
    key[i] = i * 2 + 1;
    uint32_t tmp = 0;
    assert((int)node_insert(n, &key[i], sizeof(uint64_t), &tmp, &key[i], (i % 2) ? 1 : 0 /* link */) == 1);
  }

  node_print(n, 1);

  uint64_t fence;
  node *n1 = node_split(n, &fence);

  node_unlock(n);
  node_unlock(n1);

  assert(fence == key[7]);

  for (int i = 0; i < 7; ++i) {
    uint32_t tmp = 0;
    void *suffix;
    if ((i % 2)) {
      assert((uint64_t *)node_search(n, &key[i], sizeof(uint64_t), &tmp, &suffix) == &key[i]);
    } else {
      assert(node_search(n, &key[i], sizeof(uint64_t), &tmp, &suffix) == 0);
      assert((uint64_t *)suffix == &key[i]);
    }
    tmp = 0;
    assert(node_search(n1, &key[i], sizeof(uint64_t), &tmp, &suffix) == 0);
    assert(!suffix);
  }

  for (int i = 7; i < 15; ++i) {
    uint32_t tmp = 0;
    void *suffix;
    if ((i % 2)) {
      assert((uint64_t *)node_search(n1, &key[i], sizeof(uint64_t), &tmp, &suffix) == &key[i]);
    } else {
      assert(node_search(n1, &key[i], sizeof(uint64_t), &tmp, &suffix) == 0);
      assert((uint64_t *)suffix == &key[i]);
    }
    tmp = 0;
    assert(node_search(n, &key[i], sizeof(uint64_t), &tmp, &suffix) == 0);
    assert(!suffix);
  }

  assert(node_get_next(n) == n1);

  node_print(n, 1);
  node_print(n1, 1);

  free_node_raw(n);
  free_node_raw(n1);
}

void test_interior_node_split()
{
  printf("test interior node split\n");

  node *n = new_node(Interior);
  node_lock(n);

  srand(time(0));
  uint64_t key[15];
  node *child[16];
  child[0] = new_node(Border);
  node_set_parent(child[0], n);

  node_set_first_child(n, child[0]);
  for (int i = 0; i < 15; ++i) {
    key[i] = i * 2 + 1;
    child[i+1] = new_node(Border);
    node_set_parent(child[i+1], n);
    uint32_t tmp = 0;
    assert((int)node_insert(n, &key[i], sizeof(uint64_t), &tmp, child[i+1], 1 /* link */) == 1);
  }

  node_print(n, 1);

  uint64_t fence;
  node *n1 = node_split(n, &fence);

  node_unlock(n);
  node_unlock(n1);

  assert(fence == key[7]);

  for (int i = 0; i < 7; ++i) {
    uint64_t k = i * 2;
    uint32_t tmp = 0;
    assert(node_locate_child(n, &k, sizeof(uint64_t), &tmp) == child[i]);
    assert(node_get_parent(child[i]) == n);
  }
  uint64_t k = 7 * 2;
  uint32_t tmp = 0;
  assert(node_locate_child(n, &k, sizeof(uint64_t), &tmp) == child[7]);
  assert(node_get_parent(child[7]) == n);

  for (int i = 8; i < 15; ++i) {
    uint64_t k = i * 2;
    uint32_t tmp = 0;
    assert(node_locate_child(n1, &k, sizeof(uint64_t), &tmp) == child[i]);
    assert(node_get_parent(child[i]) == n1);
  }
  k = 15 * 2;
  tmp = 0;
  assert(node_locate_child(n1, &k, sizeof(uint64_t), &tmp) == child[15]);
  assert(node_get_parent(child[15]) == n1);

  node_print(n, 1);
  node_print(n1, 1);

  for (int i = 0; i < 16; ++i)
    free_node_raw(child[i]);
  free_node_raw(n);
  free_node_raw(n1);
}

int main()
{
  test_node_marco();
  test_node_utility_functions();
  test_node_lock();
  test_border_node_insert_and_search();
  test_interior_node_insert_and_search();
  test_border_node_split();
  test_interior_node_split();
  return 0;
}
