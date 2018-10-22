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

void test_node_marco()
{
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

static long long mstime()
{
  struct timeval tv;
  long long ust;

  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec)*1000000;
  ust += tv.tv_usec;
  return ust / 1000;
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

void* _run(void *arg)
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
  node *n = new_node(Border);

  // single thread
  node_lock(n);
  assert(is_locked(n->version));
  node_unlock(n);
  assert(!is_locked(n->version));

  // multi thread
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

int main()
{
  test_node_marco();
  test_node_lock();
  return 0;
}
