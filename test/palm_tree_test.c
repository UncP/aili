/**
 *    author:     UncP
 *    date:    2018-08-31
 *    license:    BSD-3
**/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>

#include "../src/palm_tree.h"
#include "../src/node.h"
#include "../src/worker.h"
#include "../src/bounded_queue.h"
#include "../src/thread_pool.h"
#include "../src/clock.h"
#include "../src/metric.h"

const static uint64_t value = 3190;
static char *file_str;
static int queue_size;
static int thread_number;
static int total_keys;

long long mstime()
{
  struct timeval tv;
  long long ust;

  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec)*1000000;
  ust += tv.tv_usec;
  return ust / 1000;
}

// benchmark single thread performance without thread pool
void test_single_thread_palm_tree()
{
  init_metric(1);
  palm_tree *pt = new_palm_tree(1);
  worker *w = new_worker(0, 1);
  batch *b = new_batch();

  char file_name[32];
  memset(file_name, 0, 32);
  memcpy(file_name, "./data/", 7);
  memcpy(file_name + 7, file_str, strlen(file_str));

  int fd = open(file_name, O_RDONLY);
  assert(fd > 0);
  int block = 4 * 4096, curr = 0, ptr = 0, count = 0;
  char buf[block];
  int flag = 1;
  long long before = mstime();
  for (; (ptr = pread(fd, buf, block, curr)) > 0 && flag; curr += ptr) {
    while (--ptr && buf[ptr] != '\n' && buf[ptr] != '\0') buf[ptr] = '\0';
    if (ptr) buf[ptr++] = '\0';
    else break;
    for (int i = 0; i < ptr; ++i) {
      char *key = buf + i, *tmp = key;
      uint32_t len = 0;
      while (tmp[len] != '\0' && tmp[len] != '\n')
        ++len;
      tmp[len] = '\0';
      i += len;

      if (count++ == total_keys) {
        flag = 0;
        break;
      }

      if (batch_add_write(b, key, len, (void *)value) == -1) {
        palm_tree_execute(pt, b, w);
        batch_clear(b);
        assert(batch_add_write(b, key, len, (void *)value) == 1);
      }
    }
  }

  // finish remained work
  palm_tree_execute(pt, b, w);
  batch_clear(b);
  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mput time: %f  s\033[0m\n", total_keys, (float)(after - before) / 1000);

  palm_tree_validate(pt);

  curr = 0;
  flag = 1;
  count = 0;
  before = mstime();
  for (; (ptr = pread(fd, buf, block, curr)) > 0 && flag; curr += ptr) {
    while (--ptr && buf[ptr] != '\n' && buf[ptr] != '\0') buf[ptr] = '\0';
    if (ptr) buf[ptr++] = '\0';
    else break;
    for (int i = 0; i < ptr; ++i) {
      char *key = buf + i, *tmp = key;
      uint32_t len = 0;
      while (tmp[len] != '\0' && tmp[len] != '\n')
        ++len;
      tmp[len] = '\0';
      i += len;

      if (count++ == total_keys) {
        flag = 0;
        break;
      }

      if (batch_add_read(b, key, len) == -1) {
        palm_tree_execute(pt, b, w);
        for (uint32_t j = 0; j < b->keys; ++j) {
          uint32_t op;
          void *key = 0;
          uint32_t klen;
          void *val = 0;
          batch_read_at(b, j, &op, &key, &klen, &val);
          assert(*(uint64_t *)val == value);
        }
        batch_clear(b);
        assert(batch_add_read(b, key, len) == 1);
      }
    }
  }

  // finish remained work
  palm_tree_execute(pt, b, w);
  for (uint32_t j = 0; j < b->keys; ++j) {
    uint32_t op;
    void *key = 0;
    uint32_t klen;
    void *val = 0;
    batch_read_at(b, j, &op, &key, &klen, &val);
    assert(*(uint64_t *)val == value);
  }
  batch_clear(b);

  after = mstime();
  printf("\033[34mget time: %f  s\033[0m\n", (float)(after - before) / 1000);

  close(fd);

  free_batch(b);
  free_worker(w);
  free_palm_tree(pt);
  free_metric();
}

// benchmark single thread performance with thread pool
void test_palm_tree_with_thread_pool()
{
  init_metric(thread_number);
  palm_tree *pt = new_palm_tree(thread_number);
  bounded_queue *queue = new_bounded_queue(queue_size);
  batch *batches[queue_size + 1];
  thread_pool *tp = new_thread_pool(thread_number, pt, queue);
  for (int i = 0; i < queue_size + 1; ++i)
    batches[i] = new_batch();

  char file_name[32];
  memset(file_name, 0, 32);
  memcpy(file_name, "./data/", 7);
  memcpy(file_name + 7, file_str, strlen(file_str));

  int fd = open(file_name, O_RDONLY);
  assert(fd > 0);
  int block = 4 * 4096, curr = 0, ptr = 0, count = 0;
  char buf[block];
  int flag = 1;
  long long before = mstime();
  int idx = 0;
  batch *cb = batches[idx];
  for (; (ptr = pread(fd, buf, block, curr)) > 0 && flag; curr += ptr) {
    while (--ptr && buf[ptr] != '\n' && buf[ptr] != '\0') buf[ptr] = '\0';
    if (ptr) buf[ptr++] = '\0';
    else break;
    for (int i = 0; i < ptr; ++i) {
      char *key = buf + i, *tmp = key;
      uint32_t len = 0;
      while (tmp[len] != '\0' && tmp[len] != '\n')
        ++len;
      tmp[len] = '\0';
      i += len;

      if (count && (count % 1000000) == 0)
        printf("%d\n", count);

      if (count++ == total_keys) {
        flag = 0;
        break;
      }

      if (batch_add_write(cb, key, len, (void *)value) == -1) {
        bounded_queue_push(queue, cb);
        idx = idx == queue_size ? 0 : idx + 1;
        cb = batches[idx];
        batch_clear(cb);
        assert(batch_add_write(cb, key, len, (void *)value) == 1);
      }
    }
  }

  // finish remained work
  bounded_queue_push(queue, cb);

  // wait until all keys in the batches have been executed
  thread_pool_stop(tp);
  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mput time: %f  s\033[0m\n", total_keys, (float)(after - before) / 1000);
  show_metric(clock_print);

  free_bounded_queue(queue);
  free_thread_pool(tp);
  for (int i = 0; i < queue_size + 1; ++i)
    batch_clear(batches[i]);

  palm_tree_validate(pt);

  queue = new_bounded_queue(queue_size);
  tp = new_thread_pool(thread_number, pt, queue);

  curr = 0;
  flag = 1;
  count = 0;
  before = mstime();
  for (; (ptr = pread(fd, buf, block, curr)) > 0 && flag; curr += ptr) {
    while (--ptr && buf[ptr] != '\n' && buf[ptr] != '\0') buf[ptr] = '\0';
    if (ptr) buf[ptr++] = '\0';
    else break;
    for (int i = 0; i < ptr; ++i) {
      char *key = buf + i, *tmp = key;
      uint32_t len = 0;
      while (tmp[len] != '\0' && tmp[len] != '\n')
        ++len;
      tmp[len] = '\0';
      i += len;

      if (count && (count % 1000000) == 0)
        printf("%d\n", count);

      if (count++ == total_keys) {
        flag = 0;
        break;
      }

      if (batch_add_read(cb, key, len) == -1) {
        bounded_queue_push(queue, cb);
        idx = idx == queue_size ? 0 : idx + 1;
        cb = batches[idx];
        for (uint32_t j = 0; j < cb->keys; ++j) {
          uint32_t op;
          void *key = 0;
          uint32_t klen;
          void *val = 0;
          batch_read_at(cb, j, &op, &key, &klen, &val);
          assert(*(uint64_t *)val == value);
        }
        batch_clear(cb);
        assert(batch_add_read(cb, key, len) == 1);
      }
    }
  }

  // finish remained work
  bounded_queue_push(queue, cb);

  // wait until all keys in the batches have been executed
  thread_pool_stop(tp);

  for (int i = 0; i < queue_size + 1; ++i) {
    cb = batches[i];
    for (uint32_t j = 0; j < cb->keys; ++j) {
      uint32_t op;
      void *key = 0;
      uint32_t klen;
      void *val = 0;
      batch_read_at(cb, j, &op, &key, &klen, &val);
      assert(*(uint64_t *)val == value);
    }
  }

  after = mstime();
  printf("\033[34mget time: %f  s\033[0m\n", (float)(after - before) / 1000);

  close(fd);

  show_metric(clock_print);

  free_bounded_queue(queue);
  free_thread_pool(tp);

  for (int i = 0; i < queue_size + 1; ++i)
    free_batch(batches[i]);
  free_palm_tree(pt);

  free_metric();
}

int main(int argc, char **argv)
{
  if (argc < 5) {
    printf("file_name thread_number queue_size key_number\n");
    exit(1);
  }

  file_str = argv[1];
  thread_number = atoi(argv[2]);
  queue_size = atoi(argv[3]);
  total_keys = atoi(argv[4]);
  if (total_keys <= 0) total_keys = 1;
  if (queue_size <= 0) queue_size = 1;
  if (thread_number <= 0) queue_size = 1;

  // test_single_thread_palm_tree();
  test_palm_tree_with_thread_pool();

  return 0;
}
