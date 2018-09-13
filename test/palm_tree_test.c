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

const static uint64_t value = 3190;
static char *file;
static int all;

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
  palm_tree *pt = new_palm_tree();
  worker *w = new_worker(0, 1, NULL);
  batch *b = new_batch();

  char file_name[64];
  memset(file_name, 0, 64);
  memcpy(file_name, "./data/", 7);
  memcpy(file_name + 7, file, strlen(file));

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

      if (count++ == all) {
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
  printf("\033[31mtotal: %d\033[0m\n\033[32mput time: %f  s\033[0m\n", all, (float)(after - before) / 1000);

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

      if (count++ == all) {
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
}

// benchmark single thread performance with thread pool
void test_single_thread_palm_tree_with_pool()
{
  palm_tree *pt = new_palm_tree();
  bounded_queue *queue = new_bounded_queue(1);
  thread_pool *tp = new_thread_pool(1, pt, queue);
  // 2 batch is enough since we only have 1 thread
  batch *b1 = new_batch();
  batch *b2 = new_batch();

  char file_name[64];
  memset(file_name, 0, 64);
  memcpy(file_name, "./data/", 7);
  memcpy(file_name + 7, file, strlen(file));

  int fd = open(file_name, O_RDONLY);
  assert(fd > 0);
  int block = 4 * 4096, curr = 0, ptr = 0, count = 0;
  char buf[block];
  int flag = 1;
  long long before = mstime();
  batch *cb = b1;
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

      if (count++ == all) {
        flag = 0;
        break;
      }

      if (batch_add_write(cb, key, len, (void *)value) == -1) {
        bounded_queue_push(queue, cb);
        cb = cb == b1 ? b2 : b1;
        batch_clear(cb);
        assert(batch_add_write(cb, key, len, (void *)value) == 1);
      }
    }
  }

  // finish remained work
  bounded_queue_push(queue, cb);

  // wait until all the batches have been executed
  thread_pool_stop(tp);
  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mput time: %f  s\033[0m\n", all, (float)(after - before) / 1000);

  free_bounded_queue(queue);
  free_thread_pool(tp);
  batch_clear(b1);
  batch_clear(b2);

  palm_tree_validate(pt);

  cb = b1;
  queue = new_bounded_queue(1);
  tp = new_thread_pool(1, pt, queue);

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

      if (count++ == all) {
        flag = 0;
        break;
      }

      if (batch_add_read(cb, key, len) == -1) {
        bounded_queue_push(queue, cb);
        cb = cb == b1 ? b2 : b1;
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

  // wait until all the batches have been executed
  thread_pool_stop(tp);
  free_bounded_queue(queue);
  free_thread_pool(tp);

  for (uint32_t j = 0; j < b1->keys; ++j) {
    uint32_t op;
    void *key = 0;
    uint32_t klen;
    void *val = 0;
    batch_read_at(b1, j, &op, &key, &klen, &val);
    assert(*(uint64_t *)val == value);
  }
  for (uint32_t j = 0; j < b2->keys; ++j) {
    uint32_t op;
    void *key = 0;
    uint32_t klen;
    void *val = 0;
    batch_read_at(b2, j, &op, &key, &klen, &val);
    assert(*(uint64_t *)val == value);
  }

  after = mstime();
  printf("\033[34mget time: %f  s\033[0m\n", (float)(after - before) / 1000);

  close(fd);

  free_palm_tree(pt);
  free_batch(b1);
  free_batch(b2);
}

int main(int argc, char **argv)
{
  if (argc < 2) {
    printf("file_name key_number\n");
    exit(0);
  }

  file = argv[1];
  all = atoi(argv[2]);
  if (all <= 0) all = 1;

  // test_single_thread_palm_tree();
  test_single_thread_palm_tree_with_pool();

  return 0;
}
