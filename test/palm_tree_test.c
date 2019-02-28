/**
 *    author:     UncP
 *    date:    2018-08-31
 *    license:    BSD-3
**/

#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>

#include "../palm/palm_tree.h"
#include "../palm/metric.h"

static const uint64_t value = 3190;
static char *file_str;
static int queue_size;
static int thread_number;
static int total_keys;

static long long mstime()
{
  struct timeval tv;
  long long ust;

  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec)*1000000;
  ust += tv.tv_usec;
  return ust / 1000;
}

void test_palm_tree()
{
  palm_tree *pt = new_palm_tree(thread_number, queue_size);
  batch *batches[queue_size + 1];
  for (int i = 0; i < queue_size + 1; ++i)
    batches[i] = new_batch();

  char file_name[32];
  memset(file_name, 0, 32);
  memcpy(file_name, "./data/", 7);
  memcpy(file_name + 7, file_str, strlen(file_str));

  int fd = open(file_name, O_RDONLY);
  assert(fd > 0);
  int block = 65536, curr = 0, ptr = 0, count = 0;
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
        palm_tree_execute(pt, cb);
        idx = idx == queue_size ? 0 : idx + 1;
        cb = batches[idx];
        batch_clear(cb);
        assert(batch_add_write(cb, key, len, (void *)value) == 1);
      }
    }
  }

  // finish remained work
  palm_tree_execute(pt, cb);
  palm_tree_flush(pt);

  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mput time: %.4f  s\033[0m\n", total_keys, (float)(after - before) / 1000);
  show_metric();

  for (int i = 0; i < queue_size + 1; ++i)
    batch_clear(batches[i]);

  // palm_tree_validate(pt);

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
        palm_tree_execute(pt, cb);
        idx = idx == queue_size ? 0 : idx + 1;
        cb = batches[idx];
        for (uint32_t j = 0; j < cb->keys; ++j)
          assert((uint64_t)batch_get_value_at(cb, j) == value);
        batch_clear(cb);
        assert(batch_add_read(cb, key, len) == 1);
      }
    }
  }

  // finish remained work
  palm_tree_execute(pt, cb);
  palm_tree_flush(pt);

  for (int i = 0; i < queue_size + 1; ++i) {
    cb = batches[i];
    for (uint32_t j = 0; j < cb->keys; ++j)
      assert((uint64_t)batch_get_value_at(cb, j) == value);
  }

  after = mstime();
  printf("\033[34mget time: %.4f  s\033[0m\n", (float)(after - before) / 1000);

  close(fd);

  show_metric();

  for (int i = 0; i < queue_size + 1; ++i)
    free_batch(batches[i]);

  free_palm_tree(pt);
}

int main(int argc, char **argv)
{
  if (argc < 7) {
    printf("file_name node_size batch_size thread_number queue_size key_number\n");
    exit(1);
  }

  file_str = argv[1];
  int node_size = atoi(argv[2]);
  int batch_size = atoi(argv[3]);
  thread_number = atoi(argv[4]);
  queue_size = atoi(argv[5]);
  total_keys = atoi(argv[6]);
  if (total_keys <= 0) total_keys = 1;
  if (queue_size <= 0) queue_size = 1;
  if (thread_number <= 0) thread_number = 1;
  set_node_size(node_size);
  set_batch_size(batch_size);

  test_palm_tree();

  return 0;
}
