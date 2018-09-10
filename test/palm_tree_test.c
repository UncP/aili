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

// benchmark single thread performance
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

int main(int argc, char **argv)
{
  if (argc < 2) {
    printf("file_name key_number\n");
    exit(0);
  }

  file = argv[1];
  all = atoi(argv[2]);
  if (all <= 0) all = 1;

  test_single_thread_palm_tree();

  return 0;
}
