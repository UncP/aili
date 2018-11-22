/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>

#include "../blink/blink_tree.h"

static char *file_str;
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

void test_blink_tree()
{
  blink_tree *bt = new_blink_tree(thread_number);

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

      if (count && (count % 1000000) == 0)
        printf("%d\n", count);

      if (count++ == total_keys) {
        flag = 0;
        break;
      }

      // blink_tree_write(bt, key, len, (const void *)3190);
      blink_tree_schedule(bt, 1 /* is_write */, key, len, (const void *)3190);
    }
  }

  blink_tree_flush(bt);

  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mput time: %.4f  s\033[0m\n", total_keys, (float)(after - before) / 1000);

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

      // void *value;
      // assert(blink_tree_read(bt, key, len, &value));
      // assert((uint64_t)value == 3190);
      blink_tree_schedule(bt, 0 /* is_write */, key, len, 0);
    }
  }

  blink_tree_flush(bt);

  after = mstime();
  printf("\033[34mget time: %.4f  s\033[0m\n", (float)(after - before) / 1000);

  close(fd);

  free_blink_tree(bt);
}

int main(int argc, char **argv)
{
  if (argc < 5) {
    printf("file_name node_size thread_number key_number\n");
    exit(1);
  }

  file_str = argv[1];
  int node_size = atoi(argv[2]);
  thread_number = atoi(argv[3]);
  total_keys = atoi(argv[4]);
  if (total_keys <= 0) total_keys = 1;
  if (thread_number <= 0) thread_number = 1;

  set_node_size(node_size);

  test_blink_tree();

  return 0;
}
