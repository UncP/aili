/**
 *    author:     UncP
 *    date:    2018-12-04
 *    license:    BSD-3
**/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>

#include "../mass/mass_tree.h"
#include "../palm/allocator.h"

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

void test_mass_tree()
{
  init_allocator();

  mass_tree *mt = new_mass_tree(thread_number);

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

      void *slice = allocator_alloc(len);
      memcpy(slice, key, len);

      if (count && (count % 1000000) == 0)
        printf("%d\n", count);

      if (count++ == total_keys) {
        flag = 0;
        break;
      }

      assert(mass_tree_put(mt, slice, len, (const void *)3190) == 1);
      // void *value = mass_tree_get(mt, key, len);
      // if (value == 0) {
      //   char buf[len + 1];
      //   memcpy(buf, key, len);
      //   buf[len] = 0;
      //   printf("%s\n", buf);
      // }
      // assert(value);
      // assert(memcmp(value, key, len) == 0);
    }
  }

  // mass_tree_flush(mt);

  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mput time: %.4f  s\033[0m\n", total_keys, (float)(after - before) / 1000);

  mass_tree_validate(mt);

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

      void *value = mass_tree_get(mt, key, len);
      if (value == 0) {
      	char buf[len + 1];
      	memcpy(buf, key, len);
      	buf[len] = 0;
      	printf("%s\n", buf);
      }
      assert(value);
      assert(memcmp(value, key, len) == 0);
    }
  }

  // mass_tree_flush(mt);

  after = mstime();
  printf("\033[34mget time: %.4f  s\033[0m\n", (float)(after - before) / 1000);

  close(fd);

  free_mass_tree(mt);
}

int main(int argc, char **argv)
{
  if (argc < 4) {
    printf("file_name thread_number key_number\n");
    exit(1);
  }

  file_str = argv[1];
  thread_number = atoi(argv[2]);
  total_keys = atoi(argv[3]);
  if (total_keys <= 0) total_keys = 1;
  if (thread_number <= 0) thread_number = 1;

  test_mass_tree();

  return 0;
}
