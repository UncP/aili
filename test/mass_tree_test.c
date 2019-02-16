/**
 *    author:     UncP
 *    date:    2018-12-04
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
#include <pthread.h>

#include "../mass/mass_tree.h"
#ifdef Allocator
#include "../palm/allocator.h"
#endif // Allocator

static long long mstime()
{
  struct timeval tv;
  long long ust;

  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec)*1000000;
  ust += tv.tv_usec;
  return ust / 1000;
}

struct thread_arg
{
  mass_tree *mt;
  int file;
  int total_keys;
  int write;
};

static void* run(void *arg)
{
  struct thread_arg *ta = (struct thread_arg *)arg;
  mass_tree *mt = ta->mt;
  int file = ta->file;
  int total_keys = ta->total_keys;
  int write = ta->write;

  char file_name[32];
  memset(file_name, 0, 32);
  memcpy(file_name, "./data/", 7);
  char file_buf[32];
  int file_len = snprintf(file_buf, 32, "%d", file);
  memcpy(file_name + 7, file_buf, file_len);

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

      if (write) {
        void *slice;
        #ifdef Allocator
          slice = allocator_alloc_small(len);
        #else
          slice = malloc(len);
        #endif // Allocator
        memcpy(slice, key, len);
        mass_tree_put(mt, slice, len, (const void *)3190);
      } else {
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
  }

  long long after = mstime();
  printf("\033[31mtotal: %d\033[0m\n\033[32mtime: %.4f  s\033[0m\n", total_keys, (float)(after - before) / 1000);

  close(fd);

  return (void *)ta;
}

void test_mass_tree(int file, int thread_number, int total_keys)
{
  mass_tree *mt = new_mass_tree(thread_number);

  int thread_keys = total_keys / thread_number;
  pthread_t ids[thread_number];
  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta = malloc(sizeof(struct thread_arg));
    ta->mt = mt;
    ta->file = i + file;
    ta->total_keys = thread_keys;
    ta->write = 1;
    assert(pthread_create(&ids[i], 0, run, (void *)ta) == 0);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta;
    assert(pthread_join(ids[i], (void **)&ta) == 0);
    free(ta);
  }

  mass_tree_validate(mt);

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta = malloc(sizeof(struct thread_arg));
    ta->mt = mt;
    ta->file = i + file;
    ta->total_keys = thread_keys;
    ta->write = 0;
    assert(pthread_create(&ids[i], 0, run, (void *)ta) == 0);
  }

  for (int i = 0; i < thread_number; ++i) {
    struct thread_arg *ta;
    assert(pthread_join(ids[i], (void **)&ta) == 0);
    free(ta);
  }

  free_mass_tree(mt);
}

int main(int argc, char **argv)
{
  if (argc < 4) {
    printf("file_name thread_number key_number\n");
    exit(1);
  }

  int file = atoi(argv[1]);
  int thread_number = atoi(argv[2]);
  int total_keys = atoi(argv[3]);
  if (total_keys <= 0) total_keys = 1;
  if (thread_number <= 0) thread_number = 1;

  test_mass_tree(file, thread_number, total_keys);

  return 0;
}
