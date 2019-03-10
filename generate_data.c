/**
 *    author:     UncP
 *    date:    2018-09-05
 *    license:    BSD-3
**/

#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <stdint.h>

static const char *dir = "./data";

static void generate_test_data(int file_name, int key_num, int key_len, int random, int initial, int step)
{
  uint64_t total_len = 0;

  char file[64];
  memset(file, 0, 64);
  memcpy(file, dir, strlen(dir));
  sprintf(file + strlen(dir), "/%d", file_name);
  int fd = open(file, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
  assert(fd != -1);

  const char *string = "abcdefghijklmnopqrstuvwxyz0123456789";
  const int str_len = strlen(string);

  char buf[8192];
  int ptr = 0;
  char key[key_len + 1];
  for (int i = 0; i < key_len; ++i)
    key[i] = '0';
  key[key_len - 1] = '0' + initial;
  for (int i = 0; i < key_num; ++i) {
    if (random) {
      for (int k = 0; k < key_len; ++k)
        key[k] = string[rand() % str_len];
    } else {
      int cur = key_len - 1, s = step;
      while (key[cur] + step > '9') {
        key[cur] = key[cur] + step - 10;
        s = 1;
        assert(--cur >= 0);
      }
      key[cur] += s;
    }
    key[key_len] = '\n';
    if (ptr + key_len + 1 >= 8192) {
      assert(pwrite(fd, buf, ptr, total_len) == ptr);
      total_len += ptr;
      ptr = 0;
    }
    memcpy(buf + ptr, key, key_len + 1);
    ptr += key_len + 1;
    if (i && (i % 1000000) == 0)
      printf("total %d, generated %d\n", key_num, i);
  }

  assert(pwrite(fd, buf, ptr, total_len) == ptr);

  close(fd);
}

int main(int argc, char **argv)
{
  if (argc < 4) {
    printf("file_number key_number key_length\n");
    exit(1);
  }
  srand(time(NULL));

  int file_number = atoi(argv[1]);
  int key_num = atoi(argv[2]);
  int key_len = atoi(argv[3]);
  if (file_number <= 0) file_number = 1;
  if (key_num <= 0) key_num = 1000000;
  if (key_num > 100000000) key_num = 100000000;
  if (key_len <= 0) key_len = 8;
  if (key_len > 255) key_len = 255;

  if (access(dir, F_OK))
    assert(mkdir(dir, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IROTH) >= 0);

  int file = 1;
  printf("generate random test data\n");
  for (int i = 0; i < file_number; ++i, ++file)
    generate_test_data(file, key_num, key_len, 1 /* random */, 0, 0);

  printf("generate sequential test data\n");
  for (int i = 0; i < file_number; ++i, ++file)
    generate_test_data(file, key_num, key_len, 0 /* sequential */, i, file_number);

  printf("key_num: %d  key_len: %d\n", key_num, key_len);
  return 0;
}
