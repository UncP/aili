/**
 *    author:     UncP
 *    date:    2018-09-05
 *    license:    BSD-3
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

int main(int argc, char **argv)
{
  if (argc < 3) {
    printf("key_number key_length [file_name]\n");
    exit(1);
  }

  int key_num = atoi(argv[1]);
  int key_len = atoi(argv[2]);
  char *file_name = "0\0";
  if (argc == 4) file_name = argv[3];
  if (key_num <= 0) key_num = 1000000;
  if (key_num > 100000000) key_num = 100000000;
  if (key_len <= 0) key_len = 16;
  if (key_len > 255) key_len = 255;

  const char *string = "abcdefghijklmnopqrstuvwxyz0123456789";
  const int str_len = strlen(string);

  const char *dir = "./data";

  if (access(dir, F_OK))
    assert(mkdir(dir, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IROTH) >= 0);

  srand(time(NULL));

  uint64_t total_len = 0;

  char file[64];
  memset(file, 0, 64);
  memcpy(file, dir, strlen(dir));
  sprintf(file + strlen(dir), "/%s", file_name);
  int fd = open(file, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
  assert(fd != -1);

  char buf[8192];
  int ptr = 0;
  for (int i = 0; i < key_num; ++i) {
    char key[key_len + 1];
    for (int k = 0; k < key_len; ++k)
      key[k] = string[rand() % str_len];
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

  fsync(fd);
  close(fd);
  printf("name:%s  key_number:%d  key_length:%d\n", file, key_num, key_len);

  return 0;
}
