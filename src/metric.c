/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <c_hashmap/hashmap.h>

#include "metric.h"

#define max_metric_entry 64

typedef struct metric
{
  char *name[max_metric_entry];
  int   len;
  map_t hashmap;
}metric;

static metric *metrics;
static int metric_num;

void init_metric(int num)
{
  if (num <= 0) num = 1;

  metric_num = num;
  metrics = (metric *)malloc(sizeof(metric) * metric_num);

  for (int i = 0; i < metric_num; ++i) {
    metrics[i].len = 0;
    metrics[i].hashmap = hashmap_new();
  }
}

static metric* _get_metric(int id)
{
  assert(id < metric_num);
  return &metrics[id];
}

void register_metric(int id, const char *name, void *ptr)
{
	metric *m = _get_metric(id);
  if (m->len == max_metric_entry) return ;
  m->name[m->len++] = (char *)name;
  hashmap_put(m->hashmap, (char *)name, ptr);
}

void update_metric(int id, const char *name, void *new, void (*update)(void *, void *))
{
  metric *m = _get_metric(id);
  void *old;
  hashmap_get(m->hashmap, (char *)name, &old);
  (*update)(old, new);
}

void show_metric(void (*print)(void *))
{
  for (int i = 0; i < metric_num; ++i) {
  	printf("Thread %d\n", i+1);
    metric *m = _get_metric(i);
    for (int j = 0; j < m->len; ++j) {
      void *r;
      hashmap_get(m->hashmap, (char *)m->name[j], &r);
      printf("%-48s: ", m->name[j]);
      (*print)(r);
    }
  }
}

void free_metric()
{
  for (int i = 0; i < metric_num; ++i) {
    metric *m = _get_metric(i);
    for (int j = 0; j < m->len; ++j) {
      void *r;
      hashmap_get(m->hashmap, (char *)m->name[j], &r);
      free(r);
    }
    hashmap_free(m->hashmap);
  }
  free((void *)metrics);
}
