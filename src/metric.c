/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#include <stdio.h>
#include <c_hashmap/hashmap.h>

#include "metric.h"

#define max_metric_entry 64

struct metric
{
  char *name[max_metric_entry];
  int   len;
  map_t hashmap;
};

static struct metric metric;

void init_metric()
{
  metric.len = 0;
  metric.hashmap = hashmap_new();
}

void register_metric(const char *name)
{
  if (metric.len == max_metric_entry) return ;
  metric.name[metric.len++] = (char *)name;
  hashmap_put(metric.hashmap, (char *)name, 0);
}

void add_metric(const char *name, unsigned long long value)
{
  unsigned long long old;
  hashmap_get(metric.hashmap, (char *)name, (void *)&old);
  unsigned long long new = old + value;
  hashmap_put(metric.hashmap, (char *)name, (void *)new);
}

void show_metric()
{
  for (int i = 0; i < metric.len; ++i) {
    unsigned long long value;
    hashmap_get(metric.hashmap, metric.name[i], (void *)&value);
    hashmap_put(metric.hashmap, metric.name[i], 0);
    printf("%-48s : %llu\n", metric.name[i], value);
  }
}

void free_metric()
{
  hashmap_free(metric.hashmap);
}
