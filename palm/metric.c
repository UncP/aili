/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <c_hashmap/hashmap.h>

#include "metric.h"

struct clock* new_clock()
{
  struct clock *c = (struct clock *)malloc(sizeof(struct clock));
  c->cpu = 0;
  c->tot = 0;
  return c;
}

struct clock clock_get()
{
  struct clock c;
  struct timespec ts;

  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  c.cpu = (((unsigned long long)ts.tv_sec) * 1e9 + ts.tv_nsec) / 1000;

  clock_gettime(CLOCK_REALTIME, &ts);
  c.tot = (((unsigned long long)ts.tv_sec) * 1e9 + ts.tv_nsec) / 1000;
  return c;
}

static struct clock clock_get_duration(struct clock *old, struct clock* new)
{
  struct clock c;
  c.cpu = new->cpu - old->cpu;
  c.tot = new->tot - old->tot;
  return c;
}

static void clock_update(struct clock *c, struct clock *d)
{
  c->cpu += d->cpu;
  c->tot += d->tot;
}

static void clock_reset(struct clock *c)
{
  c->cpu = 0;
  c->tot = 0;
}

#define max_metric_entry 32

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
  return &metrics[id];
}

void register_metric(int id, const char *name, struct clock *c)
{
	metric *m = _get_metric(id);
  if (m->len == max_metric_entry) return ;
  m->name[m->len++] = (char *)name;
  hashmap_put(m->hashmap, (char *)name, (void *)c);
}

void update_metric(int id, const char *name, struct clock *before)
{
  struct clock now = clock_get();
  struct clock duration = clock_get_duration(before, &now);
  metric *m = _get_metric(id);
  struct clock *old;
  hashmap_get(m->hashmap, (char *)name, (void **)&old);
  clock_update(old, &duration);
  *before = now;
}

void show_metric()
{
  metric *m = _get_metric(0);
  struct clock clocks[m->len];
  for (int i = 0; i < m->len; ++i)
    clock_reset(&clocks[i]);

  for (int i = 0; i < m->len; ++i) {
    for (int j = 0; j < metric_num; ++j) {
      metric *m = _get_metric(j);
      struct clock *c;
      hashmap_get(m->hashmap, (char *)m->name[i], (void **)&c);
      clock_update(&clocks[i], c);
      clock_reset(c);
    }
  }

  struct clock all;
  clock_reset(&all);
  for (int i = 0; i < m->len; ++i) {
    clocks[i].cpu /= metric_num;
    clocks[i].tot /= metric_num;
    clock_update(&all, &clocks[i]);
  }

  printf("cpu: %llu us    total: %llu us\n", all.cpu, all.tot);
  for (int i = 0; i < m->len; ++i) {
    printf("%-24s:  cpu: %5.2f %%  tot: %5.2f %%\n", m->name[i],
      (float)clocks[i].cpu / all.cpu * 100,
      (float)clocks[i].tot / all.tot * 100);
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
