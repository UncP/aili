/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "clock.h"

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

struct clock clock_get_duration(struct clock *old, struct clock* new)
{
  struct clock c;
  c.cpu = new->cpu - old->cpu;
  c.tot = new->tot - old->tot;
  return c;
}

void clock_update(void *a, void *b)
{
  struct clock *c = (struct clock *)a;
  struct clock *d = (struct clock *)b;
  c->cpu += d->cpu;
  c->tot += d->tot;
}

void clock_print(void *a)
{
  struct clock *c = (struct clock *)a;
  printf("cpu:%llu   total:%llu\n", c->cpu, c->tot);
  c->cpu = 0;
  c->tot = 0;
}
