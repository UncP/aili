/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#ifndef _metric_h_
#define _metric_h_

struct clock
{
  unsigned long long cpu;
  unsigned long long tot;
};

struct clock* new_clock();
struct clock clock_get();

void init_metric(int num);
void register_metric(int id, const char *name, struct clock *c);
void update_metric(int id, const char *name, struct clock *before);
void show_metric();
void free_metric();

#endif /* _metric_h_ */
