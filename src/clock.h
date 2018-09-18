/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#ifndef _clock_h_
#define _clock_h_

struct clock
{
  unsigned long long cpu;
  unsigned long long tot;
};

struct clock* new_clock();
struct clock clock_get();
struct clock clock_get_duration(struct clock *old, struct clock *new);
void clock_update(void *a, void *b);
void clock_print(void *a);

#endif /* _clock_h_ */