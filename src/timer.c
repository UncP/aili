/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#include <time.h>

#include "timer.h"

unsigned long long get_cpu_clock()
{
  struct timespec ts;
  // clock_gettime(CLOCK_REALTIME, &ts);
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return (((unsigned long long)ts.tv_sec) * 1e9 + ts.tv_nsec) / 1000;
}
