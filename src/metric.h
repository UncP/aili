/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#ifndef _metric_h_
#define _metric_h_

void init_metric();
void register_metric(const char *name);
void add_metric(const char *name, unsigned long long value);
void show_metric();
void free_metric();

#endif /* _metric_h_ */
