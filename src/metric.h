/**
 *    author:     UncP
 *    date:    2018-09-17
 *    license:    BSD-3
**/

#ifndef _metric_h_
#define _metric_h_

void init_metric(int num);
void register_metric(int id, const char *name, void *ptr);
void update_metric(int id, const char *name, void *new, void (*update)(void *, void *));
void show_metric(void (*print)(void *));
void free_metric();

#endif /* _metric_h_ */
