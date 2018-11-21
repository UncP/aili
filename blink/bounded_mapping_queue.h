/**
 *    author:     UncP
 *    date:    2018-11-21
 *    license:    BSD-3
**/

#ifndef _bounded_mapping_queue_h_
#define _bounded_mapping_queue_h_

#include <pthread.h>

// do not change it
#define max_queue_size 64

typedef struct bounded_mapping_queue
{
  void    *elements[max_queue_size];
  int      clear;
  uint64_t free;  // bitmap
  uint64_t busy;  // bitmap

  pthread_mutex_t mutex[1];
  pthread_cond_t  free_cond[1];
  pthread_cond_t  busy_cond[1];
}bounded_mapping_queue;

bounded_mapping_queue* new_bounded_mapping_queue();
void free_bounded_mapping_queue(bounded_mapping_queue *q);
void bounded_mapping_queue_clear(bounded_mapping_queue *q);
void* bounded_mapping_queue_get_free(bounded_mapping_queue *q, int *idx);
void bounded_mapping_queue_put_free(bounded_mapping_queue *q, int idx);
void* bounded_mapping_queue_get_busy(bounded_mapping_queue *q, int *idx);
void bounded_mapping_queue_put_busy(bounded_mapping_queue *q, int idx);
void bounded_mapping_queue_wait_empty(bounded_mapping_queue *q);

#endif /* _bounded_mapping_queue_h_ */