/**
 *    author:     UncP
 *    date:    2018-11-21
 *    license:    BSD-3
**/

#ifndef _mapping_array_h_
#define _mapping_array_h_

#include <stdint.h>
#include <pthread.h>

// do not change it
#define max_array_size 64

// not an FIFO queue, starvation might occur
typedef struct mapping_array
{
  void    *elements[max_array_size];
  int      clear;
  uint64_t free;  // bitmap
  uint64_t busy;  // bitmap

  pthread_mutex_t mutex[1];
  pthread_cond_t  free_cond[1];
  pthread_cond_t  busy_cond[1];
}mapping_array;

mapping_array* new_mapping_array();
void free_mapping_array(mapping_array *q);
void mapping_array_clear(mapping_array *q);
void mapping_array_wait_empty(mapping_array *q);
void* mapping_array_get_free(mapping_array *q, int *idx);
void mapping_array_put_free(mapping_array *q, int idx);
void* mapping_array_get_busy(mapping_array *q, int *idx);
void mapping_array_put_busy(mapping_array *q, int idx);

#endif /* _mapping_array_h_ */
