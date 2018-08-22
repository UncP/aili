/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#ifndef _barrier_h_
#define _barrier_h_

#include <stdint.h>
#include <pthread.h>

typedef struct barrier
{
	uint32_t        member;
	uint32_t        left;
	uint32_t        current;
	pthread_mutex_t mutex;
	pthread_cond_t  cond;
}barrier;

void init_barrier(barrier *b, uint32_t member);
void barrier_wait(barrier *b);

#endif /* _barrier_h_ */