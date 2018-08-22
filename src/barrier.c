/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include <string.h>

#include "barrier.h"

void init_barrier(barrier *b, uint32_t member)
{
	memset(b, 0, sizeof(barrier));
	b->member = member;
	b->left = member;
	b->current = 0;
	pthread_mutex_init(&b->mutex, 0);
	pthread_cond_init(&b->cond, 0);
}

void barrier_wait(barrier *b)
{
	pthread_mutex_lock(&b->mutex);
	uint32_t current = b->current;
	if (--b->left == 0) {
		++b->current;
		b->left = b->member;
		pthread_mutex_unlock(&b->mutex);
		pthread_cond_broadcast(&b->cond);
	} else {
		while (b->current == current)
			pthread_cond_wait(&b->cond, &b->mutex);
		pthread_mutex_unlock(&b->mutex);
	}
}
