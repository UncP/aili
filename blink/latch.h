/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#ifndef _latch_h_
#define _latch_h_

#include <pthread.h>

// currently latch is just a posix rw lock
typedef struct latch
{
	pthread_rwlock_t val[1];
}latch;

#define latch_init(latch)  pthread_rwlock_init((latch)->val, 0);
#define latch_rlock(latch) pthread_rwlock_rdlock((latch)->val);
#define latch_wlock(latch) pthread_rwlock_wrlock((latch)->val);
#define latch_unlock(latch) pthread_rwlock_unlock((latch)->val);

#endif /* _latch_h_ */
