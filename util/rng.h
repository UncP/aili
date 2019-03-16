/**
 *    author:     UncP
 *    date:    2019-03-14
 *    license:    BSD-3
**/

#ifndef _rng_h_
#define _rng_h_

#include <stdint.h>

typedef struct rng
{
  uint64_t state[2];
}rng;

void rng_init(rng *r, uint64_t seed1, uint64_t seed2);
uint64_t rng_next(rng *r);

#endif /* _rng_h_ */
