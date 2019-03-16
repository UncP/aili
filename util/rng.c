/**
 *    author:     UncP
 *    date:    2019-03-14
 *    license:    BSD-3
**/

#include "rng.h"

// Bitwise circular left shift.
inline uint64_t rotl(uint64_t x, int k)
{
  return (x << k) | (x >> (64 - k));
}

void rng_init(rng *r, uint64_t seed1, uint64_t seed2)
{
  r->state[0] = seed1;
  r->state[1] = seed2;
}

inline uint64_t rng_next(rng *r)
{
  const uint64_t s0 = r->state[0];
  uint64_t s1 = r->state[1];
  const uint64_t value = s0 + s1;

  s1 ^= s0;
  r->state[0] = rotl(s0, 55) ^ s1 ^ (s1 << 14);
  r->state[1] = rotl(s1, 36);

  return value;
}

void rng_jump(rng *r)
{
  static const uint64_t j[] = {0xbeac0467eba5facb, 0xd86b048b86aa9922};

  uint64_t s0 = 0, s1 = 0;
  for (int i = 0; i < 2; i++) {
    for (int b = 0; b < 64; b++) {
      if (j[i] & (uint64_t)1 << b) {
        s0 ^= r->state[0];
        s1 ^= r->state[1];
      }
      rng_next(r);
    }
  }

  r->state[0] = s0;
  r->state[1] = s1;
}

