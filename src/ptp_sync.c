/**
 *    author:     UncP
 *    date:    2018-09-05
 *    license:    BSD-3
**/

/**
 *   point to point synchronization pseudo code
 *
 *   sent-first,  sent-last  = false
 *   their-first, their-last = NULL
 *
 *   initialize my-first, my-last
 *
 *   while ¬ ∧ {their-first, their-last, sent-first, sent-last}
 *       if my-first ∧ ¬sent-first
 *           SEND-FIRST(i − 1, d, my-first)
 *           sent-first = true
 *
 *       if my-last ∧ ¬sent-last
 *           SEND-LAST(i + 1, d, my-last)
 *           sent-last = true
 *
 *       if ¬their-first
 *           their-first = TRY-RECV-FIRST(i + 1, d)
 *
 *       if their-first ∧ ¬my-first
 *           my-first = their-first
 *
 *       if ¬their-last
 *           their-last = TRY-RECV-LAST(i − 1, d)
 *
 *       if their-last ∧ ¬my-last
 *           my-last = their-last
 *
**/

