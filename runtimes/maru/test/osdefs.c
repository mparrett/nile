/* Prints the sizes of the pthread types from the C headers, on the line
   test/osdefs.k prints. Check nile_osdefs compares the two. */

#include <stdio.h>
#include <pthread.h>

int main(void) {
  printf("%ld %ld %ld\n",
	 (long)sizeof (pthread_mutex_t), (long)sizeof (pthread_cond_t), (long)sizeof (pthread_t));
  return 0;
}
