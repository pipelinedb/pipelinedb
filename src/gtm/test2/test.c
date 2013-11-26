#include <stdio.h>

int
main(void)
{
  char *ptr[1024];
  int i;

  for (i=0 ; i<1024 ; i++)
  {
    fprintf(stderr, "ptr[%d] = %p\n", i, ptr[i]);
  }

  return 0;
}
