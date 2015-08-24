/*
 * utils.c
 *
 *  Created on: Aug 20, 2015
 *      Author: usmanm
 */
#include <math.h>

#include "suites.h"

float8
uniform(void)
{
	return 1.0 * (rand() % DIST_MAX);
}

/*
 * From: http://phoxis.org/2013/05/04/generating-random-numbers-from-normal-distribution-in-c/
 */
float8
gaussian(void)
{
	float8 U1, U2, W, mult;
	static float8 X1, X2;
	static int call = 0;
	float8 mu = 0;
	float8 sigma = 1;

	if (call == 1)
	{
		call = !call;
		return (mu + sigma * (float8) X2);
	}

	do
	{
		U1 = -1 + ((float8) rand () / RAND_MAX) * 2;
		U2 = -1 + ((float8) rand () / RAND_MAX) * 2;
		W = pow (U1, 2) + pow (U2, 2);
	}
	while (W >= 1 || W == 0);

	mult = sqrt ((-2 * log (W)) / W);
	X1 = U1 * mult;
	X2 = U2 * mult;

	call = !call;

	return (rand() % DIST_MAX) * (mu + sigma * (float8) X1);
}
