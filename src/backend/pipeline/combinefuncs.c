/*-------------------------------------------------------------------------
 *
 * combinefuncs.c
 *	  Functions for combining function results with other results
 *
 * Note: most of the functionality in here is taken from Postgres-XC:
 *
 * 	https://github.com/postgres-xc/postgres-xc
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/combinefuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

/* not sure what the following should be, but better to make it over-sufficient */
#define MAXFLOATWIDTH	64
#define MAXDOUBLEWIDTH	128

typedef struct Int8TransTypeData
{
	int64		count;
	int64		sum;
} Int8TransTypeData;

/*
 * check to see if a float4/8 val has underflowed or overflowed
 */
#define CHECKFLOATVAL(val, inf_is_valid, zero_is_valid)			\
do {															\
	if (isinf(val) && !(inf_is_valid))							\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		  errmsg("value out of range: overflow")));				\
																\
	if ((val) == 0.0 && !(zero_is_valid))						\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		 errmsg("value out of range: underflow")));				\
} while(0)

/*
 *		=========================
 *		FLOAT AGGREGATE OPERATORS
 *		=========================
 *
 *		float8_accum		- accumulate for AVG(), variance aggregates, etc.
 *		float4_accum		- same, but input data is float4
 *		float8_avg			- produce final result for float AVG()
 *		float8_var_samp		- produce final result for float VAR_SAMP()
 *		float8_var_pop		- produce final result for float VAR_POP()
 *		float8_stddev_samp	- produce final result for float STDDEV_SAMP()
 *		float8_stddev_pop	- produce final result for float STDDEV_POP()
 *
 * The transition datatype for all these aggregates is a 3-element array
 * of float8, holding the values N, sum(X), sum(X*X) in that order.
 *
 * Note that we represent N as a float to avoid having to build a special
 * datatype.  Given a reasonable floating-point implementation, there should
 * be no accuracy loss unless N exceeds 2 ^ 52 or so (by which time the
 * user will have doubtless lost interest anyway...)
 */

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
	return (float8 *) ARR_DATA_PTR(transarray);
}

/*
 * similar to int8_sum, except that the result is casted into int8
 */
Datum
int8_sum_to_int8(PG_FUNCTION_ARGS)
{
	Datum	result_num;
	Datum	numeric_arg;

	/* if both arguments are null, the result is null */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();
	/* if either of them is null, the other is the result */
	if (PG_ARGISNULL(0))
		PG_RETURN_DATUM(PG_GETARG_DATUM(1));
	if (PG_ARGISNULL(1))
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));

	/*
	 * convert the first argument to numeric (second one is converted into
	 * numeric)
	 * add both the arguments using int8_sum
	 * convert the result into int8 using numeric_int8
	 */
	numeric_arg = DirectFunctionCall1(int8_numeric, PG_GETARG_DATUM(0));
	result_num = DirectFunctionCall2(int8_sum, numeric_arg, PG_GETARG_DATUM(1));
	PG_RETURN_DATUM(DirectFunctionCall1(numeric_int8, result_num));
}

Datum
float8_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *collectarray = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(1);
	float8	   *collectvalues;
	float8	   *transvalues;
	float8		N,
				sumX,
				sumX2;

	collectvalues = check_float8_array(collectarray, "float8_collect", 3);
	transvalues = check_float8_array(transarray, "float8_collect", 3);
	N = collectvalues[0];
	sumX = collectvalues[1];
	sumX2 = collectvalues[2];

	N += transvalues[0];
	sumX += transvalues[1];
	CHECKFLOATVAL(sumX, isinf(collectvalues[1]) || isinf(transvalues[1]), true);
	sumX2 += transvalues[2];
	CHECKFLOATVAL(sumX2, isinf(collectvalues[2]) || isinf(transvalues[2]), true);

	/*
	 * If we're invoked by nodeAgg, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (fcinfo->context &&
		(IsA(fcinfo->context, AggState) ||
		 IsA(fcinfo->context, WindowAggState)))
	{
		collectvalues[0] = N;
		collectvalues[1] = sumX;
		collectvalues[2] = sumX2;

		PG_RETURN_ARRAYTYPE_P(collectarray);
	}
	else
	{
		Datum		collectdatums[3];
		ArrayType  *result;

		collectdatums[0] = Float8GetDatumFast(N);
		collectdatums[1] = Float8GetDatumFast(sumX);
		collectdatums[2] = Float8GetDatumFast(sumX2);

		result = construct_array(collectdatums, 3,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

Datum
float8_regr_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *collectarray = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(1);
	float8	   *collectvalues;
	float8	   *transvalues;
	float8		N,
				sumX,
				sumX2,
				sumY,
				sumY2,
				sumXY;

	collectvalues = check_float8_array(collectarray, "float8_accum", 6);
	transvalues = check_float8_array(transarray, "float8_accum", 6);
	N = collectvalues[0];
	sumX = collectvalues[1];
	sumX2 = collectvalues[2];
	sumY = collectvalues[3];
	sumY2 = collectvalues[4];
	sumXY = collectvalues[5];

	N += transvalues[0];
	sumX += transvalues[1];
	CHECKFLOATVAL(sumX, isinf(collectvalues[1]) || isinf(transvalues[1]), true);
	sumX2 += transvalues[2];
	CHECKFLOATVAL(sumX2, isinf(collectvalues[2]) || isinf(transvalues[2]), true);
	sumY += transvalues[3];
	CHECKFLOATVAL(sumY, isinf(collectvalues[3]) || isinf(transvalues[3]), true);
	sumY2 += transvalues[4];
	CHECKFLOATVAL(sumY2, isinf(collectvalues[4]) || isinf(transvalues[4]), true);
	sumXY += transvalues[5];
	CHECKFLOATVAL(sumXY, isinf(collectvalues[5]) || isinf(transvalues[5]), true);

	/*
	 * If we're invoked by nodeAgg, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (fcinfo->context &&
		(IsA(fcinfo->context, AggState) ||
		 IsA(fcinfo->context, WindowAggState)))
	{
		collectvalues[0] = N;
		collectvalues[1] = sumX;
		collectvalues[2] = sumX2;
		collectvalues[3] = sumY;
		collectvalues[4] = sumY2;
		collectvalues[5] = sumXY;

		PG_RETURN_ARRAYTYPE_P(collectarray);
	}
	else
	{
		Datum		collectdatums[6];
		ArrayType  *result;

		collectdatums[0] = Float8GetDatumFast(N);
		collectdatums[1] = Float8GetDatumFast(sumX);
		collectdatums[2] = Float8GetDatumFast(sumX2);
		collectdatums[3] = Float8GetDatumFast(sumY);
		collectdatums[4] = Float8GetDatumFast(sumY2);
		collectdatums[5] = Float8GetDatumFast(sumXY);

		result = construct_array(collectdatums, 6,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

Datum
numeric_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *collectarray = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *collectdatums;
	Datum	   *transdatums;
	int			ndatums;
	Datum		N,
				sumX,
				sumX2;

	/* We assume the input is array of numeric */
	deconstruct_array(collectarray,
					  NUMERICOID, -1, false, 'i',
					  &collectdatums, NULL, &ndatums);
	if (ndatums != 3)
		elog(ERROR, "expected 3-element numeric array");
	N = collectdatums[0];
	sumX = collectdatums[1];
	sumX2 = collectdatums[2];

	/* We assume the input is array of numeric */
	deconstruct_array(transarray,
					  NUMERICOID, -1, false, 'i',
					  &transdatums, NULL, &ndatums);
	if (ndatums != 3)
		elog(ERROR, "expected 3-element numeric array");

	N = DirectFunctionCall2(numeric_add, N, transdatums[0]);
	sumX = DirectFunctionCall2(numeric_add, sumX, transdatums[1]);
	sumX2 = DirectFunctionCall2(numeric_add, sumX2, transdatums[2]);

	collectdatums[0] = N;
	collectdatums[1] = sumX;
	collectdatums[2] = sumX2;

	PG_RETURN_ARRAYTYPE_P(construct_array(collectdatums, 3,
							 NUMERICOID, -1, false, 'i'));
}

Datum
int8_avg_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *collectarray;
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(1);
	Int8TransTypeData *collectdata;
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked by nodeAgg, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (fcinfo->context &&
		(IsA(fcinfo->context, AggState) ||
		 IsA(fcinfo->context, WindowAggState)))
		collectarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		collectarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(collectarray) ||
		ARR_SIZE(collectarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");
	collectdata = (Int8TransTypeData *) ARR_DATA_PTR(collectarray);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");
	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);

	collectdata->count += transdata->count;
	collectdata->sum += transdata->sum;

	PG_RETURN_ARRAYTYPE_P(collectarray);
}

Datum
interval_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *collectarray = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *collectdatums;
	Datum	   *transdatums;
	int			ndatums;
	Interval	sumX1,
				N1,
				sumX2,
				N2;
	Interval   *newsum;
	ArrayType  *result;

	deconstruct_array(collectarray,
					  INTERVALOID, sizeof(Interval), false, 'd',
					  &collectdatums, NULL, &ndatums);
	if (ndatums != 2)
		elog(ERROR, "expected 2-element interval array");

	deconstruct_array(transarray,
					  INTERVALOID, sizeof(Interval), false, 'd',
					  &transdatums, NULL, &ndatums);
	if (ndatums != 2)
		elog(ERROR, "expected 2-element interval array");

	/*
	 * XXX memcpy, instead of just extracting a pointer, to work around buggy
	 * array code: it won't ensure proper alignment of Interval objects on
	 * machines where double requires 8-byte alignment. That should be fixed,
	 * but in the meantime...
	 *
	 * Note: must use DatumGetPointer here, not DatumGetIntervalP, else some
	 * compilers optimize into double-aligned load/store anyway.
	 */
	memcpy((void *) &sumX1, DatumGetPointer(collectdatums[0]), sizeof(Interval));
	memcpy((void *) &N1, DatumGetPointer(collectdatums[1]), sizeof(Interval));
	memcpy((void *) &sumX2, DatumGetPointer(transdatums[0]), sizeof(Interval));
	memcpy((void *) &N2, DatumGetPointer(transdatums[1]), sizeof(Interval));

	newsum = DatumGetIntervalP(DirectFunctionCall2(interval_pl,
												   IntervalPGetDatum(&sumX1),
												   IntervalPGetDatum(&sumX2)));
	N1.time += N2.time;

	collectdatums[0] = IntervalPGetDatum(newsum);
	collectdatums[1] = IntervalPGetDatum(&N1);

	result = construct_array(collectdatums, 2,
							 INTERVALOID, sizeof(Interval), false, 'd');

	PG_RETURN_ARRAYTYPE_P(result);
}
