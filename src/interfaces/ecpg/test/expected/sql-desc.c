/* Processed by ecpg (regression mode) */
/* These include files are added by the preprocessor */
#include <ecpglib.h>
#include <ecpgerrno.h>
#include <sqlca.h>
/* End of automatic include section */
#define ECPGdebug(X,Y) ECPGdebug((X)+100,(Y))

#line 1 "desc.pgc"

#line 1 "regression.h"






#line 1 "desc.pgc"

/* exec sql whenever sqlerror  sqlprint ; */
#line 2 "desc.pgc"


int
main(void)
{
	/* exec sql begin declare section */
	   
	   
	   

	   
	      
	      
	   
	  
	   
	
#line 8 "desc.pgc"
 char * stmt1 = "INSERT INTO test1 VALUES ($1, $2)" ;
 
#line 9 "desc.pgc"
 char * stmt2 = "SELECT * from test1 where a = $1 and b = $2" ;
 
#line 10 "desc.pgc"
 char * stmt3 = "SELECT * from test1 where :var = a" ;
 
#line 12 "desc.pgc"
 int val1 = 1 ;
 
#line 13 "desc.pgc"
 char val2 [ 4 ] = "one" , val2output [] = "AAA" ;
 
#line 14 "desc.pgc"
 int val1output = 2 , val2i = 0 ;
 
#line 15 "desc.pgc"
 int val2null = - 1 ;
 
#line 16 "desc.pgc"
 int ind1 , ind2 ;
 
#line 17 "desc.pgc"
 char desc1 [ 8 ] = "outdesc" ;
/* exec sql end declare section */
#line 18 "desc.pgc"


	ECPGdebug(1, stderr);

	ECPGallocate_desc(__LINE__, "indesc");
#line 22 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();
#line 22 "desc.pgc"

	ECPGallocate_desc(__LINE__, (desc1));
#line 23 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();
#line 23 "desc.pgc"


	{ ECPGset_desc(__LINE__, "indesc", 1,ECPGd_data,
	ECPGt_int,&(val1),(long)1,(long)1,sizeof(int), ECPGd_EODT);

#line 25 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 25 "desc.pgc"

	{ ECPGset_desc(__LINE__, "indesc", 2,ECPGd_data,
	ECPGt_char,(val2),(long)4,(long)1,(4)*sizeof(char), ECPGd_indicator,
	ECPGt_int,&(val2i),(long)1,(long)1,sizeof(int), ECPGd_EODT);

#line 26 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 26 "desc.pgc"


	{ ECPGconnect(__LINE__, 0, "regress1" , NULL, NULL , NULL, 0); 
#line 28 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 28 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "create table test1 ( a int , b text )", ECPGt_EOIT, ECPGt_EORT);
#line 30 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 30 "desc.pgc"

	{ ECPGprepare(__LINE__, NULL, 0, "foo1", stmt1);
#line 31 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 31 "desc.pgc"

	{ ECPGprepare(__LINE__, NULL, 0, "Foo-1", stmt1);
#line 32 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 32 "desc.pgc"

	{ ECPGprepare(__LINE__, NULL, 0, "foo2", stmt2);
#line 33 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 33 "desc.pgc"

	{ ECPGprepare(__LINE__, NULL, 0, "foo3", stmt3);
#line 34 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 34 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_execute, "foo1", 
	ECPGt_descriptor, "indesc", 1L, 1L, 1L, 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, ECPGt_EORT);
#line 36 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 36 "desc.pgc"


	{ ECPGset_desc(__LINE__, "indesc", 1,ECPGd_data,
	ECPGt_const,"2",(long)1,(long)1,strlen("2"), ECPGd_EODT);

#line 38 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 38 "desc.pgc"

	{ ECPGset_desc(__LINE__, "indesc", 2,ECPGd_data,
	ECPGt_char,(val2),(long)4,(long)1,(4)*sizeof(char), ECPGd_indicator,
	ECPGt_int,&(val2null),(long)1,(long)1,sizeof(int), ECPGd_EODT);

#line 39 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 39 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_execute, "foo1", 
	ECPGt_descriptor, "indesc", 1L, 1L, 1L, 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, ECPGt_EORT);
#line 41 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 41 "desc.pgc"


	{ ECPGset_desc(__LINE__, "indesc", 1,ECPGd_data,
	ECPGt_const,"3",(long)1,(long)1,strlen("3"), ECPGd_EODT);

#line 43 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 43 "desc.pgc"

	{ ECPGset_desc(__LINE__, "indesc", 2,ECPGd_data,
	ECPGt_const,"this is a long test",(long)19,(long)1,strlen("this is a long test"), ECPGd_indicator,
	ECPGt_int,&(val1),(long)1,(long)1,sizeof(int), ECPGd_EODT);

#line 44 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 44 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_execute, "Foo-1", 
	ECPGt_descriptor, "indesc", 1L, 1L, 1L, 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, ECPGt_EORT);
#line 46 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 46 "desc.pgc"


	{ ECPGdeallocate(__LINE__, 0, NULL, "Foo-1");
#line 48 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 48 "desc.pgc"


	{ ECPGset_desc(__LINE__, "indesc", 1,ECPGd_data,
	ECPGt_int,&(val1),(long)1,(long)1,sizeof(int), ECPGd_EODT);

#line 50 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 50 "desc.pgc"

	{ ECPGset_desc(__LINE__, "indesc", 2,ECPGd_data,
	ECPGt_char,(val2),(long)4,(long)1,(4)*sizeof(char), ECPGd_indicator,
	ECPGt_int,&(val2i),(long)1,(long)1,sizeof(int), ECPGd_EODT);

#line 51 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 51 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_execute, "foo2", 
	ECPGt_descriptor, "indesc", 1L, 1L, 1L, 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, 
	ECPGt_descriptor, (desc1), 1L, 1L, 1L, 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EORT);
#line 53 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 53 "desc.pgc"


	{ ECPGget_desc(__LINE__, (desc1), 1,ECPGd_data,
	ECPGt_char,(val2output),(long)sizeof("AAA"),(long)1,(sizeof("AAA"))*sizeof(char), ECPGd_EODT);

#line 55 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 55 "desc.pgc"

	printf("output = %s\n", val2output);

	/* declare c1 cursor for $1 */
#line 58 "desc.pgc"

	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "declare c1 cursor for $1", 
	ECPGt_char_variable,(ECPGprepared_statement(NULL, "foo2", __LINE__)),(long)1,(long)1,(1)*sizeof(char), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, 
	ECPGt_descriptor, "indesc", 1L, 1L, 1L, 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, ECPGt_EORT);
#line 59 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 59 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "fetch next from c1", ECPGt_EOIT, 
	ECPGt_int,&(val1output),(long)1,(long)1,sizeof(int), 
	ECPGt_int,&(ind1),(long)1,(long)1,sizeof(int), 
	ECPGt_char,(val2output),(long)sizeof("AAA"),(long)1,(sizeof("AAA"))*sizeof(char), 
	ECPGt_int,&(ind2),(long)1,(long)1,sizeof(int), ECPGt_EORT);
#line 61 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 61 "desc.pgc"

	printf("val1=%d (ind1: %d) val2=%s (ind2: %d)\n",
		val1output, ind1, val2output, ind2);

	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "close c1", ECPGt_EOIT, ECPGt_EORT);
#line 65 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 65 "desc.pgc"


	{ ECPGset_desc_header(__LINE__, "indesc", (int)(1));

#line 67 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 67 "desc.pgc"

	{ ECPGset_desc(__LINE__, "indesc", 1,ECPGd_data,
	ECPGt_const,"2",(long)1,(long)1,strlen("2"), ECPGd_EODT);

#line 68 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 68 "desc.pgc"


	/* declare c2 cursor for $1 */
#line 70 "desc.pgc"

	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "declare c2 cursor for $1", 
	ECPGt_char_variable,(ECPGprepared_statement(NULL, "foo3", __LINE__)),(long)1,(long)1,(1)*sizeof(char), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, 
	ECPGt_descriptor, "indesc", 1L, 1L, 1L, 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, ECPGt_EORT);
#line 71 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 71 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "fetch next from c2", ECPGt_EOIT, 
	ECPGt_int,&(val1output),(long)1,(long)1,sizeof(int), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, 
	ECPGt_char,(val2output),(long)sizeof("AAA"),(long)1,(sizeof("AAA"))*sizeof(char), 
	ECPGt_int,&(val2i),(long)1,(long)1,sizeof(int), ECPGt_EORT);
#line 73 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 73 "desc.pgc"

	printf("val1=%d val2=%s\n", val1output, val2i ? "null" : val2output);

	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "close c2", ECPGt_EOIT, ECPGt_EORT);
#line 76 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 76 "desc.pgc"


	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "select * from test1 where a = 3", ECPGt_EOIT, 
	ECPGt_int,&(val1output),(long)1,(long)1,sizeof(int), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, 
	ECPGt_char,(val2output),(long)sizeof("AAA"),(long)1,(sizeof("AAA"))*sizeof(char), 
	ECPGt_int,&(val2i),(long)1,(long)1,sizeof(int), ECPGt_EORT);
#line 78 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 78 "desc.pgc"

	printf("val1=%d val2=%c%c%c%c warn=%c truncate=%d\n", val1output, val2output[0], val2output[1], val2output[2], val2output[3], sqlca.sqlwarn[0], val2i);

	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "drop table test1", ECPGt_EOIT, ECPGt_EORT);
#line 81 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 81 "desc.pgc"

	{ ECPGdeallocate_all(__LINE__, 0, NULL);
#line 82 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 82 "desc.pgc"

	{ ECPGdisconnect(__LINE__, "CURRENT");
#line 83 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 83 "desc.pgc"


	ECPGdeallocate_desc(__LINE__, "indesc");
#line 85 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();
#line 85 "desc.pgc"

	ECPGdeallocate_desc(__LINE__, (desc1));
#line 86 "desc.pgc"

if (sqlca.sqlcode < 0) sqlprint();
#line 86 "desc.pgc"


	return 0;
}
