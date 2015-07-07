#include "wkbtest.h"

/* example set-up

create table wkbreader_test (interesting bool,comments varchar(100), the_geom geometry);
insert into wkbreader_test values ( false, 'simple point', 'POINT( 10 10)' );
insert into wkbreader_test values ( false, 'simple line' , 'LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0)');
insert into wkbreader_test values( true, 'polygon w/ hole','POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 7 5, 7 7 , 5 7 , 5 5))');


dont forget to set the port, dbname, host, and user in the
environment so the postgres connection will work

Also, change the "declare cursor" command so it returns the columns you want, and converts the geometry
into wkb.

*/


void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

void dump_bytes( char *a, int numb)
{
	int	t;

	for (t=0; t<numb; t++)
	{
		printf("	+ Byte #%i has value %i (%x)\n",t,a[t],a[t]);
	}
}


// need to find the OID coresponding to the GEOMETRY type
//
// select OID from pg_type where typname = 'wkb';

int	find_WKB_typeid(PGconn 	*conn)
{
	PGresult *dbresult;
	char		*num;

	if (PQstatus(conn) == CONNECTION_BAD)
	{
		fprintf(stderr, "no connection to db\n");
		exit_nicely(conn);
	}

	dbresult = PQexec(conn, "select OID from pg_type where typname = 'bytea';");

	if (PQresultStatus(dbresult) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "couldnt execute query to find oid of geometry type");
		exit_nicely(conn);	//command failed
	}

	if ( PQntuples(dbresult) != 1)
	{
		fprintf(stderr, "query to find oid of geometry didnt return 1 row!");
		exit_nicely(conn);
	}

	num =  PQgetvalue(dbresult, 0, 0);  // first row, first field

	PQclear(dbresult);

	return ( atoi(num) );
}


main()
{
	char       *pghost,
	*pgport,
	*pgoptions,
	*pgtty;
	char       *dbName;
	int         nFields;
	int         row,
	field;
	PGconn     *conn;
	PGresult   *res;
	int	junk;
	char	*field_name;
	int	field_type;
	int	WKB_OID;
	char		conn_string[255];

	bool		*bool_val;
	int		*int_val;
	float		*float_val;
	double	*double_val;
	char		*char_val;
	char		*wkb_val;
	char		*table_name = "wkbreader_test";
	char		query_str[1000];

	/* make a connection to the database */
	conn = PQconnectdb("");

	/*
	* check to see that the backend connection was successfully made
	*/
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		fprintf(stderr, "%s", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	//what is the geometry type's OID #?
	WKB_OID = find_WKB_typeid(conn);


	/* start a transaction block */
	res = PQexec(conn, "BEGIN");
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "%s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	/*
	 * should PQclear PGresult whenever it is no longer needed to avoid
	 * memory leaks
	 */
	PQclear(res);

	/*
	 * fetch rows from the pg_database, the system catalog of
	 * databases
	 */
	sprintf(query_str, "DECLARE mycursor BINARY CURSOR FOR select text(num), asbinary(the_geom,'ndr') as wkb from %s", table_name);

	printf(query_str);
	printf("\n");

	res = PQexec(conn, query_str);

	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "DECLARE CURSOR command failed\n");
		fprintf(stderr, "%s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);

	res = PQexec(conn, "FETCH ALL in mycursor");
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "FETCH ALL command didn't return tuples properly\n");
		fprintf(stderr, "%s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	for (row=0; row< PQntuples(res); row++)
	{
		printf("------------------------------row %i --------------------------\n",row);
		//not so efficient, but...
		for (field =0 ; field< PQnfields(res); field++)
		{
			field_type =PQftype(res,field);
			field_name = PQfname(res, field);

			//we just handle a few of the popular type since this is just an example

			if (field_type ==16)// bool
			{
				bool_val = (bool *) PQgetvalue(res, row, field);
				if (*bool_val)
					printf("%s: TRUE\n",field_name);
				else
					printf("%s: FALSE\n",field_name);
			}
			else if (field_type ==23 )//int4 (int)
			{
				int_val = (int *) PQgetvalue(res, row, field);
				printf("%s: %i\n",field_name,*int_val);
			}
			else if (field_type ==700 )//float4 (float)
			{
				float_val = (float *) PQgetvalue(res, row, field);
				printf("%s: %g\n",field_name,*float_val);
			}
			else if (field_type ==701 )//float8 (double)
			{
				double_val = (double *) PQgetvalue(res, row, field);
				printf("%s: %g\n",field_name,*double_val);
			}
			else if ( (field_type ==1043 ) || (field_type==25) )//varchar
			{
				char_val = (char *) PQgetvalue(res, row, field);
				printf("%s: %s\n",field_name,char_val);
			}
			else if (field_type == WKB_OID ) //wkb
			{
				char_val = (char *) PQgetvalue(res, row, field);
				printf("%s: ", field_name);
				// skip 4 bytes varlena size
				decode_wkb(char_val, &junk);
				printf("\n");
			}

		}
	}

	PQclear(res);

	/* close the cursor */
	res = PQexec(conn, "CLOSE mycursor");
	PQclear(res);

	/* commit the transaction */
	res = PQexec(conn, "COMMIT");
	PQclear(res);

	/* close the connection to the database and cleanup */
	PQfinish(conn);

	return 0;
}
