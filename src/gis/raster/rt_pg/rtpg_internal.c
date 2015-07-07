/*
 *
 * WKTRaster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2011-2013 Regents of the University of California
 *   <bkpark@ucdavis.edu>
 * Copyright (C) 2010-2011 Jorge Arevalo <jorge.arevalo@deimos-space.com>
 * Copyright (C) 2010-2011 David Zwarg <dzwarg@azavea.com>
 * Copyright (C) 2009-2011 Pierre Racine <pierre.racine@sbf.ulaval.ca>
 * Copyright (C) 2009-2011 Mateusz Loskot <mateusz@loskot.net>
 * Copyright (C) 2008-2009 Sandro Santilli <strk@keybit.net>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 */

#include <ctype.h> /* for isspace */
#include <postgres.h> /* for palloc */
#include <executor/spi.h>

#include "rtpg_internal.h"

/* string replacement function taken from
 * http://ubuntuforums.org/showthread.php?s=aa6f015109fd7e4c7e30d2fd8b717497&t=141670&page=3
 */
/* ---------------------------------------------------------------------------
  Name       : replace - Search & replace a substring by another one.
  Creation   : Thierry Husson, Sept 2010
  Parameters :
      str    : Big string where we search
      oldstr : Substring we are looking for
      newstr : Substring we want to replace with
      count  : Optional pointer to int (input / output value). NULL to ignore.
               Input:  Maximum replacements to be done. NULL or < 1 to do all.
               Output: Number of replacements done or -1 if not enough memory.
  Returns    : Pointer to the new string or NULL if error.
  Notes      :
     - Case sensitive - Otherwise, replace functions "strstr" by "strcasestr"
     - Always allocates memory for the result.
--------------------------------------------------------------------------- */
char*
rtpg_strreplace(
	const char *str,
	const char *oldstr, const char *newstr,
	int *count
) {
	const char *tmp = str;
	char *result;
	int found = 0;
	int length, reslen;
	int oldlen = strlen(oldstr);
	int newlen = strlen(newstr);
	int limit = (count != NULL && *count > 0) ? *count : -1;

	tmp = str;
	while ((tmp = strstr(tmp, oldstr)) != NULL && found != limit)
		found++, tmp += oldlen;

	length = strlen(str) + found * (newlen - oldlen);
	if ((result = (char *) palloc(length + 1)) == NULL) {
		fprintf(stderr, "Not enough memory\n");
		found = -1;
	}
	else {
		tmp = str;
		limit = found; /* Countdown */
		reslen = 0; /* length of current result */

		/* Replace each old string found with new string  */
		while ((limit-- > 0) && (tmp = strstr(tmp, oldstr)) != NULL) {
			length = (tmp - str); /* Number of chars to keep intouched */
			strncpy(result + reslen, str, length); /* Original part keeped */
			strcpy(result + (reslen += length), newstr); /* Insert new string */

			reslen += newlen;
			tmp += oldlen;
			str = tmp;
		}
		strcpy(result + reslen, str); /* Copies last part and ending null char */
	}

	if (count != NULL) *count = found;
	return result;
}

char *
rtpg_strtoupper(char * str) {
	int j;

	for (j = strlen(str) - 1; j >= 0; j--)
		str[j] = toupper(str[j]);

	return str;
}

char*
rtpg_chartrim(const char *input, char *remove) {
	char *rtn = NULL;
	char *ptr = NULL;
	uint32_t offset = 0;

	if (!input)
		return NULL;
	else if (!*input)
		return (char *) input;

	/* trim left */
	while (strchr(remove, *input) != NULL)
		input++;

	/* trim right */
	ptr = ((char *) input) + strlen(input);
	while (strchr(remove, *--ptr) != NULL)
		offset++;

	rtn = palloc(sizeof(char) * (strlen(input) - offset + 1));
	if (rtn == NULL) {
		fprintf(stderr, "Not enough memory\n");
		return NULL;
	}
	strncpy(rtn, input, strlen(input) - offset);
	rtn[strlen(input) - offset] = '\0';

	return rtn;
}

/* split a string based on a delimiter */
char**
rtpg_strsplit(const char *str, const char *delimiter, int *n) {
	char *tmp = NULL;
	char **rtn = NULL;
	char *token = NULL;

	*n = 0;
	if (!str)
		return NULL;

	/* copy str to tmp as strtok will mangle the string */
	tmp = palloc(sizeof(char) * (strlen(str) + 1));
	if (NULL == tmp) {
		fprintf(stderr, "Not enough memory\n");
		return NULL;
	}
	strcpy(tmp, str);

	if (!strlen(tmp) || !delimiter || !strlen(delimiter)) {
		*n = 1;
		rtn = (char **) palloc(*n * sizeof(char *));
		if (NULL == rtn) {
			fprintf(stderr, "Not enough memory\n");
			return NULL;
		}
		rtn[0] = (char *) palloc(sizeof(char) * (strlen(tmp) + 1));
		if (NULL == rtn[0]) {
			fprintf(stderr, "Not enough memory\n");
			return NULL;
		}
		strcpy(rtn[0], tmp);
		pfree(tmp);
		return rtn;
	}

	token = strtok(tmp, delimiter);
	while (token != NULL) {
		if (*n < 1) {
			rtn = (char **) palloc(sizeof(char *));
		}
		else {
			rtn = (char **) repalloc(rtn, (*n + 1) * sizeof(char *));
		}
		if (NULL == rtn) {
			fprintf(stderr, "Not enough memory\n");
			return NULL;
		}

		rtn[*n] = NULL;
		rtn[*n] = (char *) palloc(sizeof(char) * (strlen(token) + 1));
		if (NULL == rtn[*n]) {
			fprintf(stderr, "Not enough memory\n");
			return NULL;
		}

		strcpy(rtn[*n], token);
		*n = *n + 1;

		token = strtok(NULL, delimiter);
	}

	pfree(tmp);
	return rtn;
}

char *
rtpg_removespaces(char *str) {
	char *rtn;
	char *tmp;

	rtn = rtpg_strreplace(str, " ", "", NULL);

	tmp = rtpg_strreplace(rtn, "\n", "", NULL);
	pfree(rtn);
	rtn = rtpg_strreplace(tmp, "\t", "", NULL);
	pfree(tmp);
	tmp = rtpg_strreplace(rtn, "\f", "", NULL);
	pfree(rtn);
	rtn = rtpg_strreplace(tmp, "\r", "", NULL);
	pfree(tmp);

	return rtn;
}

char*
rtpg_trim(const char *input) {
	char *rtn;
	char *ptr;
	uint32_t offset = 0;
	int inputlen = 0;

	if (!input)
		return NULL;
	else if (!*input)
		return (char *) input;

	/* trim left */
	while (isspace(*input) && *input != '\0')
		input++;

	/* trim right */
	inputlen = strlen(input);
	if (inputlen) {
		ptr = ((char *) input) + inputlen;
		while (isspace(*--ptr))
			offset++;
	}

	rtn = palloc(sizeof(char) * (inputlen - offset + 1));
	if (rtn == NULL) {
		fprintf(stderr, "Not enough memory\n");
		return NULL;
	}
	strncpy(rtn, input, inputlen - offset);
	rtn[inputlen - offset] = '\0';

	return rtn;
}

/*
 * reverse string search function from
 * http://stackoverflow.com/a/1634398
 */
char *
rtpg_strrstr(const char *s1, const char *s2) {
	int s1len = strlen(s1);
	int s2len = strlen(s2);
	char *s;

	if (s2len > s1len)
		return NULL;

	s = (char *) (s1 + s1len - s2len);
	for (; s >= s1; --s)
		if (strncmp(s, s2, s2len) == 0)
			return s;

	return NULL;
}

char*
rtpg_getSR(int srid) {
	int i = 0;
	int len = 0;
	char *sql = NULL;
	int spi_result;
	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple;
	char *tmp = NULL;
	char *srs = NULL;

/*
SELECT
	CASE
		WHEN (upper(auth_name) = 'EPSG' OR upper(auth_name) = 'EPSGA') AND length(COALESCE(auth_srid::text, '')) > 0
			THEN upper(auth_name) || ':' || auth_srid
		WHEN length(COALESCE(auth_name, '') || COALESCE(auth_srid::text, '')) > 0
			THEN COALESCE(auth_name, '') || COALESCE(auth_srid::text, '')
		ELSE ''
	END,
	proj4text,
	srtext
FROM spatial_ref_sys
WHERE srid = X
LIMIT 1
*/

	len = sizeof(char) * (strlen("SELECT CASE WHEN (upper(auth_name) = 'EPSG' OR upper(auth_name) = 'EPSGA') AND length(COALESCE(auth_srid::text, '')) > 0 THEN upper(auth_name) || ':' || auth_srid WHEN length(COALESCE(auth_name, '') || COALESCE(auth_srid::text, '')) > 0 THEN COALESCE(auth_name, '') || COALESCE(auth_srid::text, '') ELSE '' END, proj4text, srtext FROM spatial_ref_sys WHERE srid =  LIMIT 1") + MAX_INT_CHARLEN + 1);
	sql = (char *) palloc(len);
	if (NULL == sql) {
		elog(ERROR, "rtpg_getSR: Could not allocate memory for sql\n");
		return NULL;
	}

	spi_result = SPI_connect();
	if (spi_result != SPI_OK_CONNECT) {
		pfree(sql);
		elog(ERROR, "rtpg_getSR: Could not connect to database using SPI\n");
		return NULL;
	}

	/* execute query */
	snprintf(sql, len, "SELECT CASE WHEN (upper(auth_name) = 'EPSG' OR upper(auth_name) = 'EPSGA') AND length(COALESCE(auth_srid::text, '')) > 0 THEN upper(auth_name) || ':' || auth_srid WHEN length(COALESCE(auth_name, '') || COALESCE(auth_srid::text, '')) > 0 THEN COALESCE(auth_name, '') || COALESCE(auth_srid::text, '') ELSE '' END, proj4text, srtext FROM spatial_ref_sys WHERE srid = %d LIMIT 1", srid);
	POSTGIS_RT_DEBUGF(4, "SRS query: %s", sql);
	spi_result = SPI_execute(sql, TRUE, 0);
	SPI_pfree(sql);
	if (spi_result != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {
		if (SPI_tuptable) SPI_freetuptable(tuptable);
		SPI_finish();
		elog(ERROR, "rtpg_getSR: Cannot find SRID (%d) in spatial_ref_sys", srid);
		return NULL;
	}

	tupdesc = SPI_tuptable->tupdesc;
	tuptable = SPI_tuptable;
	tuple = tuptable->vals[0];

	/* which column to use? */
	for (i = 1; i < 4; i++) {
		tmp = SPI_getvalue(tuple, tupdesc, i);

		/* value AND GDAL supports this SR */
		if (
			SPI_result != SPI_ERROR_NOATTRIBUTE &&
			SPI_result != SPI_ERROR_NOOUTFUNC &&
			tmp != NULL &&
			strlen(tmp) &&
			rt_util_gdal_supported_sr(tmp)
		) {
			POSTGIS_RT_DEBUGF(4, "Value for column %d is %s", i, tmp);

			len = strlen(tmp) + 1;
			srs = SPI_palloc(sizeof(char) * len);
			if (NULL == srs) {
				pfree(tmp);
				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_finish();
				elog(ERROR, "rtpg_getSR: Could not allocate memory for spatial reference text\n");
				return NULL;
			}
			strncpy(srs, tmp, len);
			pfree(tmp);

			break;
		}

		if (tmp != NULL)
			pfree(tmp);
		continue;
	}

	if (SPI_tuptable) SPI_freetuptable(tuptable);
	SPI_finish();

	/* unable to get SR info */
	if (srs == NULL) {
		if (SPI_tuptable) SPI_freetuptable(tuptable);
		SPI_finish();
		elog(ERROR, "rtpg_getSR: Could not find a viable spatial reference for SRID (%d)", srid);
		return NULL;
	}

	return srs;
}
