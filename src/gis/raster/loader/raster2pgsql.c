/*
 *
 * PostGIS raster loader
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright 2001-2003 Refractions Research Inc.
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright 2009 Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 * Copyright (C) 2011 Regents of the University of California
 *   <bkpark@ucdavis.edu>
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

#include "raster2pgsql.h"
#include "gdal_vrt.h"
#include "ogr_srs_api.h"
#include <assert.h>

static void
loader_rt_error_handler(const char *fmt, va_list ap) {
	static const char *label = "ERROR: ";
	char newfmt[1024] = {0};
	snprintf(newfmt, 1024, "%s%s\n", label, fmt);
	newfmt[1023] = '\0';
	vfprintf(stderr, newfmt, ap);
	va_end(ap);
}

static void
loader_rt_warning_handler(const char *fmt, va_list ap) {
	static const char *label = "WARNING: ";
	char newfmt[1024] = {0};
	snprintf(newfmt, 1024, "%s%s\n", label, fmt);
	newfmt[1023] = '\0';
	vfprintf(stderr, newfmt, ap);
	va_end(ap);
}

static void
loader_rt_info_handler(const char *fmt, va_list ap) {
	static const char *label = "INFO: ";
	char newfmt[1024] = {0};
	snprintf(newfmt, 1024, "%s%s\n", label, fmt);
	newfmt[1023] = '\0';
	vfprintf(stderr, newfmt, ap);
	va_end(ap);
}

void rt_init_allocators(void) {
	rt_set_handlers(
		default_rt_allocator,
		default_rt_reallocator,
		default_rt_deallocator,
		loader_rt_error_handler,
		loader_rt_info_handler,
		loader_rt_warning_handler
	);
}

static void
raster_destroy(rt_raster raster) {
	uint16_t i;
	uint16_t nbands = rt_raster_get_num_bands(raster);
	for (i = 0; i < nbands; i++) {
		rt_band band = rt_raster_get_band(raster, i);
		if (band == NULL) continue;

		if (!rt_band_is_offline(band) && !rt_band_get_ownsdata_flag(band)) {
			void* mem = rt_band_get_data(band);
			if (mem) rtdealloc(mem);
		}
		rt_band_destroy(band);
	}
	rt_raster_destroy(raster);
}

static int
array_range(int min, int max, int step, int **range, int *len) {
	int i = 0;
	int j = 0;

	step = abs(step);
	*len = (abs(max - min) + 1 + (step / 2)) / step;
	*range = rtalloc(sizeof(int) * *len);

	if (min < max) {
		for (i = min, j = 0; i <= max; i += step, j++)
			(*range)[j] = i;
	}
	else if (max < min) {
		if (step > 0) step *= -1;
		for (i = min, j = 0; i >= max; i += step, j++)
			(*range)[j] = i;
	}
	else if (min == max) {
		(*range)[0] = min;
	}
	else {
		*len = 0;
		*range = NULL;
		return 0;
	}

	return 1;
}

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
static char*
strreplace(
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
	if ((result = (char *) rtalloc(length + 1)) == NULL) {
		rterror(_("strreplace: Not enough memory"));
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

static char *
strtolower(char * str) {
	int j;

	for (j = strlen(str) - 1; j >= 0; j--)
		str[j] = tolower(str[j]);

	return str;
}

/* split a string based on a delimiter */
static char**
strsplit(const char *str, const char *delimiter, int *n) {
	char *tmp = NULL;
	char **rtn = NULL;
	char *token = NULL;

	*n = 0;
	if (!str)
		return NULL;

	/* copy str to tmp as strtok will mangle the string */
	tmp = rtalloc(sizeof(char) * (strlen(str) + 1));
	if (NULL == tmp) {
		rterror(_("strsplit: Not enough memory"));
		return NULL;
	}
	strcpy(tmp, str);

	if (!strlen(tmp) || !delimiter || !strlen(delimiter)) {
		*n = 1;
		rtn = (char **) rtalloc(*n * sizeof(char *));
		if (NULL == rtn) {
			rterror(_("strsplit: Not enough memory"));
			return NULL;
		}
		rtn[0] = (char *) rtalloc(sizeof(char) * (strlen(tmp) + 1));
		if (NULL == rtn[0]) {
			rterror(_("strsplit: Not enough memory"));
			return NULL;
		}
		strcpy(rtn[0], tmp);
		rtdealloc(tmp);
		return rtn;
	}

	token = strtok(tmp, delimiter);
	while (token != NULL) {
		if (*n < 1) {
			rtn = (char **) rtalloc(sizeof(char *));
		}
		else {
			rtn = (char **) rtrealloc(rtn, (*n + 1) * sizeof(char *));
		}
		if (NULL == rtn) {
			rterror(_("strsplit: Not enough memory"));
			return NULL;
		}

		rtn[*n] = NULL;
		rtn[*n] = (char *) rtalloc(sizeof(char) * (strlen(token) + 1));
		if (NULL == rtn[*n]) {
			rterror(_("strsplit: Not enough memory"));
			return NULL;
		}

		strcpy(rtn[*n], token);
		*n = *n + 1;

		token = strtok(NULL, delimiter);
	}

	rtdealloc(tmp);
	return rtn;
}

static char*
trim(const char *input) {
	char *rtn;
	char *ptr;
	uint32_t offset = 0;

	if (!input)
		return NULL;
	else if (!*input)
		return (char *) input;

	/* trim left */
	while (isspace(*input))
		input++;

	/* trim right */
	ptr = ((char *) input) + strlen(input);
	while (isspace(*--ptr))
		offset++;

	rtn = rtalloc(sizeof(char) * (strlen(input) - offset + 1));
	if (NULL == rtn) {
		rterror(_("trim: Not enough memory"));
		return NULL;
	}
	strncpy(rtn, input, strlen(input) - offset);
	rtn[strlen(input) - offset] = '\0';

	return rtn;
}

static char*
chartrim(const char *input, char *remove) {
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

	rtn = rtalloc(sizeof(char) * (strlen(input) - offset + 1));
	if (NULL == rtn) {
		rterror(_("chartrim: Not enough memory"));
		return NULL;
	}
	strncpy(rtn, input, strlen(input) - offset);
	rtn[strlen(input) - offset] = '\0';

	return rtn;
}

static void
usage() {
	printf(_("RELEASE: %s GDAL_VERSION=%d (r%d)\n"), POSTGIS_LIB_VERSION, POSTGIS_GDAL_VERSION, POSTGIS_SVN_REVISION);
	printf(_(
		"USAGE: raster2pgsql [<options>] <raster>[ <raster>[ ...]] [[<schema>.]<table>]\n"
		"  Multiple rasters can also be specified using wildcards (*,?).\n"
		"\n"
		"OPTIONS:\n"
	));
	/*
	printf(_(
		"  -s [<from>:]<srid> Set the SRID field. Defaults to %d.\n"
		"     Optionally reprojects from given SRID (cannot be used with -Y).\n"
		"     Raster's metadata will be checked to determine an appropriate SRID.\n"
		"     If a srid of %d is provided (either as from or as target).\n"
	), SRID_UNKNOWN, SRID_UNKNOWN);
	*/
	printf(_(
		"  -s <srid> Set the SRID field. Defaults to %d. If SRID not\n"
		"     provided or is %d, raster's metadata will be checked to\n"
		"     determine an appropriate SRID.\n"
	), SRID_UNKNOWN, SRID_UNKNOWN);
	printf(_(
		"  -b <band> Index (1-based) of band to extract from raster. For more\n"
		"      than one band index, separate with comma (,). Ranges can be\n"
		"      defined by separating with dash (-). If unspecified, all bands\n"
		"      of raster will be extracted.\n"
	));
	printf(_(
		"  -t <tile size> Cut raster into tiles to be inserted one per\n"
		"      table row. <tile size> is expressed as WIDTHxHEIGHT.\n"
		"      <tile size> can also be \"auto\" to allow the loader to compute\n"
		"      an appropriate tile size using the first raster and applied to\n"
		"      all rasters.\n"
	));
	printf(_(
		"  -P Pad right-most and bottom-most tiles to guarantee that all tiles\n"
		"     have the same width and height.\n"
	));
	printf(_(
		"  -R  Register the raster as an out-of-db (filesystem) raster. Provided\n"
		"      raster should have absolute path to the file\n"
	));
	printf(_(
		" (-d|a|c|p) These are mutually exclusive options:\n"
		"     -d  Drops the table, then recreates it and populates\n"
		"         it with current raster data.\n"
		"     -a  Appends raster into current table, must be\n"
		"         exactly the same table schema.\n"
		"     -c  Creates a new table and populates it, this is the\n"
		"         default if you do not specify any options.\n"
		"     -p  Prepare mode, only creates the table.\n"
	));
	printf(_(
		"  -f <column> Specify the name of the raster column\n"
	));
	printf(_(
		"  -F  Add a column with the filename of the raster.\n"
	));
	printf(_(
		"  -n <column> Specify the name of the filename column. Implies -F.\n"
	));
	printf(_(
		"  -l <overview factor> Create overview of the raster. For more than\n"
		"      one factor, separate with comma(,). Overview table name follows\n" 
		"      the pattern o_<overview factor>_<table>. Created overview is\n"
		"      stored in the database and is not affected by -R.\n"
	));
	printf(_(
		"  -q  Wrap PostgreSQL identifiers in quotes.\n"
	));
	printf(_(
		"  -I  Create a GIST spatial index on the raster column. The ANALYZE\n"
		"      command will automatically be issued for the created index.\n"
	));
	printf(_(
		"  -M  Run VACUUM ANALYZE on the table of the raster column. Most\n"
		"      useful when appending raster to existing table with -a.\n"
	));
	printf(_(
		"  -C  Set the standard set of constraints on the raster\n"
		"      column after the rasters are loaded. Some constraints may fail\n"
		"      if one or more rasters violate the constraint.\n"
		"  -x  Disable setting the max extent constraint. Only applied if\n"
		"      -C flag is also used.\n"
		"  -r  Set the constraints (spatially unique and coverage tile) for\n"
		"      regular blocking. Only applied if -C flag is also used.\n"
	));
	printf(_(
		"  -T <tablespace> Specify the tablespace for the new table.\n"
		"      Note that indices (including the primary key) will still use\n"
		"      the default tablespace unless the -X flag is also used.\n"
	));
	printf(_(
		"  -X <tablespace> Specify the tablespace for the table's new index.\n"
		"      This applies to the primary key and the spatial index if\n"
		"      the -I flag is used.\n"
	));
	printf(_(
		"  -N <nodata> NODATA value to use on bands without a NODATA value.\n"
	));
	printf(_(
		"  -k  Skip NODATA value checks for each raster band.\n"
	));
	printf(_(
		"  -E <endian> Control endianness of generated binary output of\n"
		"      raster. Use 0 for XDR and 1 for NDR (default). Only NDR\n"
		"      is supported at this time.\n"
	));
	printf(_(
		"  -V <version> Specify version of output WKB format. Default\n"
		"      is 0. Only 0 is supported at this time.\n"
	));
	printf(_(
		"  -e  Execute each statement individually, do not use a transaction.\n"
	));
	printf(_(
		"  -Y  Use COPY statements instead of INSERT statements.\n"
	));
	printf(_(
		"  -G  Print the supported GDAL raster formats.\n"
	));
	printf(_(
		"  -?  Display this help screen.\n"
	));
}

static void calc_tile_size(
	int dimX, int dimY,
	int *tileX, int *tileY
) {
	int i = 0;
	int j = 0;
	int min = 30;
	int max = 100;

	int d = 0;
	double r = 0;
	/*int _d = 0;*/
	double _r = -1;
	int _i = 0;

	/* j = 0, X */
	for (j = 0; j < 2; j++) {
		_i = 0;
		/*_d = 0;*/
		_r = -1;

		if (j < 1 && dimX <= max) {
			*tileX = dimX;
			continue;
		}
		else if (dimY <= max) {
			*tileY = dimY;
			continue;
		}

		for (i = max; i >= min; i--) {
			if (j < 1) {
				d = dimX / i;
				r = (double) dimX / (double) i;

			}
			else {
				d = dimY / i;
				r = (double) dimY / (double) i;
			}
			r = r - (double) d;

			if (
				FLT_EQ(_r, -1) ||
				(r < _r) ||
				FLT_EQ(r, _r)
			) {
				/*_d = d;*/
				_r = r;
				_i = i;
			}
		}

		if (j < 1)
			*tileX = _i;
		else
			*tileY = _i;
	}
}

static void
init_rastinfo(RASTERINFO *info) {
	info->srid = SRID_UNKNOWN;
	info->srs = NULL;
	memset(info->dim, 0, sizeof(double) * 2);
	info->nband_count = 0;
	info->nband = NULL;
	info->gdalbandtype = NULL;
	info->bandtype = NULL;
	info->hasnodata = NULL;
	info->nodataval = NULL;
	memset(info->gt, 0, sizeof(double) * 6);
	memset(info->tile_size, 0, sizeof(int) * 2);
}

static void
rtdealloc_rastinfo(RASTERINFO *info) {
	if (info->srs != NULL)
		rtdealloc(info->srs);
	if (info->nband_count > 0 && info->nband != NULL)
		rtdealloc(info->nband);
	if (info->gdalbandtype != NULL)
		rtdealloc(info->gdalbandtype);
	if (info->bandtype != NULL)
		rtdealloc(info->bandtype);
	if (info->hasnodata != NULL)
		rtdealloc(info->hasnodata);
	if (info->nodataval != NULL)
		rtdealloc(info->nodataval);
}

static int
copy_rastinfo(RASTERINFO *dst, RASTERINFO *src) {
	if (src->srs != NULL) {
		dst->srs = rtalloc(sizeof(char) * (strlen(src->srs) + 1));
		if (dst->srs == NULL) {
			rterror(_("copy_rastinfo: Not enough memory"));
			return 0;
		}
		strcpy(dst->srs, src->srs);
	}
	memcpy(dst->dim, src->dim, sizeof(uint32_t) * 2);
	dst->nband_count = src->nband_count;
	if (src->nband_count && src->nband != NULL) {
		dst->nband = rtalloc(sizeof(int) * src->nband_count);
		if (dst->nband == NULL) {
			rterror(_("copy_rastinfo: Not enough memory"));
			return 0;
		}
		memcpy(dst->nband, src->nband, sizeof(int) * src->nband_count);
	}
	if (src->gdalbandtype != NULL) {
		dst->gdalbandtype = rtalloc(sizeof(GDALDataType) * src->nband_count);
		if (dst->gdalbandtype == NULL) {
			rterror(_("copy_rastinfo: Not enough memory"));
			return 0;
		}
		memcpy(dst->gdalbandtype, src->gdalbandtype, sizeof(GDALDataType) * src->nband_count);
	}
	if (src->bandtype != NULL) {
		dst->bandtype = rtalloc(sizeof(rt_pixtype) * src->nband_count);
		if (dst->bandtype == NULL) {
			rterror(_("copy_rastinfo: Not enough memory"));
			return 0;
		}
		memcpy(dst->bandtype, src->bandtype, sizeof(rt_pixtype) * src->nband_count);
	}
	if (src->hasnodata != NULL) {
		dst->hasnodata = rtalloc(sizeof(int) * src->nband_count);
		if (dst->hasnodata == NULL) {
			rterror(_("copy_rastinfo: Not enough memory"));
			return 0;
		}
		memcpy(dst->hasnodata, src->hasnodata, sizeof(int) * src->nband_count);
	}
	if (src->nodataval != NULL) {
		dst->nodataval = rtalloc(sizeof(double) * src->nband_count);
		if (dst->nodataval == NULL) {
			rterror(_("copy_rastinfo: Not enough memory"));
			return 0;
		}
		memcpy(dst->nodataval, src->nodataval, sizeof(double) * src->nband_count);
	}
	memcpy(dst->gt, src->gt, sizeof(double) * 6);
	memcpy(dst->tile_size, src->tile_size, sizeof(int) * 2);

	return 1;
}

static void
diff_rastinfo(RASTERINFO *x, RASTERINFO *ref) {
	static uint8_t msg[6] = {0};
	int i = 0;

	/* # of bands */
	if (
		!msg[0] &&
		x->nband_count != ref->nband_count
	) {
		rtwarn(_("Different number of bands found in the set of rasters being converted to PostGIS raster"));
		msg[0]++;
	}

	/* pixel types */
	if (!msg[1]) {
		for (i = 0; i < ref->nband_count; i++) {
			if (x->bandtype[i] != ref->bandtype[i]) {
				rtwarn(_("Different pixel types found for band %d in the set of rasters being converted to PostGIS raster"), ref->nband[i]);
				msg[1]++;
			}
		}
	}

	/* hasnodata */
	if (!msg[2]) {
		for (i = 0; i < ref->nband_count; i++) {
			if (x->hasnodata[i] != ref->hasnodata[i]) {
				rtwarn(_("Different hasnodata flags found for band %d in the set of rasters being converted to PostGIS raster"), ref->nband[i]);
				msg[2]++;
			}
		}
	}

	/* nodataval */
	if (!msg[3]) {
		for (i = 0; i < ref->nband_count; i++) {
			if (!x->hasnodata[i] && !ref->hasnodata[i]) continue;
			if (x->hasnodata[i] != ref->hasnodata[i]) {
				rtwarn(_("Different NODATA values found for band %d in the set of rasters being converted to PostGIS raster"), ref->nband[i]);
				msg[3]++;
			}
		}
	}

	/* alignment */
	if (!msg[4]) {
		rt_raster rx = NULL;
		rt_raster rref = NULL;
		int err;
		int aligned;

		if (
			(rx = rt_raster_new(1, 1)) == NULL ||
			(rref = rt_raster_new(1, 1)) == NULL
		) {
			rterror(_("diff_rastinfo: Could not allocate memory for raster alignment test"));
			if (rx != NULL) rt_raster_destroy(rx);
			if (rref != NULL) rt_raster_destroy(rref);
			return;
		}

		rt_raster_set_geotransform_matrix(rx, x->gt);
		rt_raster_set_geotransform_matrix(rref, ref->gt);

		err = rt_raster_same_alignment(rx, rref, &aligned, NULL);
		rt_raster_destroy(rx);
		rt_raster_destroy(rref);
		if (err != ES_NONE) {
			rterror(_("diff_rastinfo: Could not run raster alignment test"));
			return;
		}

		if (!aligned) {
			rtwarn(_("Raster with different alignment found in the set of rasters being converted to PostGIS raster"));
			msg[4]++;
		}
	}

	/* tile size */
	if (!msg[5]) {
		for (i = 0; i < 2; i++) {
			if (x->tile_size[i] != ref->tile_size[i]) {
				rtwarn(_("Different tile sizes found in the set of rasters being converted to PostGIS raster"));
				msg[5]++;
				break;
			}
		}
	}
}

static void
init_config(RTLOADERCFG *config) {
	config->rt_file_count = 0;
	config->rt_file = NULL;
	config->rt_filename = NULL;
	config->schema = NULL;
	config->table = NULL;
	config->raster_column = NULL;
	config->file_column = 0;
	config->file_column_name = NULL;
	config->overview_count = 0;
	config->overview = NULL;
	config->overview_table = NULL;
	config->quoteident = 0;
	config->srid = config->out_srid = SRID_UNKNOWN;
	config->nband = NULL;
	config->nband_count = 0;
	memset(config->tile_size, 0, sizeof(int) * 2);
	config->pad_tile = 0;
	config->outdb = 0;
	config->opt = 'c';
	config->idx = 0;
	config->maintenance = 0;
	config->constraints = 0;
	config->max_extent = 1;
	config->regular_blocking = 0;
	config->tablespace = NULL;
	config->idx_tablespace = NULL;
	config->hasnodata = 0;
	config->nodataval = 0;
	config->skip_nodataval_check = 0;
	config->endian = 1;
	config->version = 0;
	config->transaction = 1;
	config->copy_statements = 0;
}

static void
rtdealloc_config(RTLOADERCFG *config) {
	int i = 0;
	if (config->rt_file_count) {
		for (i = config->rt_file_count - 1; i >= 0; i--) {
			rtdealloc(config->rt_file[i]);
			if (config->rt_filename)
				rtdealloc(config->rt_filename[i]);
		}
		rtdealloc(config->rt_file);
		if (config->rt_filename)
			rtdealloc(config->rt_filename);
	}
	if (config->schema != NULL)
		rtdealloc(config->schema);
	if (config->table != NULL)
		rtdealloc(config->table);
	if (config->raster_column != NULL)
		rtdealloc(config->raster_column);
	if (config->file_column_name != NULL)
		rtdealloc(config->file_column_name);
	if (config->overview_count > 0) {
		if (config->overview != NULL)
			rtdealloc(config->overview);
		if (config->overview_table != NULL) {
			for (i = config->overview_count - 1; i >= 0; i--)
				rtdealloc(config->overview_table[i]);
			rtdealloc(config->overview_table);
		}
	}
	if (config->nband_count > 0 && config->nband != NULL)
		rtdealloc(config->nband);
	if (config->tablespace != NULL)
		rtdealloc(config->tablespace);
	if (config->idx_tablespace != NULL)
		rtdealloc(config->idx_tablespace);

	rtdealloc(config);
}

static void
init_stringbuffer(STRINGBUFFER *buffer) {
	buffer->line = NULL;
	buffer->length = 0;
}

static void
rtdealloc_stringbuffer(STRINGBUFFER *buffer, int freebuffer) {
	if (buffer->length) {
		uint32_t i = 0;
		for (i = 0; i < buffer->length; i++) {
			if (buffer->line[i] != NULL)
				rtdealloc(buffer->line[i]);
		}
		rtdealloc(buffer->line);
	}
	buffer->line = NULL;
	buffer->length = 0;

	if (freebuffer)
		rtdealloc(buffer);
}

static void
dump_stringbuffer(STRINGBUFFER *buffer) {
	int i = 0;

	for (i = 0; i < buffer->length; i++) {
		printf("%s\n", buffer->line[i]);
	}
}

static void
flush_stringbuffer(STRINGBUFFER *buffer) {
	dump_stringbuffer(buffer);
	rtdealloc_stringbuffer(buffer, 0);
}

/* Takes ownership of the passed string */
static int
append_stringbuffer(STRINGBUFFER *buffer, const char *str) {
	buffer->length++;

	buffer->line = rtrealloc(buffer->line, sizeof(char *) * buffer->length);
	if (buffer->line == NULL) {
		rterror(_("append_stringbuffer: Could not allocate memory for appending string to buffer"));
		return 0;
	}

	buffer->line[buffer->length - 1] = (char *) str;

	return 1;
}

static int
append_sql_to_buffer(STRINGBUFFER *buffer, const char *str) {
	if (buffer->length > 9)
		flush_stringbuffer(buffer);

	return append_stringbuffer(buffer, str);
}

static int
copy_from(const char *schema, const char *table, const char *column,
          const char *filename, const char *file_column_name,
          STRINGBUFFER *buffer)
{
	char *sql = NULL;
	uint32_t len = 0;

	assert(table != NULL);
	assert(column != NULL);

	len = strlen("COPY  () FROM stdin;") + 1;
	if (schema != NULL)
		len += strlen(schema);
	len += strlen(table);
	len += strlen(column);
	if (filename != NULL)
		len += strlen(",") + strlen(file_column_name);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("copy_from: Could not allocate memory for COPY statement"));
		return 0;
	}
	sprintf(sql, "COPY %s%s (%s%s%s) FROM stdin;",
		(schema != NULL ? schema : ""),
		table,
		column,
		(filename != NULL ? "," : ""),
		(filename != NULL ? file_column_name : "")
	);

	append_sql_to_buffer(buffer, sql);
	sql = NULL;

	return 1;
}

static int
copy_from_end(STRINGBUFFER *buffer)
{
	/* end of data */
	append_sql_to_buffer(buffer, strdup("\\."));

	return 1;
}

static int
insert_records(
	const char *schema, const char *table, const char *column,
	const char *filename, const char *file_column_name,
	int copy_statements, int out_srid,
	STRINGBUFFER *tileset, STRINGBUFFER *buffer
) {
	char *fn = NULL;
	uint32_t len = 0;
	char *sql = NULL;
	uint32_t x = 0;

	assert(table != NULL);
	assert(column != NULL);

	/* COPY statements */
	if (copy_statements) {

    if (!copy_from(
      schema, table, column,
      (file_column_name ? filename : NULL), file_column_name,
      buffer
    )) {
      rterror(_("insert_records: Could not add COPY statement to string buffer"));
      return 0;
    }


		/* escape tabs in filename */
		if (filename != NULL)
			fn = strreplace(filename, "\t", "\\t", NULL);

		/* rows */
		for (x = 0; x < tileset->length; x++) {
			len = strlen(tileset->line[x]) + 1;

			if (filename != NULL)
				len += strlen(fn) + 1;

			sql = rtalloc(sizeof(char) * len);
			if (sql == NULL) {
				rterror(_("insert_records: Could not allocate memory for COPY statement"));
				return 0;
			}
			sprintf(sql, "%s%s%s",
				tileset->line[x],
				(filename != NULL ? "\t" : ""),
				(filename != NULL ? fn : "")
			);

			append_sql_to_buffer(buffer, sql);
			sql = NULL;
		}

    if (!copy_from_end(buffer)) {
      rterror(_("process_rasters: Could not add COPY end statement to string buffer"));
      return 0;
    }

	}
	/* INSERT statements */
	else {
		len = strlen("INSERT INTO  () VALUES (ST_Transform(''::raster,xxxxxxxxx));") + 1;
		if (schema != NULL)
			len += strlen(schema);
		len += strlen(table);
		len += strlen(column);
		if (filename != NULL)
			len += strlen(",") + strlen(file_column_name);

		/* escape single-quotes in filename */
		if (filename != NULL)
			fn = strreplace(filename, "'", "''", NULL);

		for (x = 0; x < tileset->length; x++) {
			char *ptr;
			int sqllen = len;

			sqllen += strlen(tileset->line[x]);
			if (filename != NULL)
				sqllen += strlen(",''") + strlen(fn);

			sql = rtalloc(sizeof(char) * sqllen);
			if (sql == NULL) {
				rterror(_("insert_records: Could not allocate memory for INSERT statement"));
				return 0;
			}
			ptr = sql;
			ptr += sprintf(sql, "INSERT INTO %s%s (%s%s%s) VALUES (",
					(schema != NULL ? schema : ""),
					table,
					column,
					(filename != NULL ? "," : ""),
					(filename != NULL ? file_column_name : "")
				);
			if (out_srid != SRID_UNKNOWN) {
				ptr += sprintf(ptr, "ST_Transform(");
			}
			ptr += sprintf(ptr, "'%s'::raster",
					tileset->line[x]
				);
			if (out_srid != SRID_UNKNOWN) {
				ptr += sprintf(ptr, ", %d)", out_srid);
			}
			if (filename != NULL) {
				ptr += sprintf(ptr, ",'%s'", fn);
			}
			ptr += sprintf(ptr, ");");

			append_sql_to_buffer(buffer, sql);
			sql = NULL;
		}
	}

	if (fn != NULL) rtdealloc(fn);
	return 1;
}

static int
drop_table(const char *schema, const char *table, STRINGBUFFER *buffer) {
	char *sql = NULL;
	uint32_t len = 0;

	len = strlen("DROP TABLE IF EXISTS ;") + 1;
	if (schema != NULL)
		len += strlen(schema);
	len += strlen(table);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("drop_table: Could not allocate memory for DROP TABLE statement"));
		return 0;
	}
	sprintf(sql, "DROP TABLE IF EXISTS %s%s;",
		(schema != NULL ? schema : ""),
		table
	);

	append_sql_to_buffer(buffer, sql);

	return 1;
}

static int
create_table(
	const char *schema, const char *table, const char *column,
	const int file_column, const char *file_column_name,
	const char *tablespace, const char *idx_tablespace,
	STRINGBUFFER *buffer
) {
	char *sql = NULL;
	uint32_t len = 0;

	assert(table != NULL);
	assert(column != NULL);

	len = strlen("CREATE TABLE  (\"rid\" serial PRIMARY KEY, raster);") + 1;
	if (schema != NULL)
		len += strlen(schema);
	len += strlen(table);
	len += strlen(column);
	if (file_column)
		len += strlen(", text") + strlen(file_column_name);
	if (tablespace != NULL)
		len += strlen(" TABLESPACE ") + strlen(tablespace);
	if (idx_tablespace != NULL)
		len += strlen(" USING INDEX TABLESPACE ") + strlen(idx_tablespace);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("create_table: Could not allocate memory for CREATE TABLE statement"));
		return 0;
	}
	sprintf(sql, "CREATE TABLE %s%s (\"rid\" serial PRIMARY KEY%s%s,%s raster%s%s%s)%s%s;",
		(schema != NULL ? schema : ""),
		table,
		(idx_tablespace != NULL ? " USING INDEX TABLESPACE " : ""),
		(idx_tablespace != NULL ? idx_tablespace : ""),
		column,
		(file_column ? "," : ""),
		(file_column ? file_column_name : ""),
		(file_column ? " text" : ""),
		(tablespace != NULL ? " TABLESPACE " : ""),
		(tablespace != NULL ? tablespace : "")
	);

	append_sql_to_buffer(buffer, sql);

	return 1;
}

static int
create_index(
	const char *schema, const char *table, const char *column,
	const char *tablespace,
	STRINGBUFFER *buffer
) {
	char *sql = NULL;
	uint32_t len = 0;
	char *_table = NULL;
	char *_column = NULL;

	assert(table != NULL);
	assert(column != NULL);

	_table = chartrim(table, "\"");
	_column = chartrim(column, "\"");

	/* create index */
	len = strlen("CREATE INDEX \"__gist\" ON  USING gist (st_convexhull());") + 1;
	if (schema != NULL)
		len += strlen(schema);
	len += strlen(_table);
	len += strlen(_column);
	len += strlen(table);
	len += strlen(column);
	if (tablespace != NULL)
		len += strlen(" TABLESPACE ") + strlen(tablespace);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("create_index: Could not allocate memory for CREATE INDEX statement"));
		rtdealloc(_table);
		rtdealloc(_column);
		return 0;
	}
	sprintf(sql, "CREATE INDEX ON %s%s USING gist (st_convexhull(%s))%s%s;",
		(schema != NULL ? schema : ""),
		table,
		column,
		(tablespace != NULL ? " TABLESPACE " : ""),
		(tablespace != NULL ? tablespace : "")
	);
	rtdealloc(_table);
	rtdealloc(_column);

	append_sql_to_buffer(buffer, sql);

	return 1;
}

static int
analyze_table(
	const char *schema, const char *table,
	STRINGBUFFER *buffer
) {
	char *sql = NULL;
	uint32_t len = 0;

	assert(table != NULL);

	len = strlen("ANALYZE ;") + 1;
	if (schema != NULL)
		len += strlen(schema);
	len += strlen(table);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("analyze_table: Could not allocate memory for ANALYZE TABLE statement"));
		return 0;
	}
	sprintf(sql, "ANALYZE %s%s;",
		(schema != NULL ? schema : ""),
		table
	);

	append_sql_to_buffer(buffer, sql);

	return 1;
}

static int
vacuum_table(
	const char *schema, const char *table,
	STRINGBUFFER *buffer
) {
	char *sql = NULL;
	uint32_t len = 0;

	assert(table != NULL);

	len = strlen("VACUUM ANALYZE ;") + 1;
	if (schema != NULL)
		len += strlen(schema);
	len += strlen(table);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("vacuum_table: Could not allocate memory for VACUUM statement"));
		return 0;
	}
	sprintf(sql, "VACUUM ANALYZE %s%s;",
		(schema != NULL ? schema : ""),
		table
	);

	append_sql_to_buffer(buffer, sql);

	return 1;
}

static int
add_raster_constraints(
	const char *schema, const char *table, const char *column,
	int regular_blocking, int max_extent,
	STRINGBUFFER *buffer
) {
	char *sql = NULL;
	uint32_t len = 0;

	char *_tmp = NULL;
	char *_schema = NULL;
	char *_table = NULL;
	char *_column = NULL;

	assert(table != NULL);
	assert(column != NULL);

	/* schema */
	if (schema != NULL) {
		_tmp = chartrim(schema, ".");
		_schema = chartrim(_tmp, "\"");
		rtdealloc(_tmp);
		_tmp = strreplace(_schema, "'", "''", NULL);
		rtdealloc(_schema);
		_schema = _tmp;
	}

	/* table */
	_tmp = chartrim(table, "\"");
	_table = strreplace(_tmp, "'", "''", NULL);
	rtdealloc(_tmp);

	/* column */
	_tmp = chartrim(column, "\"");
	_column = strreplace(_tmp, "'", "''", NULL);
	rtdealloc(_tmp);

	len = strlen("SELECT AddRasterConstraints('','','',TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,FALSE,TRUE,TRUE,TRUE,TRUE,FALSE);") + 1;
	if (_schema != NULL)
		len += strlen(_schema);
	len += strlen(_table);
	len += strlen(_column);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("add_raster_constraints: Could not allocate memory for AddRasterConstraints statement"));
		return 0;
	}
	sprintf(sql, "SELECT AddRasterConstraints('%s','%s','%s',TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,%s,TRUE,TRUE,TRUE,TRUE,%s);",
		(_schema != NULL ? _schema : ""),
		_table,
		_column,
		(regular_blocking ? "TRUE" : "FALSE"),
		(max_extent ? "TRUE" : "FALSE")
	);
	
	if (_schema != NULL)
		rtdealloc(_schema);
	rtdealloc(_table);
	rtdealloc(_column);

	append_sql_to_buffer(buffer, sql);

	return 1;
}

static int
add_overview_constraints(
	const char *ovschema, const char *ovtable, const char *ovcolumn,
	const char *schema, const char *table, const char *column,
	const int factor,
	STRINGBUFFER *buffer
) {
	char *sql = NULL;
	uint32_t len = 0;

	char *_tmp = NULL;

	char *_ovschema = NULL;
	char *_ovtable = NULL;
	char *_ovcolumn = NULL;

	char *_schema = NULL;
	char *_table = NULL;
	char *_column = NULL;

	assert(ovtable != NULL);
	assert(ovcolumn != NULL);
	assert(table != NULL);
	assert(column != NULL);
	assert(factor >= MINOVFACTOR && factor <= MAXOVFACTOR);

	/* overview schema */
	if (ovschema != NULL) {
		_tmp = chartrim(ovschema, ".");
		_ovschema = chartrim(_tmp, "\"");
		rtdealloc(_tmp);
		_tmp = strreplace(_ovschema, "'", "''", NULL);
		rtdealloc(_ovschema);
		_ovschema = _tmp;
	}

	/* overview table */
	_tmp = chartrim(ovtable, "\"");
	_ovtable = strreplace(_tmp, "'", "''", NULL);
	rtdealloc(_tmp);

	/* overview column*/
	_tmp = chartrim(ovcolumn, "\"");
	_ovcolumn = strreplace(_tmp, "'", "''", NULL);
	rtdealloc(_tmp);

	/* schema */
	if (schema != NULL) {
		_tmp = chartrim(schema, ".");
		_schema = chartrim(_tmp, "\"");
		rtdealloc(_tmp);
		_tmp = strreplace(_schema, "'", "''", NULL);
		rtdealloc(_schema);
		_schema = _tmp;
	}

	/* table */
	_tmp = chartrim(table, "\"");
	_table = strreplace(_tmp, "'", "''", NULL);
	rtdealloc(_tmp);

	/* column */
	_tmp = chartrim(column, "\"");
	_column = strreplace(_tmp, "'", "''", NULL);
	rtdealloc(_tmp);

	len = strlen("SELECT AddOverviewConstraints('','','','','','',);") + 5;
	if (_ovschema != NULL)
		len += strlen(_ovschema);
	len += strlen(_ovtable);
	len += strlen(_ovcolumn);
	if (_schema != NULL)
		len += strlen(_schema);
	len += strlen(_table);
	len += strlen(_column);

	sql = rtalloc(sizeof(char) * len);
	if (sql == NULL) {
		rterror(_("add_overview_constraints: Could not allocate memory for AddOverviewConstraints statement"));
		return 0;
	}
	sprintf(sql, "SELECT AddOverviewConstraints('%s','%s','%s','%s','%s','%s',%d);",
		(_ovschema != NULL ? _ovschema : ""),
		_ovtable,
		_ovcolumn,
		(_schema != NULL ? _schema : ""),
		_table,
		_column,
		factor
	);
	
	if (_ovschema != NULL)
		rtdealloc(_ovschema);
	rtdealloc(_ovtable);
	rtdealloc(_ovcolumn);

	if (_schema != NULL)
		rtdealloc(_schema);
	rtdealloc(_table);
	rtdealloc(_column);

	append_sql_to_buffer(buffer, sql);

	return 1;
}

static int
build_overview(int idx, RTLOADERCFG *config, RASTERINFO *info, int ovx, STRINGBUFFER *tileset, STRINGBUFFER *buffer) {
	GDALDatasetH hdsSrc;
	VRTDatasetH hdsOv;
	VRTSourcedRasterBandH hbandOv;
	double gtOv[6] = {0.};
	int dimOv[2] = {0};

	int j = 0;
	int factor;
	const char *ovtable = NULL;

	VRTDatasetH hdsDst;
	VRTSourcedRasterBandH hbandDst;
	int tile_size[2] = {0};
	int _tile_size[2] = {0};
	int ntiles[2] = {1, 1};
	int xtile = 0;
	int ytile = 0;
	double gt[6] = {0.};

	rt_raster rast = NULL;
	char *hex;
	uint32_t hexlen = 0;

	hdsSrc = GDALOpenShared(config->rt_file[idx], GA_ReadOnly);
	if (hdsSrc == NULL) {
		rterror(_("build_overview: Could not open raster: %s"), config->rt_file[idx]);
		return 0;
	}

	/* working copy of geotransform matrix */
	memcpy(gtOv, info->gt, sizeof(double) * 6);

	if (ovx >= config->overview_count) {
		rterror(_("build_overview: Invalid overview index: %d"), ovx);
		return 0;
	}
	factor = config->overview[ovx];
	ovtable = (const char *) config->overview_table[ovx];

	/* factor must be within valid range */
	if (factor < MINOVFACTOR || factor > MAXOVFACTOR) {
		rterror(_("build_overview: Overview factor %d is not between %d and %d"), factor, MINOVFACTOR, MAXOVFACTOR);
		return 0;
	}

	dimOv[0] = (int) (info->dim[0] + (factor / 2)) / factor;
	dimOv[1] = (int) (info->dim[1] + (factor / 2)) / factor;

	/* create VRT dataset */
	hdsOv = VRTCreate(dimOv[0], dimOv[1]);
	/*
	GDALSetDescription(hdsOv, "/tmp/ov.vrt");
	*/
	GDALSetProjection(hdsOv, info->srs);

	/* adjust scale */
	gtOv[1] *= factor;
	gtOv[5] *= factor;

	GDALSetGeoTransform(hdsOv, gtOv);

	/* add bands as simple sources */
	for (j = 0; j < info->nband_count; j++) {
		GDALAddBand(hdsOv, info->gdalbandtype[j], NULL);
		hbandOv = (VRTSourcedRasterBandH) GDALGetRasterBand(hdsOv, j + 1);

		if (info->hasnodata[j])
			GDALSetRasterNoDataValue(hbandOv, info->nodataval[j]);

		VRTAddSimpleSource(
			hbandOv, GDALGetRasterBand(hdsSrc, info->nband[j]),
			0, 0,
			info->dim[0], info->dim[1],
			0, 0,
			dimOv[0], dimOv[1],
			"near", VRT_NODATA_UNSET
		);
	}

	/* make sure VRT reflects all changes */
	VRTFlushCache(hdsOv);

	/* decide on tile size */
	if (!config->tile_size[0])
		tile_size[0] = dimOv[0];
	else
		tile_size[0] = config->tile_size[0];
	if (!config->tile_size[1])
		tile_size[1] = dimOv[1];
	else
		tile_size[1] = config->tile_size[1];

	/* number of tiles */
	if (
		tile_size[0] != dimOv[0] &&
		tile_size[1] != dimOv[1]
	) {
		ntiles[0] = (dimOv[0] + tile_size[0] -  1) / tile_size[0];
		ntiles[1] = (dimOv[1] + tile_size[1]  - 1) / tile_size[1];
	}

	/* working copy of geotransform matrix */
	memcpy(gt, gtOv, sizeof(double) * 6);

	/* tile overview */
	/* each tile is a VRT with constraints set for just the data required for the tile */
	for (ytile = 0; ytile < ntiles[1]; ytile++) {

		/* edge y tile */
		if (!config->pad_tile && ntiles[1] > 1 && (ytile + 1) == ntiles[1])
			_tile_size[1] = dimOv[1] - (ytile * tile_size[1]);
		else
			_tile_size[1] = tile_size[1];

		for (xtile = 0; xtile < ntiles[0]; xtile++) {
			/*
			char fn[100];
			sprintf(fn, "/tmp/ovtile%d.vrt", (ytile * ntiles[0]) + xtile);
			*/

			/* edge x tile */
			if (!config->pad_tile && ntiles[0] > 1 && (xtile + 1) == ntiles[0])
				_tile_size[0] = dimOv[0] - (xtile * tile_size[0]);
			else
				_tile_size[0] = tile_size[0];

			/* compute tile's upper-left corner */
			GDALApplyGeoTransform(
				gtOv,
				xtile * tile_size[0], ytile * tile_size[1],
				&(gt[0]), &(gt[3])
			);

			/* create VRT dataset */
			hdsDst = VRTCreate(_tile_size[0], _tile_size[1]);
			/*
			GDALSetDescription(hdsDst, fn);
			*/
			GDALSetProjection(hdsDst, info->srs);
			GDALSetGeoTransform(hdsDst, gt);

			/* add bands as simple sources */
			for (j = 0; j < info->nband_count; j++) {
				GDALAddBand(hdsDst, info->gdalbandtype[j], NULL);
				hbandDst = (VRTSourcedRasterBandH) GDALGetRasterBand(hdsDst, j + 1);

				if (info->hasnodata[j])
					GDALSetRasterNoDataValue(hbandDst, info->nodataval[j]);

				VRTAddSimpleSource(
					hbandDst, GDALGetRasterBand(hdsOv, j + 1),
					xtile * tile_size[0], ytile * tile_size[1],
					_tile_size[0], _tile_size[1],
					0, 0,
					_tile_size[0], _tile_size[1],
					"near", VRT_NODATA_UNSET
				);
			}

			/* make sure VRT reflects all changes */
			VRTFlushCache(hdsDst);

			/* convert VRT dataset to rt_raster */
			rast = rt_raster_from_gdal_dataset(hdsDst);
			if (rast == NULL) {
				rterror(_("build_overview: Could not convert VRT dataset to PostGIS raster"));
				GDALClose(hdsDst);
				return 0;
			}

			/* set srid if provided */
			rt_raster_set_srid(rast, info->srid);

			/* convert rt_raster to hexwkb */
			hex = rt_raster_to_hexwkb(rast, FALSE, &hexlen);
			raster_destroy(rast);

			if (hex == NULL) {
				rterror(_("build_overview: Could not convert PostGIS raster to hex WKB"));
				GDALClose(hdsDst);
				return 0;
			}

			/* add hexwkb to tileset */
			append_stringbuffer(tileset, hex);

			GDALClose(hdsDst);

			/* flush if tileset gets too big */
			if (tileset->length > 10) {
				if (!insert_records(
					config->schema, ovtable, config->raster_column,
					(config->file_column ? config->rt_filename[idx] : NULL), config->file_column_name,
					config->copy_statements, config->out_srid,
					tileset, buffer
				)) {
					rterror(_("build_overview: Could not convert raster tiles into INSERT or COPY statements"));
					GDALClose(hdsSrc);
					return 0;
				}

				rtdealloc_stringbuffer(tileset, 0);
			}
		}
	}

	GDALClose(hdsOv);
	GDALClose(hdsSrc);
	return 1;
}

static int
convert_raster(int idx, RTLOADERCFG *config, RASTERINFO *info, STRINGBUFFER *tileset, STRINGBUFFER *buffer) {
	GDALDatasetH hdsSrc;
	GDALRasterBandH hbandSrc;
	int nband = 0;
	int i = 0;
	int ntiles[2] = {1, 1};
	int _tile_size[2] = {0, 0};
	int xtile = 0;
	int ytile = 0;
	double gt[6] = {0.};
	const char* pszProjectionRef = NULL;
	int tilesize = 0;

	rt_raster rast = NULL;
	int numbands = 0;
	rt_band band = NULL;
	char *hex;
	uint32_t hexlen = 0;

	info->srid = config->srid;

	hdsSrc = GDALOpenShared(config->rt_file[idx], GA_ReadOnly);
	if (hdsSrc == NULL) {
		rterror(_("convert_raster: Could not open raster: %s"), config->rt_file[idx]);
		return 0;
	}

	nband = GDALGetRasterCount(hdsSrc);
	if (!nband) {
		rterror(_("convert_raster: No bands found in raster: %s"), config->rt_file[idx]);
		GDALClose(hdsSrc);
		return 0;
	}

	/* check that bands specified are available */
	for (i = 0; i < config->nband_count; i++) {
		if (config->nband[i] > nband) {
			rterror(_("convert_raster: Band %d not found in raster: %s"), config->nband[i], config->rt_file[idx]);
			GDALClose(hdsSrc);
			return 0;
		}
	}

	/* record srs */
	pszProjectionRef = GDALGetProjectionRef(hdsSrc);
	if (pszProjectionRef != NULL && pszProjectionRef[0] != '\0') {
		info->srs = rtalloc(sizeof(char) * (strlen(pszProjectionRef) + 1));
		if (info->srs == NULL) {
			rterror(_("convert_raster: Could not allocate memory for storing SRS"));
			GDALClose(hdsSrc);
			return 0;
		}
		strcpy(info->srs, pszProjectionRef);

		if (info->srid == SRID_UNKNOWN) {
			OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);
			if (OSRSetFromUserInput(hSRS, pszProjectionRef) == OGRERR_NONE) {
				const char* pszAuthorityName = OSRGetAuthorityName(hSRS, NULL);
				const char* pszAuthorityCode = OSRGetAuthorityCode(hSRS, NULL);
				if (
					pszAuthorityName != NULL &&
					strcmp(pszAuthorityName, "EPSG") == 0 &&
					pszAuthorityCode != NULL
				) {
					info->srid = atoi(pszAuthorityCode);
				}
			}
			OSRDestroySpatialReference(hSRS);
		}
	}

	if ( info->srid == SRID_UNKNOWN && config->out_srid != SRID_UNKNOWN ) {
		  rterror(_("convert_raster: could not determine source srid, cannot transform to target srid %d"), config->out_srid);
		  GDALClose(hdsSrc);
		  return 0;
	}

	/* record geotransform matrix */
	if (GDALGetGeoTransform(hdsSrc, info->gt) != CE_None) {
		rtinfo(_("Using default geotransform matrix (0, 1, 0, 0, 0, -1) for raster: %s"), config->rt_file[idx]);
		info->gt[0] = 0;
		info->gt[1] = 1;
		info->gt[2] = 0;
		info->gt[3] = 0;
		info->gt[4] = 0;
		info->gt[5] = -1;
	}
	memcpy(gt, info->gt, sizeof(double) * 6);

	/* record # of bands */
	/* user-specified bands */
	if (config->nband_count > 0) {
		info->nband_count = config->nband_count;
		info->nband = rtalloc(sizeof(int) * info->nband_count);
		if (info->nband == NULL) {
			rterror(_("convert_raster: Could not allocate memory for storing band indices"));
			GDALClose(hdsSrc);
			return 0;
		}
		memcpy(info->nband, config->nband, sizeof(int) * info->nband_count);
	}
	/* all bands */
	else {
		info->nband_count = nband;
		info->nband = rtalloc(sizeof(int) * info->nband_count);
		if (info->nband == NULL) {
			rterror(_("convert_raster: Could not allocate memory for storing band indices"));
			GDALClose(hdsSrc);
			return 0;
		}
		for (i = 0; i < info->nband_count; i++)
			info->nband[i] = i + 1;
	}

	/* initialize parameters dependent on nband */
	info->gdalbandtype = rtalloc(sizeof(GDALDataType) * info->nband_count);
	if (info->gdalbandtype == NULL) {
		rterror(_("convert_raster: Could not allocate memory for storing GDAL data type"));
		GDALClose(hdsSrc);
		return 0;
	}
	info->bandtype = rtalloc(sizeof(rt_pixtype) * info->nband_count);
	if (info->bandtype == NULL) {
		rterror(_("convert_raster: Could not allocate memory for storing pixel type"));
		GDALClose(hdsSrc);
		return 0;
	}
	info->hasnodata = rtalloc(sizeof(int) * info->nband_count);
	if (info->hasnodata == NULL) {
		rterror(_("convert_raster: Could not allocate memory for storing hasnodata flag"));
		GDALClose(hdsSrc);
		return 0;
	}
	info->nodataval = rtalloc(sizeof(double) * info->nband_count);
	if (info->nodataval == NULL) {
		rterror(_("convert_raster: Could not allocate memory for storing nodata value"));
		GDALClose(hdsSrc);
		return 0;
	}
	memset(info->gdalbandtype, GDT_Unknown, sizeof(GDALDataType) * info->nband_count);
	memset(info->bandtype, PT_END, sizeof(rt_pixtype) * info->nband_count);
	memset(info->hasnodata, 0, sizeof(int) * info->nband_count);
	memset(info->nodataval, 0, sizeof(double) * info->nband_count);

	/* dimensions of raster */
	info->dim[0] = GDALGetRasterXSize(hdsSrc);
	info->dim[1] = GDALGetRasterYSize(hdsSrc);

	/* tile size is "auto" */
	if (
		config->tile_size[0] == -1 &&
		config->tile_size[1] == -1
	) {
		calc_tile_size(
			info->dim[0], info->dim[1],
			&(config->tile_size[0]), &(config->tile_size[1])
		);

		rtinfo(_("Using computed tile size: %dx%d"), config->tile_size[0], config->tile_size[1]);
	}

	/* decide on tile size */
	if (!config->tile_size[0])
		info->tile_size[0] = info->dim[0];
	else
		info->tile_size[0] = config->tile_size[0];
	if (!config->tile_size[1])
		info->tile_size[1] = info->dim[1];
	else
		info->tile_size[1] = config->tile_size[1];

	/* number of tiles */
	if (info->tile_size[0] != info->dim[0])
		ntiles[0] = (info->dim[0] + info->tile_size[0]  - 1) / info->tile_size[0];
	if (info->tile_size[1] != info->dim[1]) 
		ntiles[1] = (info->dim[1] + info->tile_size[1]  - 1) / info->tile_size[1];

	/* estimate size of 1 tile */
	tilesize = info->tile_size[0] * info->tile_size[1];

	/* go through bands for attributes */
	for (i = 0; i < info->nband_count; i++) {
		hbandSrc = GDALGetRasterBand(hdsSrc, info->nband[i]);

		/* datatype */
		info->gdalbandtype[i] = GDALGetRasterDataType(hbandSrc);

		/* complex data type? */
		if (GDALDataTypeIsComplex(info->gdalbandtype[i])) {
			rterror(_("convert_raster: The pixel type of band %d is a complex data type.  PostGIS raster does not support complex data types"), i + 1);
			GDALClose(hdsSrc);
			return 0;
		}

		/* convert data type to that of postgis raster */
		info->bandtype[i] = rt_util_gdal_datatype_to_pixtype(info->gdalbandtype[i]);

		/* hasnodata and nodataval */
		info->nodataval[i] = GDALGetRasterNoDataValue(hbandSrc, &(info->hasnodata[i]));
		if (!info->hasnodata[i]) {
			/* does NOT have nodata value, but user-specified */
			if (config->hasnodata) {
				info->hasnodata[i] = 1;
				info->nodataval[i] = config->nodataval;
			}
			else
				info->nodataval[i] = 0;
		}

		/* update estimated size of 1 tile */
		tilesize *= rt_pixtype_size(info->bandtype[i]);
	}

	/* roughly estimate size of one tile and all bands */
	tilesize *= 1.1;
	if (tilesize > MAXTILESIZE)
		rtwarn(_("The size of each output tile may exceed 1 GB. Use -t to specify a reasonable tile size"));

	/* out-db raster */
	if (config->outdb) {
		GDALClose(hdsSrc);

		/* each tile is a raster */
		for (ytile = 0; ytile < ntiles[1]; ytile++) {
			/* edge y tile */
			if (!config->pad_tile && ntiles[1] > 1 && (ytile + 1) == ntiles[1])
				_tile_size[1] = info->dim[1] - (ytile * info->tile_size[1]);
			else
				_tile_size[1] = info->tile_size[1];

			for (xtile = 0; xtile < ntiles[0]; xtile++) {

				/* edge x tile */
				if (!config->pad_tile && ntiles[0] > 1 && (xtile + 1) == ntiles[0])
					_tile_size[0] = info->dim[0] - (xtile * info->tile_size[0]);
				else
					_tile_size[0] = info->tile_size[0];

				/* compute tile's upper-left corner */
				GDALApplyGeoTransform(
					info->gt,
					xtile * info->tile_size[0], ytile * info->tile_size[1],
					&(gt[0]), &(gt[3])
				);

				/* create raster object */
				rast = rt_raster_new(_tile_size[0], _tile_size[1]);
				if (rast == NULL) {
					rterror(_("convert_raster: Could not create raster"));
					return 0;
				}

				/* set raster attributes */
				rt_raster_set_srid(rast, info->srid);
				rt_raster_set_geotransform_matrix(rast, gt);

				/* add bands */
				for (i = 0; i < info->nband_count; i++) {
					band = rt_band_new_offline(
						_tile_size[0], _tile_size[1],
						info->bandtype[i],
						info->hasnodata[i], info->nodataval[i],
						info->nband[i] - 1,
						config->rt_file[idx]
					);
					if (band == NULL) {
						rterror(_("convert_raster: Could not create offline band"));
						raster_destroy(rast);
						return 0;
					}

					/* add band to raster */
					if (rt_raster_add_band(rast, band, rt_raster_get_num_bands(rast)) == -1) {
						rterror(_("convert_raster: Could not add offlineband to raster"));
						rt_band_destroy(band);
						raster_destroy(rast);
						return 0;
					}

					/* inspect each band of raster where band is NODATA */
					if (!config->skip_nodataval_check)
						rt_band_check_is_nodata(band);
				}

				/* convert rt_raster to hexwkb */
				hex = rt_raster_to_hexwkb(rast, FALSE, &hexlen);
				raster_destroy(rast);

				if (hex == NULL) {
					rterror(_("convert_raster: Could not convert PostGIS raster to hex WKB"));
					return 0;
				}

				/* add hexwkb to tileset */
				append_stringbuffer(tileset, hex);

				/* flush if tileset gets too big */
				if (tileset->length > 10) {
					if (!insert_records(
						config->schema, config->table, config->raster_column,
						(config->file_column ? config->rt_filename[idx] : NULL), config->file_column_name,
						config->copy_statements, config->out_srid,
						tileset, buffer
					)) {
						rterror(_("convert_raster: Could not convert raster tiles into INSERT or COPY statements"));
						return 0;
					}

					rtdealloc_stringbuffer(tileset, 0);
				}
			}
		}
	}
	/* in-db raster */
	else {
		VRTDatasetH hdsDst;
		VRTSourcedRasterBandH hbandDst;

		/* each tile is a VRT with constraints set for just the data required for the tile */
		for (ytile = 0; ytile < ntiles[1]; ytile++) {

			/* edge y tile */
			if (!config->pad_tile && ntiles[1] > 1 && (ytile + 1) == ntiles[1])
				_tile_size[1] = info->dim[1] - (ytile * info->tile_size[1]);
			else
				_tile_size[1] = info->tile_size[1];

			for (xtile = 0; xtile < ntiles[0]; xtile++) {
				/*
				char fn[100];
				sprintf(fn, "/tmp/tile%d.vrt", (ytile * ntiles[0]) + xtile);
				*/

				/* edge x tile */
				if (!config->pad_tile && ntiles[0] > 1 && (xtile + 1) == ntiles[0])
					_tile_size[0] = info->dim[0] - (xtile * info->tile_size[0]);
				else
					_tile_size[0] = info->tile_size[0];

				/* compute tile's upper-left corner */
				GDALApplyGeoTransform(
					info->gt,
					xtile * info->tile_size[0], ytile * info->tile_size[1],
					&(gt[0]), &(gt[3])
				);
				/*
				rtinfo(_("tile (%d, %d) gt = (%f, %f, %f, %f, %f, %f)"),
					xtile, ytile,
					gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]
				);
				*/

				/* create VRT dataset */
				hdsDst = VRTCreate(_tile_size[0], _tile_size[1]);
				/*
  	 		GDALSetDescription(hdsDst, fn);
				*/
				GDALSetProjection(hdsDst, info->srs);
				GDALSetGeoTransform(hdsDst, gt);

				/* add bands as simple sources */
				for (i = 0; i < info->nband_count; i++) {
					GDALAddBand(hdsDst, info->gdalbandtype[i], NULL);
					hbandDst = (VRTSourcedRasterBandH) GDALGetRasterBand(hdsDst, i + 1);

					if (info->hasnodata[i])
						GDALSetRasterNoDataValue(hbandDst, info->nodataval[i]);

					VRTAddSimpleSource(
						hbandDst, GDALGetRasterBand(hdsSrc, info->nband[i]),
						xtile * info->tile_size[0], ytile * info->tile_size[1],
						_tile_size[0], _tile_size[1],
						0, 0,
						_tile_size[0], _tile_size[1],
						"near", VRT_NODATA_UNSET
					);
				}

				/* make sure VRT reflects all changes */
				VRTFlushCache(hdsDst);

				/* convert VRT dataset to rt_raster */
				rast = rt_raster_from_gdal_dataset(hdsDst);
				if (rast == NULL) {
					rterror(_("convert_raster: Could not convert VRT dataset to PostGIS raster"));
					GDALClose(hdsDst);
					return 0;
				}

				/* set srid if provided */
				rt_raster_set_srid(rast, info->srid);

				/* inspect each band of raster where band is NODATA */
				numbands = rt_raster_get_num_bands(rast);
				for (i = 0; i < numbands; i++) {
					band = rt_raster_get_band(rast, i);
					if (band != NULL && !config->skip_nodataval_check)
						rt_band_check_is_nodata(band);
				}

				/* convert rt_raster to hexwkb */
				hex = rt_raster_to_hexwkb(rast, FALSE, &hexlen);
				raster_destroy(rast);

				if (hex == NULL) {
					rterror(_("convert_raster: Could not convert PostGIS raster to hex WKB"));
					GDALClose(hdsDst);
					return 0;
				}

				/* add hexwkb to tileset */
				append_stringbuffer(tileset, hex);

				GDALClose(hdsDst);

				/* flush if tileset gets too big */
				if (tileset->length > 10) {
					if (!insert_records(
						config->schema, config->table, config->raster_column,
						(config->file_column ? config->rt_filename[idx] : NULL), config->file_column_name,
						config->copy_statements, config->out_srid,
						tileset, buffer
					)) {
						rterror(_("convert_raster: Could not convert raster tiles into INSERT or COPY statements"));
						GDALClose(hdsSrc);
						return 0;
					}

					rtdealloc_stringbuffer(tileset, 0);
				}
			}
		}

		GDALClose(hdsSrc);
	}

	return 1;
}

static int
process_rasters(RTLOADERCFG *config, STRINGBUFFER *buffer) {
	int i = 0;

	assert(config != NULL);
	assert(config->table != NULL);
	assert(config->raster_column != NULL);

	if (config->transaction) {
		if (!append_sql_to_buffer(buffer, strdup("BEGIN;"))) {
			rterror(_("process_rasters: Could not add BEGIN statement to string buffer"));
			return 0;
		}
	}

	/* drop table */
	if (config->opt == 'd') {
		if (!drop_table(config->schema, config->table, buffer)) {
			rterror(_("process_rasters: Could not add DROP TABLE statement to string buffer"));
			return 0;
		}

		if (config->overview_count) {
			for (i = 0; i < config->overview_count; i++) {
				if (!drop_table(config->schema, config->overview_table[i], buffer)) {
					rterror(_("process_rasters: Could not add an overview's DROP TABLE statement to string buffer"));
					return 0;
				}
			}
		}
	}

	/* create table */
	if (config->opt != 'a') {
		if (!create_table(
			config->schema, config->table, config->raster_column,
			config->file_column, config->file_column_name,
			config->tablespace, config->idx_tablespace,
			buffer
		)) {
			rterror(_("process_rasters: Could not add CREATE TABLE statement to string buffer"));
			return 0;
		}

		if (config->overview_count) {
			for (i = 0; i < config->overview_count; i++) {
				if (!create_table(
					config->schema, config->overview_table[i], config->raster_column,
					config->file_column, config->file_column_name,
					config->tablespace, config->idx_tablespace,
					buffer
				)) {
					rterror(_("process_rasters: Could not add an overview's CREATE TABLE statement to string buffer"));
					return 0;
				}
			}
		}
	}

	/* no need to run if opt is 'p' */
	if (config->opt != 'p') {
		RASTERINFO refinfo;
		init_rastinfo(&refinfo);

		/* process each raster */
		for (i = 0; i < config->rt_file_count; i++) {
			RASTERINFO rastinfo;
			STRINGBUFFER tileset;

			fprintf(stderr, _("Processing %d/%d: %s\n"), i + 1, config->rt_file_count, config->rt_file[i]);

			init_rastinfo(&rastinfo);
			init_stringbuffer(&tileset);

			/* convert raster */
			if (!convert_raster(i, config, &rastinfo, &tileset, buffer)) {
				rterror(_("process_rasters: Could not process raster: %s"), config->rt_file[i]);
				rtdealloc_rastinfo(&rastinfo);
				rtdealloc_stringbuffer(&tileset, 0);
				return 0;
			}

			/* process raster tiles into COPY or INSERT statements */
			if (tileset.length && !insert_records(
				config->schema, config->table, config->raster_column,
				(config->file_column ? config->rt_filename[i] : NULL),
        config->file_column_name,
				config->copy_statements, config->out_srid,
				&tileset, buffer
			)) {
				rterror(_("process_rasters: Could not convert raster tiles into INSERT or COPY statements"));
				rtdealloc_rastinfo(&rastinfo);
				rtdealloc_stringbuffer(&tileset, 0);
				return 0;
			}

			rtdealloc_stringbuffer(&tileset, 0);

			/* flush buffer after every raster */
			flush_stringbuffer(buffer);

			/* overviews */
			if (config->overview_count) {
				int j = 0;

				for (j = 0; j < config->overview_count; j++) {

					if (!build_overview(i, config, &rastinfo, j, &tileset, buffer)) {
						rterror(_("process_rasters: Could not create overview of factor %d for raster %s"), config->overview[j], config->rt_file[i]);
						rtdealloc_rastinfo(&rastinfo);
						rtdealloc_stringbuffer(&tileset, 0);
						return 0;
					}

					if (tileset.length && !insert_records(
						config->schema, config->overview_table[j], config->raster_column,
						(config->file_column ? config->rt_filename[i] : NULL), config->file_column_name,
						config->copy_statements, config->out_srid,
						&tileset, buffer
					)) {
						rterror(_("process_rasters: Could not convert overview tiles into INSERT or COPY statements"));
						rtdealloc_rastinfo(&rastinfo);
						rtdealloc_stringbuffer(&tileset, 0);
						return 0;
					}

					rtdealloc_stringbuffer(&tileset, 0);

					/* flush buffer after every raster */
					flush_stringbuffer(buffer);
				}
			}

			if (config->rt_file_count > 1) {
				if (i < 1)
					copy_rastinfo(&refinfo, &rastinfo);
				else {
					diff_rastinfo(&rastinfo, &refinfo);
				}
			}

			rtdealloc_rastinfo(&rastinfo);
		}

		rtdealloc_rastinfo(&refinfo);
	}

	/* index */
	if (config->idx) {
		/* create index */
		if (!create_index(
			config->schema, config->table, config->raster_column,
			config->idx_tablespace,
			buffer
		)) {
			rterror(_("process_rasters: Could not add CREATE INDEX statement to string buffer"));
			return 0;
		}

		/* analyze */
		if (config->opt != 'p') {
			if (!analyze_table(
				config->schema, config->table,
				buffer
			)) {
				rterror(_("process_rasters: Could not add ANALYZE statement to string buffer"));
				return 0;
			}
		}

		if (config->overview_count) {
			for (i = 0; i < config->overview_count; i++) {
				/* create index */
				if (!create_index(
					config->schema, config->overview_table[i], config->raster_column,
					config->idx_tablespace,
					buffer
				)) {
					rterror(_("process_rasters: Could not add an overview's CREATE INDEX statement to string buffer"));
					return 0;
				}

				/* analyze */
				if (config->opt != 'p') {
					if (!analyze_table(
						config->schema, config->overview_table[i],
						buffer
					)) {
						rterror(_("process_rasters: Could not add an overview's ANALYZE statement to string buffer"));
						return 0;
					}
				}
			}
		}
	}

	/* add constraints */
	if (config->constraints) {
		if (!add_raster_constraints(
			config->schema, config->table, config->raster_column,
			config->regular_blocking, config->max_extent,
			buffer
		)) {
			rterror(_("process:rasters: Could not add AddRasterConstraints statement to string buffer"));
			return 0;
		}

		if (config->overview_count) {
			for (i = 0; i < config->overview_count; i++) {
				if (!add_raster_constraints(
					config->schema, config->overview_table[i], config->raster_column,
					config->regular_blocking, config->max_extent,
					buffer
				)) {
					rterror(_("process_rasters: Could not add an overview's AddRasterConstraints statement to string buffer"));
					return 0;
				}
			}
		}
	}

	/* overview constraint is automatically added */
	if (config->overview_count) {
		for (i = 0; i < config->overview_count; i++) {
			if (!add_overview_constraints(
				config->schema, config->overview_table[i], config->raster_column,
				config->schema, config->table, config->raster_column,
				config->overview[i],
				buffer
			)) {
				rterror(_("process_rasters: Could not add an overview's AddOverviewConstraints statement to string buffer"));
				return 0;
			}
		}
	}

	if (config->transaction) {
		if (!append_sql_to_buffer(buffer, strdup("END;"))) {
			rterror(_("process_rasters: Could not add END statement to string buffer"));
			return 0;
		}
	}

	/* maintenance */
	if (config->opt != 'p' && config->maintenance) {
		if (!vacuum_table(
			config->schema, config->table,
			buffer
		)) {
			rterror(_("process_rasters: Could not add VACUUM statement to string buffer"));
			return 0;
		}

		if (config->overview_count) {
			for (i = 0; i < config->overview_count; i++) {
				if (!vacuum_table(
					config->schema, config->overview_table[i],
					buffer
				)) {
					rterror(_("process_rasters: Could not add an overview's VACUUM statement to string buffer"));
					return 0;
				}
			}
		}

	}

	return 1;
}

int
main(int argc, char **argv) {
	RTLOADERCFG *config = NULL;
	STRINGBUFFER *buffer = NULL;
	int i = 0;
	int j = 0;
	char **elements = NULL;
	int n = 0;
	GDALDriverH drv = NULL;
	char *tmp = NULL;

#ifdef USE_NLS
	setlocale (LC_ALL, "");
	bindtextdomain (PACKAGE, LOCALEDIR);
	textdomain (PACKAGE);
#endif

	/* no args, show usage */
	if (argc == 1) {
		usage();
		exit(0);
	}

	/* initialize config */
	config = rtalloc(sizeof(RTLOADERCFG));
	if (config == NULL) {
		rterror(_("Could not allocate memory for loader configuration"));
		exit(1);
	}
	init_config(config);

	/****************************************************************************
	* parse arguments
	****************************************************************************/

	for (i = 1; i < argc; i++) {
		char *optarg, *ptr;
		/* srid */
		if (CSEQUAL(argv[i], "-s") && i < argc - 1) {
			optarg = argv[++i];
			ptr = strchr(optarg, ':');
			if (ptr) {
				*ptr++ = '\0';
				sscanf(optarg, "%d", &config->srid);
				sscanf(ptr, "%d", &config->out_srid);
			} else {
				config->srid = config->out_srid = atoi(optarg);
			}
		}
		/* band index */
		else if (CSEQUAL(argv[i], "-b") && i < argc - 1) {
			elements = strsplit(argv[++i], ",", &n);
			if (n < 1) {
				rterror(_("Could not process -b"));
				rtdealloc_config(config);
				exit(1);
			}

			config->nband_count = 0;
			for (j = 0; j < n; j++) {
				char *t = trim(elements[j]);
				char **minmax = NULL;
				int *range = NULL;
				int p = 0;
				int l = 0;
				int m = 0;
				int o = 0;

				/* is t a range? */
				minmax = strsplit(t, "-", &o);
				if (o == 2) {
					if (!array_range(atoi(minmax[0]), atoi(minmax[1]), 1, &range, &p)) {
						rterror(_("Could not allocate memory for storing band indices"));
						for (l = 0; l < o; l++)
							rtdealloc(minmax[l]);
						rtdealloc(minmax);
						for (j = 0; j < n; j++)
							rtdealloc(elements[j]);
						rtdealloc(elements);
						rtdealloc(t);
						rtdealloc_config(config);
						exit(1);
					}
				}
				else {
					p = 1;
					range = rtalloc(sizeof(int));
					if (range == NULL) {
						rterror(_("Could not allocate memory for storing band indices"));
						for (l = 0; l < o; l++)
							rtdealloc(minmax[l]);
						rtdealloc(minmax);
						for (j = 0; j < n; j++)
							rtdealloc(elements[j]);
						rtdealloc(elements);
						rtdealloc(t);
						rtdealloc_config(config);
						exit(1);
					}
					*range = atoi(t);
				}

				m = config->nband_count;
				config->nband_count += p;
				config->nband = rtrealloc(config->nband, sizeof(int) * config->nband_count);
				if (config->nband == NULL) {
					rterror(_("Could not allocate memory for storing band indices"));
					rtdealloc(range);
					for (l = 0; l < o; l++)
						rtdealloc(minmax[l]);
					rtdealloc(minmax);
					for (j = 0; j < n; j++)
						rtdealloc(elements[j]);
					rtdealloc(elements);
					rtdealloc(t);
					rtdealloc_config(config);
					exit(1);
				}

				for (l = 0; l < p; l++, m++)
					config->nband[m] = range[l];

				rtdealloc(range);

				for (l = 0; l < o; l++)
					rtdealloc(minmax[l]);
				rtdealloc(minmax);

				rtdealloc(t);
				rtdealloc(elements[j]);
			}
			rtdealloc(elements);
			elements = NULL;
			n = 0;

			for (j = 0; j < config->nband_count; j++) {
				if (config->nband[j] < 1) {
					rterror(_("Band index %d must be greater than 0"), config->nband[j]);
					rtdealloc_config(config);
					exit(1);
				}
			}
		}
		/* tile size */
		else if (CSEQUAL(argv[i], "-t") && i < argc - 1) {
			if (CSEQUAL(argv[++i], "auto")) {
				config->tile_size[0] = -1;
				config->tile_size[1] = -1;
			}
			else {
				elements = strsplit(argv[i], "x", &n);
				if (n != 2) {
					rterror(_("Could not process -t"));
					rtdealloc_config(config);
					exit(1);
				}

				for (j = 0; j < n; j++) {
					char *t = trim(elements[j]);
					config->tile_size[j] = atoi(t);
					rtdealloc(t);
					rtdealloc(elements[j]);
				}
				rtdealloc(elements);
				elements = NULL;
				n = 0;

				for (j = 0; j < 2; j++) {
					if (config->tile_size[j] < 1) {
						rterror(_("Tile size must be greater than 0x0"));
						rtdealloc_config(config);
						exit(1);
					}
				}
			}
		}
		/* pad tiles */
		else if (CSEQUAL(argv[i], "-P")) {
			config->pad_tile = 1;
		}
		/* out-of-db raster */
		else if (CSEQUAL(argv[i], "-R")) {
			config->outdb = 1;
		}
		/* drop table and recreate */
		else if (CSEQUAL(argv[i], "-d")) {
			config->opt = 'd';
		}
		/* append to table */
		else if (CSEQUAL(argv[i], "-a")) {
			config->opt = 'a';
		}
		/* create new table */
		else if (CSEQUAL(argv[i], "-c")) {
			config->opt = 'c';
		}
		/* prepare only */
		else if (CSEQUAL(argv[i], "-p")) {
			config->opt = 'p';
		}
		/* raster column name */
		else if (CSEQUAL(argv[i], "-f") && i < argc - 1) {
			config->raster_column = rtalloc(sizeof(char) * (strlen(argv[++i]) + 1));
			if (config->raster_column == NULL) {
				rterror(_("Could not allocate memory for storing raster column name"));
				rtdealloc_config(config);
				exit(1);
			}
			strncpy(config->raster_column, argv[i], strlen(argv[i]) + 1);
		}
		/* filename column */
		else if (CSEQUAL(argv[i], "-F")) {
			config->file_column = 1;
		}
		/* filename column name */
		else if (CSEQUAL(argv[i], "-n") && i < argc - 1) {
			config->file_column_name = rtalloc(sizeof(char) * (strlen(argv[++i]) + 1));
			if (config->file_column_name == NULL) {
				rterror(_("Could not allocate memory for storing filename column name"));
				rtdealloc_config(config);
				exit(1);
			}
			strncpy(config->file_column_name, argv[i], strlen(argv[i]) + 1);
			config->file_column = 1;
		}
		/* overview factors */
		else if (CSEQUAL(argv[i], "-l") && i < argc - 1) {
			elements = strsplit(argv[++i], ",", &n);
			if (n < 1) {
				rterror(_("Could not process -l"));
				rtdealloc_config(config);
				exit(1);
			}

			config->overview_count = n;
			config->overview = rtalloc(sizeof(int) * n);
			if (config->overview == NULL) {
				rterror(_("Could not allocate memory for storing overview factors"));
				rtdealloc_config(config);
				exit(1);
			}
			for (j = 0; j < n; j++) {
				char *t = trim(elements[j]);
				config->overview[j] = atoi(t);
				rtdealloc(t);
				rtdealloc(elements[j]);
			}
			rtdealloc(elements);
			elements = NULL;
			n = 0;

			for (j = 0; j < config->overview_count; j++) {
				if (config->overview[j] < MINOVFACTOR || config->overview[j] > MAXOVFACTOR) {
					rterror(_("Overview factor %d is not between %d and %d"), config->overview[j], MINOVFACTOR, MAXOVFACTOR);
					rtdealloc_config(config);
					exit(1);
				}
			}
		}
		/* quote identifiers */
		else if (CSEQUAL(argv[i], "-q")) {
			config->quoteident = 1;
		}
		/* create index */
		else if (CSEQUAL(argv[i], "-I")) {
			config->idx = 1;
		}
		/* maintenance */
		else if (CSEQUAL(argv[i], "-M")) {
			config->maintenance = 1;
		}
		/* set constraints */
		else if (CSEQUAL(argv[i], "-C")) {
			config->constraints = 1;
		}
		/* disable extent constraint */
		else if (CSEQUAL(argv[i], "-x")) {
			config->max_extent = 0;
		}
		/* enable regular_blocking */
		else if (CSEQUAL(argv[i], "-r")) {
			config->regular_blocking = 1;
		}
		/* tablespace of new table */
		else if (CSEQUAL(argv[i], "-T") && i < argc - 1) {
			config->tablespace = rtalloc(sizeof(char) * (strlen(argv[++i]) + 1));
			if (config->tablespace == NULL) {
				rterror(_("Could not allocate memory for storing tablespace of new table"));
				rtdealloc_config(config);
				exit(1);
			}
			strncpy(config->tablespace, argv[i], strlen(argv[i]) + 1);
		}
		/* tablespace of new index */
		else if (CSEQUAL(argv[i], "-X") && i < argc - 1) {
			config->idx_tablespace = rtalloc(sizeof(char) * (strlen(argv[++i]) + 1));
			if (config->idx_tablespace == NULL) {
				rterror(_("Could not allocate memory for storing tablespace of new indices"));
				rtdealloc_config(config);
				exit(1);
			}
			strncpy(config->idx_tablespace, argv[i], strlen(argv[i]) + 1);
		}
		/* nodata value */
		else if (CSEQUAL(argv[i], "-N") && i < argc - 1) {
			config->hasnodata = 1;
			config->nodataval = atof(argv[++i]);
		}
		/* skip NODATA value check for bands */
		else if (CSEQUAL(argv[i], "-k")) {
			config->skip_nodataval_check = 1;
		}
		/* endianness */
		else if (CSEQUAL(argv[i], "-E") && i < argc - 1) {
			config->endian = atoi(argv[++i]);
			config->endian = 1;
		}
		/* version */
		else if (CSEQUAL(argv[i], "-V") && i < argc - 1) {
			config->version = atoi(argv[++i]);
			config->version = 0;
		}
		/* transaction */
		else if (CSEQUAL(argv[i], "-e")) {
			config->transaction = 0;
		}
		/* COPY statements */
		else if (CSEQUAL(argv[i], "-Y")) {
			config->copy_statements = 1;
		}
		/* GDAL formats */
		else if (CSEQUAL(argv[i], "-G")) {
			uint32_t drv_count = 0;
			rt_gdaldriver drv_set = rt_raster_gdal_drivers(&drv_count, 0);
			if (drv_set == NULL || !drv_count) {
				rterror(_("Could not get list of available GDAL raster formats"));
			}
			else {
				printf(_("Supported GDAL raster formats:\n"));
				for (j = 0; j < drv_count; j++) {
					printf(_("  %s\n"), drv_set[j].long_name);

					rtdealloc(drv_set[j].short_name);
					rtdealloc(drv_set[j].long_name);
					rtdealloc(drv_set[j].create_options);
				}
				rtdealloc(drv_set);
			}

			rtdealloc_config(config);
			exit(0);
		}
		/* help */
		else if (CSEQUAL(argv[i], "-?")) {
			usage();
			rtdealloc_config(config);
			exit(0);
		}
		else {
			config->rt_file_count++;
			config->rt_file = (char **) rtrealloc(config->rt_file, sizeof(char *) * config->rt_file_count);
			if (config->rt_file == NULL) {
				rterror(_("Could not allocate memory for storing raster files"));
				rtdealloc_config(config);
				exit(1);
			}

			config->rt_file[config->rt_file_count - 1] = rtalloc(sizeof(char) * (strlen(argv[i]) + 1));
			if (config->rt_file[config->rt_file_count - 1] == NULL) {
				rterror(_("Could not allocate memory for storing raster filename"));
				rtdealloc_config(config);
				exit(1);
			}
			strncpy(config->rt_file[config->rt_file_count - 1], argv[i], strlen(argv[i]) + 1);
		}
	}

	if (config->srid != config->out_srid) {
		if (config->copy_statements) {
			rterror(_("Invalid argument combination - cannot use -Y with -s FROM_SRID:TO_SRID"));
			exit(1);
		}
		if (config->out_srid == SRID_UNKNOWN) {
			rterror(_("Unknown target SRID is invalid when source SRID is given"));
			exit(1);
		}
	}

	/* register GDAL drivers */
	GDALAllRegister();

	/* no files provided */
	if (!config->rt_file_count) {
		rterror(_("No raster provided"));
		rtdealloc_config(config);
		exit(1);
	}
	/*
		at least two files, see if last is table
		last isn't recognized by GDAL
	*/
	else if (config->rt_file_count > 1) {
		drv = GDALIdentifyDriver(config->rt_file[config->rt_file_count - 1], NULL);

		if (drv == NULL) {
			char *ptr;
			ptr = strchr(config->rt_file[config->rt_file_count - 1], '.');

			/* schema.table */
			if (ptr) {
				config->schema = rtalloc(sizeof(char) * (ptr - config->rt_file[config->rt_file_count - 1] + 1));
				if (config->schema == NULL) {
					rterror(_("Could not allocate memory for storing schema name"));
					rtdealloc_config(config);
					exit(1);
				}
				snprintf(config->schema, ptr - config->rt_file[config->rt_file_count - 1] + 1, "%s", config->rt_file[config->rt_file_count - 1]);
				config->schema[ptr - config->rt_file[config->rt_file_count - 1]] = '\0';

				config->table = rtalloc(sizeof(char) * (strlen(config->rt_file[config->rt_file_count - 1]) - strlen(config->schema) + 1));
				if (config->table == NULL) {
					rterror(_("Could not allocate memory for storing table name"));
					rtdealloc_config(config);
					exit(1);
				}
				snprintf(config->table, strlen(config->rt_file[config->rt_file_count - 1]) - strlen(config->schema), "%s", ptr + 1);
				config->table[strlen(config->rt_file[config->rt_file_count - 1]) - strlen(config->schema)] = '\0';
			}
			/* table */
			else {
				config->table = rtalloc(sizeof(char) * strlen(config->rt_file[config->rt_file_count - 1]) + 1);
				if (config->table == NULL) {
					rterror(_("Could not allocate memory for storing table name"));
					rtdealloc_config(config);
					exit(1);
				}
				strncpy(config->table, config->rt_file[config->rt_file_count - 1], strlen(config->rt_file[config->rt_file_count - 1]) + 1);
			}

			rtdealloc(config->rt_file[--(config->rt_file_count)]);
			config->rt_file = (char **) rtrealloc(config->rt_file, sizeof(char *) * config->rt_file_count);
			if (config->rt_file == NULL) {
				rterror(_("Could not reallocate the memory holding raster names"));
				rtdealloc_config(config);
				exit(1);
			}
		}
	}

	/****************************************************************************
	* validate raster files
	****************************************************************************/

	/* check that GDAL recognizes all files */
	for (i = 0; i < config->rt_file_count; i++) {
		drv = GDALIdentifyDriver(config->rt_file[i], NULL);

		if (drv == NULL) {
			rterror(_("Unable to read raster file: %s"), config->rt_file[i]);
			rtdealloc_config(config);
			exit(1);
		}
	}

	/* process each file for just the filename */
	config->rt_filename = (char **) rtalloc(sizeof(char *) * config->rt_file_count);
	if (config->rt_filename == NULL) {
		rterror(_("Could not allocate memory for cleaned raster filenames"));
		rtdealloc_config(config);
		exit(1);
	}
	for (i = 0; i < config->rt_file_count; i++) {
		char *file;
		char *ptr;

		file = rtalloc(sizeof(char) * (strlen(config->rt_file[i]) + 1));
		if (file == NULL) {
			rterror(_("Could not allocate memory for cleaned raster filename"));
			rtdealloc_config(config);
			exit(1);
		}
		strcpy(file, config->rt_file[i]);

		for (ptr = file + strlen(file); ptr > file; ptr--) {
			if (*ptr == '/' || *ptr == '\\') {
				ptr++;
				break;
			}
		}

		config->rt_filename[i] = rtalloc(sizeof(char) * (strlen(ptr) + 1));
		if (config->rt_filename[i] == NULL) {
			rterror(_("Could not allocate memory for cleaned raster filename"));
			rtdealloc_config(config);
			exit(1);
		}
		strcpy(config->rt_filename[i], ptr);
		rtdealloc(file);
	}

	/****************************************************************************
	* defaults for table and column names
	****************************************************************************/

	/* first file as proxy table name */
	if (config->table == NULL) {
		char *file;
		char *ptr;

		file = rtalloc(sizeof(char) * (strlen(config->rt_filename[0]) + 1));
		if (file == NULL) {
			rterror(_("Could not allocate memory for proxy table name"));
			rtdealloc_config(config);
			exit(1);
		}
		strcpy(file, config->rt_filename[0]);

		for (ptr = file + strlen(file); ptr > file; ptr--) {
			if (*ptr == '.') {
				*ptr = '\0';
				break;
			}
		}

		config->table = rtalloc(sizeof(char) * (strlen(file) + 1));
		if (config->table == NULL) {
			rterror(_("Could not allocate memory for proxy table name"));
			rtdealloc_config(config);
			exit(1);
		}
		strcpy(config->table, file);
		rtdealloc(file);
	}

	/* raster_column not specified, default to "rast" */
	if (config->raster_column == NULL) {
		config->raster_column = rtalloc(sizeof(char) * (strlen("rast") + 1));
		if (config->raster_column == NULL) {
			rterror(_("Could not allocate memory for default raster column name"));
			rtdealloc_config(config);
			exit(1);
		}
		strcpy(config->raster_column, "rast");
	}

	/* file_column_name not specified, default to "filename" */
	if (config->file_column_name == NULL) {
		config->file_column_name = rtalloc(sizeof(char) * (strlen("filename") + 1));
		if (config->file_column_name == NULL) {
			rterror(_("Could not allocate memory for default filename column name"));
			rtdealloc_config(config);
			exit(1);
		}
		strcpy(config->file_column_name, "filename");
	}

	/****************************************************************************
	* literal PostgreSQL identifiers disabled
	****************************************************************************/

	/* no quotes, lower case everything */
	if (!config->quoteident) {
		if (config->schema != NULL)
			config->schema = strtolower(config->schema);
		if (config->table != NULL)
			config->table = strtolower(config->table);
		if (config->raster_column != NULL)
			config->raster_column = strtolower(config->raster_column);
		if (config->file_column_name != NULL)
			config->file_column_name = strtolower(config->file_column_name);
		if (config->tablespace != NULL)
			config->tablespace = strtolower(config->tablespace);
		if (config->idx_tablespace != NULL)
			config->idx_tablespace = strtolower(config->idx_tablespace);
	}

	/****************************************************************************
	* overview table names
	****************************************************************************/

	if (config->overview_count) {
		char factor[4];
		config->overview_table = rtalloc(sizeof(char *) * config->overview_count);
		if (config->overview_table == NULL) {
			rterror(_("Could not allocate memory for overview table names"));
			rtdealloc_config(config);
			exit(1);
		}

		for (i = 0; i < config->overview_count; i++) {
			sprintf(factor, "%d", config->overview[i]);

			config->overview_table[i] = rtalloc(sizeof(char) * (strlen("o__") + strlen(factor) + strlen(config->table) + 1));
			if (config->overview_table[i] == NULL) {
				rterror(_("Could not allocate memory for overview table name"));
				rtdealloc_config(config);
				exit(1);
			}
			sprintf(config->overview_table[i], "o_%d_%s", config->overview[i], config->table);
		}
	}

	/****************************************************************************
	* check that identifiers won't get truncated
	****************************************************************************/

	if (config->schema != NULL && strlen(config->schema) > MAXNAMELEN) {
		rtwarn(_("The schema name \"%s\" may exceed the maximum string length permitted for PostgreSQL identifiers (%d)"),
			config->schema,
			MAXNAMELEN
		);
	}
	if (config->table != NULL && strlen(config->table) > MAXNAMELEN) {
		rtwarn(_("The table name \"%s\" may exceed the maximum string length permitted for PostgreSQL identifiers (%d)"),
			config->table,
			MAXNAMELEN
		);
	}
	if (config->raster_column != NULL && strlen(config->raster_column) > MAXNAMELEN) {
		rtwarn(_("The column name \"%s\" may exceed the maximum string length permitted for PostgreSQL identifiers (%d)"),
			config->raster_column,
			MAXNAMELEN
		);
	}
	if (config->file_column_name != NULL && strlen(config->file_column_name) > MAXNAMELEN) {
		rtwarn(_("The column name \"%s\" may exceed the maximum string length permitted for PostgreSQL identifiers (%d)"),
			config->file_column_name,
			MAXNAMELEN
		);
	}
	if (config->tablespace != NULL && strlen(config->tablespace) > MAXNAMELEN) {
		rtwarn(_("The tablespace name \"%s\" may exceed the maximum string length permitted for PostgreSQL identifiers (%d)"),
			config->tablespace,
			MAXNAMELEN
		);
	}
	if (config->idx_tablespace != NULL && strlen(config->idx_tablespace) > MAXNAMELEN) {
		rtwarn(_("The index tablespace name \"%s\" may exceed the maximum string length permitted for PostgreSQL identifiers (%d)"),
			config->idx_tablespace,
			MAXNAMELEN
		);
	}
	if (config->overview_count) {
		for (i = 0; i < config->overview_count; i++) {
			if (strlen(config->overview_table[i]) > MAXNAMELEN) {
				rtwarn(_("The overview table name \"%s\" may exceed the maximum string length permitted for PostgreSQL identifiers (%d)"),
					config->overview_table[i],
					MAXNAMELEN
				);
			}
		}
	}

	/****************************************************************************
	* double quote identifiers
	****************************************************************************/

	if (config->schema != NULL) {
		tmp = rtalloc(sizeof(char) * (strlen(config->schema) + 4));
		if (tmp == NULL) {
			rterror(_("Could not allocate memory for quoting schema name"));
			rtdealloc_config(config);
			exit(1);
		}

		sprintf(tmp, "\"%s\".", config->schema);
		rtdealloc(config->schema);
		config->schema = tmp;
	}
	if (config->table != NULL) {
		tmp = rtalloc(sizeof(char) * (strlen(config->table) + 3));
		if (tmp == NULL) {
			rterror(_("Could not allocate memory for quoting table name"));
			rtdealloc_config(config);
			exit(1);
		}

		sprintf(tmp, "\"%s\"", config->table);
		rtdealloc(config->table);
		config->table = tmp;
	}
	if (config->raster_column != NULL) {
		tmp = rtalloc(sizeof(char) * (strlen(config->raster_column) + 3));
		if (tmp == NULL) {
			rterror(_("Could not allocate memory for quoting raster column name"));
			rtdealloc_config(config);
			exit(1);
		}

		sprintf(tmp, "\"%s\"", config->raster_column);
		rtdealloc(config->raster_column);
		config->raster_column = tmp;
	}
	if (config->file_column_name != NULL) {
		tmp = rtalloc(sizeof(char) * (strlen(config->file_column_name) + 3));
		if (tmp == NULL) {
			rterror(_("Could not allocate memory for quoting raster column name"));
			rtdealloc_config(config);
			exit(1);
		}

		sprintf(tmp, "\"%s\"", config->file_column_name);
		rtdealloc(config->file_column_name);
		config->file_column_name = tmp;
	}
	if (config->tablespace != NULL) {
		tmp = rtalloc(sizeof(char) * (strlen(config->tablespace) + 3));
		if (tmp == NULL) {
			rterror(_("Could not allocate memory for quoting tablespace name"));
			rtdealloc_config(config);
			exit(1);
		}

		sprintf(tmp, "\"%s\"", config->tablespace);
		rtdealloc(config->tablespace);
		config->tablespace = tmp;
	}
	if (config->idx_tablespace != NULL) {
		tmp = rtalloc(sizeof(char) * (strlen(config->idx_tablespace) + 3));
		if (tmp == NULL) {
			rterror(_("Could not allocate memory for quoting index tablespace name"));
			rtdealloc_config(config);
			exit(1);
		}

		sprintf(tmp, "\"%s\"", config->idx_tablespace);
		rtdealloc(config->idx_tablespace);
		config->idx_tablespace = tmp;
	}
	if (config->overview_count) {
		for (i = 0; i < config->overview_count; i++) {
			tmp = rtalloc(sizeof(char) * (strlen(config->overview_table[i]) + 3));
			if (tmp == NULL) {
				rterror(_("Could not allocate memory for quoting overview table name"));
				rtdealloc_config(config);
				exit(1);
			}

			sprintf(tmp, "\"%s\"", config->overview_table[i]);
			rtdealloc(config->overview_table[i]);
			config->overview_table[i] = tmp;
		}
	}

	/****************************************************************************
	* processing of rasters
	****************************************************************************/

	/* initialize string buffer */
	buffer = rtalloc(sizeof(STRINGBUFFER));
	if (buffer == NULL) {
		rterror(_("Could not allocate memory for output string buffer"));
		rtdealloc_config(config);
		exit(1);
	}
	init_stringbuffer(buffer);

	/* pass off to processing function */
	if (!process_rasters(config, buffer)) {
		rterror(_("Unable to process rasters"));
		rtdealloc_stringbuffer(buffer, 1);
		rtdealloc_config(config);
		exit(1);
	}

	flush_stringbuffer(buffer);

	rtdealloc_stringbuffer(buffer, 1);
	rtdealloc_config(config);

	return 0;
}
