/**********************************************************************
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <string.h>
#include "liblwgeom_internal.h"

/* Ensures the given lat and lon are in the "normal" range:
 * -90 to +90 for lat, -180 to +180 for lon. */
static void lwprint_normalize_latlon(double *lat, double *lon)
{
	/* First remove all the truly excessive trips around the world via up or down. */
	while (*lat > 270)
	{
		*lat -= 360;
	}
	while (*lat < -270)
	{
		*lat += 360;
	}

	/* Now see if latitude is past the top or bottom of the world.
	 * Past 90  or -90 puts us on the other side of the earth,
	     * so wrap latitude and add 180 to longitude to reflect that. */
	if (*lat > 90)
	{
		*lat = 180 - *lat;
		*lon += 180;
	}
	if (*lat < -90)
	{
		*lat = -180 - *lat;
		*lon += 180;
	}
	/* Now make sure lon is in the normal range.  Wrapping longitude
	 * has no effect on latitude. */
	while (*lon > 180)
	{
		*lon -= 360;
	}
	while (*lon < -180)
	{
		*lon += 360;
	}
}

/* Converts a single double to DMS given the specified DMS format string.
 * Symbols are specified since N/S or E/W are the only differences when printing
 * lat vs. lon.  They are only used if the "C" (compass dir) token appears in the
 * format string.
 * NOTE: Format string and symbols are required to be in UTF-8. */
static char * lwdouble_to_dms(double val, const char *pos_dir_symbol, const char *neg_dir_symbol, const char * format)
{
	/* 3 numbers, 1 sign or compass dir, and 5 possible strings (degree signs, spaces, misc text, etc) between or around them.*/
	const int NUM_PIECES = 9;
	const int WORK_SIZE = 1024;
	char pieces[NUM_PIECES][WORK_SIZE];
	int current_piece = 0;
	int is_negative = 0;

	double degrees = 0.0;
	double minutes = 0.0;
	double seconds = 0.0;

	int compass_dir_piece = -1;

	int reading_deg = 0;
	int deg_digits = 0;
	int deg_has_decpoint = 0;
	int deg_dec_digits = 0;
	int deg_piece = -1;

	int reading_min = 0;
	int min_digits = 0;
	int min_has_decpoint = 0;
	int min_dec_digits = 0;
	int min_piece = -1;

	int reading_sec = 0;
	int sec_digits = 0;
	int sec_has_decpoint = 0;
	int sec_dec_digits = 0;
	int sec_piece = -1;

	int format_length = ((NULL == format) ? 0 : strlen(format));

	char * result;

	int index, following_byte_index;
	int multibyte_char_width = 1;

	/* Initialize the working strs to blank.  We may not populate all of them, and
	 * this allows us to concat them all at the end without worrying about how many
	 * we actually needed. */
	for (index = 0; index < NUM_PIECES; index++)
	{
		pieces[index][0] = '\0';
	}

	/* If no format is provided, use a default. */
	if (0 == format_length)
	{
		/* C2B0 is UTF-8 for the degree symbol. */
		format = "D\xC2\xB0""M'S.SSS\"C";
		format_length = strlen(format);
	}
	else if (format_length > WORK_SIZE)
	{
		/* Sanity check, we don't want to overwrite an entire piece of work and no one should need a 1K-sized
		* format string anyway. */
		lwerror("Bad format, exceeds maximum length (%d).", WORK_SIZE);
	}

	for (index = 0; index < format_length; index++)
	{
		char next_char = format[index];
		switch (next_char)
		{
		case 'D':
			if (reading_deg)
			{
				/* If we're reading degrees, add another digit. */
				deg_has_decpoint ? deg_dec_digits++ : deg_digits++;
			}
			else
			{
				/* If we're not reading degrees, we are now. */
				current_piece++;
				deg_piece = current_piece;
				if (deg_digits > 0)
				{
					lwerror("Bad format, cannot include degrees (DD.DDD) more than once.");
				}
				reading_deg = 1;
				reading_min = 0;
				reading_sec = 0;
				deg_digits++;
			}
			break;
		case 'M':
			if (reading_min)
			{
				/* If we're reading minutes, add another digit. */
				min_has_decpoint ? min_dec_digits++ : min_digits++;
			}
			else
			{
				/* If we're not reading minutes, we are now. */
				current_piece++;
				min_piece = current_piece;
				if (min_digits > 0)
				{
					lwerror("Bad format, cannot include minutes (MM.MMM) more than once.");
				}
				reading_deg = 0;
				reading_min = 1;
				reading_sec = 0;
				min_digits++;
			}
			break;
		case 'S':
			if (reading_sec)
			{
				/* If we're reading seconds, add another digit. */
				sec_has_decpoint ? sec_dec_digits++ : sec_digits++;
			}
			else
			{
				/* If we're not reading seconds, we are now. */
				current_piece++;
				sec_piece = current_piece;
				if (sec_digits > 0)
				{
					lwerror("Bad format, cannot include seconds (SS.SSS) more than once.");
				}
				reading_deg = 0;
				reading_min = 0;
				reading_sec = 1;
				sec_digits++;
			}
			break;
		case 'C':
			/* We're done reading anything else we might have been reading. */
			if (reading_deg || reading_min || reading_sec)
			{
				/* We were reading something, that means this is the next piece. */
				reading_deg = 0;
				reading_min = 0;
				reading_sec = 0;
			}
			current_piece++;

			if (compass_dir_piece >= 0)
			{
				lwerror("Bad format, cannot include compass dir (C) more than once.");
			}
			/* The compass dir is a piece all by itself.  */
			compass_dir_piece = current_piece;
			current_piece++;
			break;
		case '.':
			/* If we're reading deg, min, or sec, we want a decimal point for it. */
			if (reading_deg)
			{
				deg_has_decpoint = 1;
			}
			else if (reading_min)
			{
				min_has_decpoint = 1;
			}
			else if (reading_sec)
			{
				sec_has_decpoint = 1;
			}
			else
			{
				/* Not reading anything, just pass through the '.' */
				strncat(pieces[current_piece], &next_char, 1);
			}
			break;
		default:
			/* Any other char is just passed through unchanged.  But it does mean we are done reading D, M, or S.*/
			if (reading_deg || reading_min || reading_sec)
			{
				/* We were reading something, that means this is the next piece. */
				current_piece++;
				reading_deg = 0;
				reading_min = 0;
				reading_sec = 0;
			}

			/* Check if this is a multi-byte UTF-8 character.  If so go ahead and read the rest of the bytes as well. */
			multibyte_char_width = 1;
			if (next_char & 0x80)
			{
				if ((next_char & 0xF8) == 0xF0)
				{
					multibyte_char_width += 3;
				}
				else if ((next_char & 0xF0) == 0xE0)
				{
					multibyte_char_width += 2;
				}
				else if ((next_char & 0xE0) == 0xC0)
				{
					multibyte_char_width += 1;
				}
				else
				{
					lwerror("Bad format, invalid high-order byte found first, format string may not be UTF-8.");
				}
			}
			if (multibyte_char_width > 1)
			{
				if (index + multibyte_char_width >= format_length)
				{
					lwerror("Bad format, UTF-8 character first byte found with insufficient following bytes, format string may not be UTF-8.");
				}
				for (following_byte_index = (index + 1); following_byte_index < (index + multibyte_char_width); following_byte_index++)
				{
					if ((format[following_byte_index] & 0xC0) != 0x80)
					{
						lwerror("Bad format, invalid byte found following leading byte of multibyte character, format string may not be UTF-8.");
					}
				}
			}
			/* Copy all the character's bytes into the current piece. */
			strncat(pieces[current_piece], &(format[index]), multibyte_char_width);
			/* Now increment index past the rest of those bytes. */
			index += multibyte_char_width - 1;
			break;
		}
		if (current_piece >= NUM_PIECES)
		{
			lwerror("Internal error, somehow needed more pieces than it should.");
		}
	}
	if (deg_piece < 0)
	{
		lwerror("Bad format, degrees (DD.DDD) must be included.");
	}

	/* Divvy the number up into D, DM, or DMS */
	if (val < 0)
	{
		val *= -1;
		is_negative = 1;
	}
	degrees = val;
	if (min_digits > 0)
	{
		degrees = (long)degrees;
		minutes = (val - degrees) * 60;
	}
	if (sec_digits > 0)
	{
		if (0 == min_digits)
		{
			lwerror("Bad format, cannot include seconds (SS.SSS) without including minutes (MM.MMM).");
		}
		minutes = (long)minutes;
		seconds = (val - (degrees + (minutes / 60))) * 3600;
	}

	/* Handle the compass direction.  If not using compass dir, display degrees as a positive/negative number. */
	if (compass_dir_piece >= 0)
	{
		strcpy(pieces[compass_dir_piece], is_negative ? neg_dir_symbol : pos_dir_symbol);
	}
	else if (is_negative)
	{
		degrees *= -1;
	}

	/* Format the degrees into their string piece. */
	if (deg_digits + deg_dec_digits + 2 > WORK_SIZE)
	{
		lwerror("Bad format, degrees (DD.DDD) number of digits was greater than our working limit.");
	}
	sprintf(pieces[deg_piece], "%*.*f", deg_digits, deg_dec_digits, degrees);

	if (min_piece >= 0)
	{
		/* Format the minutes into their string piece. */
		if (min_digits + min_dec_digits + 2 > WORK_SIZE)
		{
			lwerror("Bad format, minutes (MM.MMM) number of digits was greater than our working limit.");
		}
		sprintf(pieces[min_piece], "%*.*f", min_digits, min_dec_digits, minutes);
	}
	if (sec_piece >= 0)
	{
		/* Format the seconds into their string piece. */
		if (sec_digits + sec_dec_digits + 2 > WORK_SIZE)
		{
			lwerror("Bad format, seconds (SS.SSS) number of digits was greater than our working limit.");
		}
		sprintf(pieces[sec_piece], "%*.*f", sec_digits, sec_dec_digits, seconds);
	}

	/* Allocate space for the result.  Leave plenty of room for excess digits, negative sign, etc.*/
	result = (char*)lwalloc(format_length + WORK_SIZE);
	/* Append all the pieces together. There may be less than 9, but in that case the rest will be blank. */
	strcpy(result, pieces[0]);
	for (index = 1; index < NUM_PIECES; index++)
	{
		strcat(result, pieces[index]);
	}

	return result;
}

/* Print two doubles (lat and lon) in DMS form using the specified format.
 * First normalizes them so they will display as -90 to 90 and -180 to 180.
 * Format string may be null or 0-length, in which case a default format will be used.
 * NOTE: Format string is required to be in UTF-8.
 * NOTE2: returned string is lwalloc'ed, caller is responsible to lwfree it up
 */
static char * lwdoubles_to_latlon(double lat, double lon, const char * format)
{
	char * lat_text;
	char * lon_text;
	char * result;

	/* Normalize lat/lon to the normal (-90 to 90, -180 to 180) range. */
	lwprint_normalize_latlon(&lat, &lon);
	/* This is somewhat inefficient as the format is parsed twice. */
	lat_text = lwdouble_to_dms(lat, "N", "S", format);
	lon_text = lwdouble_to_dms(lon, "E", "W", format);

	/* lat + lon + a space between + the null terminator. */
	result = (char*)lwalloc(strlen(lat_text) + strlen(lon_text) + 2);
	sprintf(result, "%s %s", lat_text, lon_text);
	lwfree(lat_text);
	lwfree(lon_text);
	return result;
}

/* Print the X (lon) and Y (lat) of the given point in DMS form using
 * the specified format.
 * First normalizes the values so they will display as -90 to 90 and -180 to 180.
 * Format string may be null or 0-length, in which case a default format will be used.
 * NOTE: Format string is required to be in UTF-8.
 * NOTE2: returned string is lwalloc'ed, caller is responsible to lwfree it up
 */
char* lwpoint_to_latlon(const LWPOINT * pt, const char *format)
{
	const POINT2D *p;
	if (NULL == pt)
	{
		lwerror("Cannot convert a null point into formatted text.");
	}
	if (lwgeom_is_empty((LWGEOM *)pt))
	{
		lwerror("Cannot convert an empty point into formatted text.");
	}
	p = getPoint2d_cp(pt->point, 0);
	return lwdoubles_to_latlon(p->y, p->x, format);
}
