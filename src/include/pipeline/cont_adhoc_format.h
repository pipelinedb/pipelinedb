#ifndef CONT_ADHOC_FORMAT_H
#define CONT_ADHOC_FORMAT_H

#include "lib/stringinfo.h"

/* this module is reponsible for escaping and formatting data for use with 
 * adhoc frontends. It is basically copy text format (escaped tab separated values) */

extern void adhoc_write_string(StringInfo msgbuf, const char *str);
extern void adhoc_write_data(StringInfo msgbuf, const void *databuf, int datasize);

extern void adhoc_write_char(StringInfo msgbuf, char c);
extern void adhoc_write_end_of_row(StringInfo msgbuf);
extern void adhoc_write_msg_type(StringInfo buf, char type);

extern void adhoc_write_attribute_out_text(StringInfo msgbuf,
										   char delim, char *string);

#endif
