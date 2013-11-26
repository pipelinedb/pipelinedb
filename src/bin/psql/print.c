/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/print.c
 */
#include "postgres_fe.h"

#include <limits.h>
#include <math.h>
#include <signal.h>
#include <unistd.h>

#ifndef WIN32
#include <sys/ioctl.h>			/* for ioctl() */
#endif

#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

#include <locale.h>

#include "catalog/pg_type.h"
#include "pqsignal.h"

#include "common.h"
#include "mbprint.h"
#include "print.h"

/*
 * We define the cancel_pressed flag in this file, rather than common.c where
 * it naturally belongs, because this file is also used by non-psql programs
 * (see the bin/scripts/ directory).  In those programs cancel_pressed will
 * never become set and will have no effect.
 *
 * Note: print.c's general strategy for when to check cancel_pressed is to do
 * so at completion of each row of output.
 */
volatile bool cancel_pressed = false;

static char *decimal_point;
static char *grouping;
static char *thousands_sep;

static char default_footer[100];
static printTableFooter default_footer_cell = {default_footer, NULL};

/* Line style control structures */
const printTextFormat pg_asciiformat =
{
	"ascii",
	{
		{"-", "+", "+", "+"},
		{"-", "+", "+", "+"},
		{"-", "+", "+", "+"},
		{"", "|", "|", "|"}
	},
	"|",
	"|",
	"|",
	" ",
	"+",
	" ",
	"+",
	".",
	".",
	true
};

const printTextFormat pg_asciiformat_old =
{
	"old-ascii",
	{
		{"-", "+", "+", "+"},
		{"-", "+", "+", "+"},
		{"-", "+", "+", "+"},
		{"", "|", "|", "|"}
	},
	":",
	";",
	" ",
	"+",
	" ",
	" ",
	" ",
	" ",
	" ",
	false
};

const printTextFormat pg_utf8format =
{
	"unicode",
	{
		/* ─, ┌, ┬, ┐ */
		{"\342\224\200", "\342\224\214", "\342\224\254", "\342\224\220"},
		/* ─, ├, ┼, ┤ */
		{"\342\224\200", "\342\224\234", "\342\224\274", "\342\224\244"},
		/* ─, └, ┴, ┘ */
		{"\342\224\200", "\342\224\224", "\342\224\264", "\342\224\230"},
		/* N/A, │, │, │ */
		{"", "\342\224\202", "\342\224\202", "\342\224\202"}
	},
	/* │ */
	"\342\224\202",
	/* │ */
	"\342\224\202",
	/* │ */
	"\342\224\202",
	" ",
	/* ↵ */
	"\342\206\265",
	" ",
	/* ↵ */
	"\342\206\265",
	/* … */
	"\342\200\246",
	/* … */
	"\342\200\246",
	true
};


/* Local functions */
static int	strlen_max_width(unsigned char *str, int *target_width, int encoding);
static void IsPagerNeeded(const printTableContent *cont, const int extra_lines, bool expanded,
			  FILE **fout, bool *is_pager);

static void print_aligned_vertical(const printTableContent *cont, FILE *fout);


static void *
pg_local_malloc(size_t size)
{
	void	   *tmp;

	tmp = malloc(size);
	if (!tmp)
	{
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
	}
	return tmp;
}

static void *
pg_local_calloc(int count, size_t size)
{
	void	   *tmp;

	tmp = calloc(count, size);
	if (!tmp)
	{
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
	}
	return tmp;
}

static int
integer_digits(const char *my_str)
{
	int			frac_len;

	if (my_str[0] == '-')
		my_str++;

	frac_len = strchr(my_str, '.') ? strlen(strchr(my_str, '.')) : 0;

	return strlen(my_str) - frac_len;
}

/* Return additional length required for locale-aware numeric output */
static int
additional_numeric_locale_len(const char *my_str)
{
	int			int_len = integer_digits(my_str),
				len = 0;
	int			groupdigits = atoi(grouping);

	if (int_len > 0)
		/* Don't count a leading separator */
		len = (int_len / groupdigits - (int_len % groupdigits == 0)) *
			strlen(thousands_sep);

	if (strchr(my_str, '.') != NULL)
		len += strlen(decimal_point) - strlen(".");

	return len;
}

static int
strlen_with_numeric_locale(const char *my_str)
{
	return strlen(my_str) + additional_numeric_locale_len(my_str);
}

/*
 * Returns the appropriately formatted string in a new allocated block,
 * caller must free
 */
static char *
format_numeric_locale(const char *my_str)
{
	int			i,
				j,
				int_len = integer_digits(my_str),
				leading_digits;
	int			groupdigits = atoi(grouping);
	int			new_str_start = 0;
	char	   *new_str = pg_local_malloc(
									 strlen_with_numeric_locale(my_str) + 1);

	leading_digits = (int_len % groupdigits != 0) ?
		int_len % groupdigits : groupdigits;

	if (my_str[0] == '-')		/* skip over sign, affects grouping
								 * calculations */
	{
		new_str[0] = my_str[0];
		my_str++;
		new_str_start = 1;
	}

	for (i = 0, j = new_str_start;; i++, j++)
	{
		/* Hit decimal point? */
		if (my_str[i] == '.')
		{
			strcpy(&new_str[j], decimal_point);
			j += strlen(decimal_point);
			/* add fractional part */
			strcpy(&new_str[j], &my_str[i] + 1);
			break;
		}

		/* End of string? */
		if (my_str[i] == '\0')
		{
			new_str[j] = '\0';
			break;
		}

		/* Add separator? */
		if (i != 0 && (i - leading_digits) % groupdigits == 0)
		{
			strcpy(&new_str[j], thousands_sep);
			j += strlen(thousands_sep);
		}

		new_str[j] = my_str[i];
	}

	return new_str;
}


/*
 * fputnbytes: print exactly N bytes to a file
 *
 * We avoid using %.*s here because it can misbehave if the data
 * is not valid in what libc thinks is the prevailing encoding.
 */
static void
fputnbytes(FILE *f, const char *str, size_t n)
{
	while (n-- > 0)
		fputc(*str++, f);
}


static void
print_separator(struct separator sep, FILE *fout)
{
	if (sep.separator_zero)
		fputc('\000', fout);
	else if (sep.separator)
		fputs(sep.separator, fout);
}


/*
 * Return the list of explicitly-requested footers or, when applicable, the
 * default "(xx rows)" footer.	Always omit the default footer when given
 * non-default footers, "\pset footer off", or a specific instruction to that
 * effect from a calling backslash command.  Vertical formats number each row,
 * making the default footer redundant; they do not call this function.
 *
 * The return value may point to static storage; do not keep it across calls.
 */
static printTableFooter *
footers_with_default(const printTableContent *cont)
{
	if (cont->footers == NULL && cont->opt->default_footer)
	{
		unsigned long total_records;

		total_records = cont->opt->prior_records + cont->nrows;
		snprintf(default_footer, sizeof(default_footer),
				 ngettext("(%lu row)", "(%lu rows)", total_records),
				 total_records);

		return &default_footer_cell;
	}
	else
		return cont->footers;
}


/*************************/
/* Unaligned text		 */
/*************************/


static void
print_unaligned_text(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned int i;
	const char *const * ptr;
	bool		need_recordsep = false;

	if (cancel_pressed)
		return;

	if (cont->opt->start_table)
	{
		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs(cont->title, fout);
			print_separator(cont->opt->recordSep, fout);
		}

		/* print headers */
		if (!opt_tuples_only)
		{
			for (ptr = cont->headers; *ptr; ptr++)
			{
				if (ptr != cont->headers)
					print_separator(cont->opt->fieldSep, fout);
				fputs(*ptr, fout);
			}
			need_recordsep = true;
		}
	}
	else
		/* assume continuing printout */
		need_recordsep = true;

	/* print cells */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		if (need_recordsep)
		{
			print_separator(cont->opt->recordSep, fout);
			need_recordsep = false;
			if (cancel_pressed)
				break;
		}
		fputs(*ptr, fout);

		if ((i + 1) % cont->ncolumns)
			print_separator(cont->opt->fieldSep, fout);
		else
			need_recordsep = true;
	}

	/* print footers */
	if (cont->opt->stop_table)
	{
		printTableFooter *footers = footers_with_default(cont);

		if (!opt_tuples_only && footers != NULL && !cancel_pressed)
		{
			printTableFooter *f;

			for (f = footers; f; f = f->next)
			{
				if (need_recordsep)
				{
					print_separator(cont->opt->recordSep, fout);
					need_recordsep = false;
				}
				fputs(f->data, fout);
				need_recordsep = true;
			}
		}

		/*
		 * The last record is terminated by a newline, independent of the set
		 * record separator.  But when the record separator is a zero byte, we
		 * use that (compatible with find -print0 and xargs).
		 */
		if (need_recordsep)
		{
			if (cont->opt->recordSep.separator_zero)
				print_separator(cont->opt->recordSep, fout);
			else
				fputc('\n', fout);
		}
	}
}


static void
print_unaligned_vertical(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned int i;
	const char *const * ptr;
	bool		need_recordsep = false;

	if (cancel_pressed)
		return;

	if (cont->opt->start_table)
	{
		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs(cont->title, fout);
			need_recordsep = true;
		}
	}
	else
		/* assume continuing printout */
		need_recordsep = true;

	/* print records */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		if (need_recordsep)
		{
			/* record separator is 2 occurrences of recordsep in this mode */
			print_separator(cont->opt->recordSep, fout);
			print_separator(cont->opt->recordSep, fout);
			need_recordsep = false;
			if (cancel_pressed)
				break;
		}

		fputs(cont->headers[i % cont->ncolumns], fout);
		print_separator(cont->opt->fieldSep, fout);
		fputs(*ptr, fout);

		if ((i + 1) % cont->ncolumns)
			print_separator(cont->opt->recordSep, fout);
		else
			need_recordsep = true;
	}

	if (cont->opt->stop_table)
	{
		/* print footers */
		if (!opt_tuples_only && cont->footers != NULL && !cancel_pressed)
		{
			printTableFooter *f;

			print_separator(cont->opt->recordSep, fout);
			for (f = cont->footers; f; f = f->next)
			{
				print_separator(cont->opt->recordSep, fout);
				fputs(f->data, fout);
			}
		}

		/* see above in print_unaligned_text() */
		if (cont->opt->recordSep.separator_zero)
			print_separator(cont->opt->recordSep, fout);
		else
			fputc('\n', fout);
	}
}


/********************/
/* Aligned text		*/
/********************/


/* draw "line" */
static void
_print_horizontal_line(const unsigned int ncolumns, const unsigned int *widths,
					   unsigned short border, printTextRule pos,
					   const printTextFormat *format,
					   FILE *fout)
{
	const printTextLineFormat *lformat = &format->lrule[pos];
	unsigned int i,
				j;

	if (border == 1)
		fputs(lformat->hrule, fout);
	else if (border == 2)
		fprintf(fout, "%s%s", lformat->leftvrule, lformat->hrule);

	for (i = 0; i < ncolumns; i++)
	{
		for (j = 0; j < widths[i]; j++)
			fputs(lformat->hrule, fout);

		if (i < ncolumns - 1)
		{
			if (border == 0)
				fputc(' ', fout);
			else
				fprintf(fout, "%s%s%s", lformat->hrule,
						lformat->midvrule, lformat->hrule);
		}
	}

	if (border == 2)
		fprintf(fout, "%s%s", lformat->hrule, lformat->rightvrule);
	else if (border == 1)
		fputs(lformat->hrule, fout);

	fputc('\n', fout);
}


/*
 *	Print pretty boxes around cells.
 */
static void
print_aligned_text(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	int			encoding = cont->opt->encoding;
	unsigned short opt_border = cont->opt->border;
	const printTextFormat *format = get_line_style(cont->opt);
	const printTextLineFormat *dformat = &format->lrule[PRINT_RULE_DATA];

	unsigned int col_count = 0,
				cell_count = 0;

	unsigned int i,
				j;

	unsigned int *width_header,
			   *max_width,
			   *width_wrap,
			   *width_average;
	unsigned int *max_nl_lines, /* value split by newlines */
			   *curr_nl_line,
			   *max_bytes;
	unsigned char **format_buf;
	unsigned int width_total;
	unsigned int total_header_width;
	unsigned int extra_row_output_lines = 0;
	unsigned int extra_output_lines = 0;

	const char *const * ptr;

	struct lineptr **col_lineptrs;		/* pointers to line pointer per column */

	bool	   *header_done;	/* Have all header lines been output? */
	int		   *bytes_output;	/* Bytes output for column value */
	printTextLineWrap *wrap;	/* Wrap status for each column */
	int			output_columns = 0;		/* Width of interactive console */
	bool		is_pager = false;

	if (cancel_pressed)
		return;

	if (opt_border > 2)
		opt_border = 2;

	if (cont->ncolumns > 0)
	{
		col_count = cont->ncolumns;
		width_header = pg_local_calloc(col_count, sizeof(*width_header));
		width_average = pg_local_calloc(col_count, sizeof(*width_average));
		max_width = pg_local_calloc(col_count, sizeof(*max_width));
		width_wrap = pg_local_calloc(col_count, sizeof(*width_wrap));
		max_nl_lines = pg_local_calloc(col_count, sizeof(*max_nl_lines));
		curr_nl_line = pg_local_calloc(col_count, sizeof(*curr_nl_line));
		col_lineptrs = pg_local_calloc(col_count, sizeof(*col_lineptrs));
		max_bytes = pg_local_calloc(col_count, sizeof(*max_bytes));
		format_buf = pg_local_calloc(col_count, sizeof(*format_buf));
		header_done = pg_local_calloc(col_count, sizeof(*header_done));
		bytes_output = pg_local_calloc(col_count, sizeof(*bytes_output));
		wrap = pg_local_calloc(col_count, sizeof(*wrap));
	}
	else
	{
		width_header = NULL;
		width_average = NULL;
		max_width = NULL;
		width_wrap = NULL;
		max_nl_lines = NULL;
		curr_nl_line = NULL;
		col_lineptrs = NULL;
		max_bytes = NULL;
		format_buf = NULL;
		header_done = NULL;
		bytes_output = NULL;
		wrap = NULL;
	}

	/* scan all column headers, find maximum width and max max_nl_lines */
	for (i = 0; i < col_count; i++)
	{
		int			width,
					nl_lines,
					bytes_required;

		pg_wcssize((const unsigned char *) cont->headers[i], strlen(cont->headers[i]),
				   encoding, &width, &nl_lines, &bytes_required);
		if (width > max_width[i])
			max_width[i] = width;
		if (nl_lines > max_nl_lines[i])
			max_nl_lines[i] = nl_lines;
		if (bytes_required > max_bytes[i])
			max_bytes[i] = bytes_required;
		if (nl_lines > extra_row_output_lines)
			extra_row_output_lines = nl_lines;

		width_header[i] = width;
	}
	/* Add height of tallest header column */
	extra_output_lines += extra_row_output_lines;
	extra_row_output_lines = 0;

	/* scan all cells, find maximum width, compute cell_count */
	for (i = 0, ptr = cont->cells; *ptr; ptr++, i++, cell_count++)
	{
		int			width,
					nl_lines,
					bytes_required;

		pg_wcssize((const unsigned char *) *ptr, strlen(*ptr), encoding,
				   &width, &nl_lines, &bytes_required);

		if (width > max_width[i % col_count])
			max_width[i % col_count] = width;
		if (nl_lines > max_nl_lines[i % col_count])
			max_nl_lines[i % col_count] = nl_lines;
		if (bytes_required > max_bytes[i % col_count])
			max_bytes[i % col_count] = bytes_required;

		width_average[i % col_count] += width;
	}

	/* If we have rows, compute average */
	if (col_count != 0 && cell_count != 0)
	{
		int			rows = cell_count / col_count;

		for (i = 0; i < col_count; i++)
			width_average[i] /= rows;
	}

	/* adjust the total display width based on border style */
	if (opt_border == 0)
		width_total = col_count;
	else if (opt_border == 1)
		width_total = col_count * 3 - 1;
	else
		width_total = col_count * 3 + 1;
	total_header_width = width_total;

	for (i = 0; i < col_count; i++)
	{
		width_total += max_width[i];
		total_header_width += width_header[i];
	}

	/*
	 * At this point: max_width[] contains the max width of each column,
	 * max_nl_lines[] contains the max number of lines in each column,
	 * max_bytes[] contains the maximum storage space for formatting strings,
	 * width_total contains the giant width sum.  Now we allocate some memory
	 * for line pointers.
	 */
	for (i = 0; i < col_count; i++)
	{
		/* Add entry for ptr == NULL array termination */
		col_lineptrs[i] = pg_local_calloc(max_nl_lines[i] + 1,
										  sizeof(**col_lineptrs));

		format_buf[i] = pg_local_malloc(max_bytes[i] + 1);

		col_lineptrs[i]->ptr = format_buf[i];
	}

	/* Default word wrap to the full width, i.e. no word wrap */
	for (i = 0; i < col_count; i++)
		width_wrap[i] = max_width[i];

	/*
	 * Choose target output width: \pset columns, or $COLUMNS, or ioctl
	 */
	if (cont->opt->columns > 0)
		output_columns = cont->opt->columns;
	else if ((fout == stdout && isatty(fileno(stdout))) || is_pager)
	{
		if (cont->opt->env_columns > 0)
			output_columns = cont->opt->env_columns;
#ifdef TIOCGWINSZ
		else
		{
			struct winsize screen_size;

			if (ioctl(fileno(stdout), TIOCGWINSZ, &screen_size) != -1)
				output_columns = screen_size.ws_col;
		}
#endif
	}

	if (cont->opt->format == PRINT_WRAPPED)
	{
		/*
		 * Optional optimized word wrap. Shrink columns with a high max/avg
		 * ratio.  Slighly bias against wider columns. (Increases chance a
		 * narrow column will fit in its cell.)  If available columns is
		 * positive...	and greater than the width of the unshrinkable column
		 * headers
		 */
		if (output_columns > 0 && output_columns >= total_header_width)
		{
			/* While there is still excess width... */
			while (width_total > output_columns)
			{
				double		max_ratio = 0;
				int			worst_col = -1;

				/*
				 * Find column that has the highest ratio of its maximum width
				 * compared to its average width.  This tells us which column
				 * will produce the fewest wrapped values if shortened.
				 * width_wrap starts as equal to max_width.
				 */
				for (i = 0; i < col_count; i++)
				{
					if (width_average[i] && width_wrap[i] > width_header[i])
					{
						/* Penalize wide columns by 1% of their width */
						double		ratio;

						ratio = (double) width_wrap[i] / width_average[i] +
							max_width[i] * 0.01;
						if (ratio > max_ratio)
						{
							max_ratio = ratio;
							worst_col = i;
						}
					}
				}

				/* Exit loop if we can't squeeze any more. */
				if (worst_col == -1)
					break;

				/* Decrease width of target column by one. */
				width_wrap[worst_col]--;
				width_total--;
			}
		}
	}

	/*
	 * If in expanded auto mode, we have now calculated the expected width, so
	 * we can now escape to vertical mode if necessary.
	 */
	if (cont->opt->expanded == 2 && output_columns > 0 &&
		(output_columns < total_header_width || output_columns < width_total))
	{
		print_aligned_vertical(cont, fout);
		goto cleanup;
	}

	/* If we wrapped beyond the display width, use the pager */
	if (!is_pager && fout == stdout && output_columns > 0 &&
		(output_columns < total_header_width || output_columns < width_total))
	{
		fout = PageOutput(INT_MAX, cont->opt->pager);	/* force pager */
		is_pager = true;
	}

	/* Check if newlines or our wrapping now need the pager */
	if (!is_pager)
	{
		/* scan all cells, find maximum width, compute cell_count */
		for (i = 0, ptr = cont->cells; *ptr; ptr++, cell_count++)
		{
			int			width,
						nl_lines,
						bytes_required;

			pg_wcssize((const unsigned char *) *ptr, strlen(*ptr), encoding,
					   &width, &nl_lines, &bytes_required);

			/*
			 * A row can have both wrapping and newlines that cause it to
			 * display across multiple lines.  We check for both cases below.
			 */
			if (width > 0 && width_wrap[i])
			{
				unsigned int extra_lines;

				extra_lines = (width - 1) / width_wrap[i] + nl_lines;
				if (extra_lines > extra_row_output_lines)
					extra_row_output_lines = extra_lines;
			}

			/* i is the current column number: increment with wrap */
			if (++i >= col_count)
			{
				i = 0;
				/* At last column of each row, add tallest column height */
				extra_output_lines += extra_row_output_lines;
				extra_row_output_lines = 0;
			}
		}
		IsPagerNeeded(cont, extra_output_lines, false, &fout, &is_pager);
	}

	/* time to output */
	if (cont->opt->start_table)
	{
		/* print title */
		if (cont->title && !opt_tuples_only)
		{
			int			width,
						height;

			pg_wcssize((const unsigned char *) cont->title, strlen(cont->title),
					   encoding, &width, &height, NULL);
			if (width >= width_total)
				/* Aligned */
				fprintf(fout, "%s\n", cont->title);
			else
				/* Centered */
				fprintf(fout, "%-*s%s\n", (width_total - width) / 2, "",
						cont->title);
		}

		/* print headers */
		if (!opt_tuples_only)
		{
			int			more_col_wrapping;
			int			curr_nl_line;

			if (opt_border == 2)
				_print_horizontal_line(col_count, width_wrap, opt_border,
									   PRINT_RULE_TOP, format, fout);

			for (i = 0; i < col_count; i++)
				pg_wcsformat((const unsigned char *) cont->headers[i],
							 strlen(cont->headers[i]), encoding,
							 col_lineptrs[i], max_nl_lines[i]);

			more_col_wrapping = col_count;
			curr_nl_line = 0;
			memset(header_done, false, col_count * sizeof(bool));
			while (more_col_wrapping)
			{
				if (opt_border == 2)
					fputs(dformat->leftvrule, fout);

				for (i = 0; i < cont->ncolumns; i++)
				{
					struct lineptr *this_line = col_lineptrs[i] + curr_nl_line;
					unsigned int nbspace;

					if (opt_border != 0 ||
						(!format->wrap_right_border && i > 0))
						fputs(curr_nl_line ? format->header_nl_left : " ",
							  fout);

					if (!header_done[i])
					{
						nbspace = width_wrap[i] - this_line->width;

						/* centered */
						fprintf(fout, "%-*s%s%-*s",
								nbspace / 2, "", this_line->ptr, (nbspace + 1) / 2, "");

						if (!(this_line + 1)->ptr)
						{
							more_col_wrapping--;
							header_done[i] = 1;
						}
					}
					else
						fprintf(fout, "%*s", width_wrap[i], "");

					if (opt_border != 0 || format->wrap_right_border)
						fputs(!header_done[i] ? format->header_nl_right : " ",
							  fout);

					if (opt_border != 0 && i < col_count - 1)
						fputs(dformat->midvrule, fout);
				}
				curr_nl_line++;

				if (opt_border == 2)
					fputs(dformat->rightvrule, fout);
				fputc('\n', fout);
			}

			_print_horizontal_line(col_count, width_wrap, opt_border,
								   PRINT_RULE_MIDDLE, format, fout);
		}
	}

	/* print cells, one loop per row */
	for (i = 0, ptr = cont->cells; *ptr; i += col_count, ptr += col_count)
	{
		bool		more_lines;

		if (cancel_pressed)
			break;

		/*
		 * Format each cell.
		 */
		for (j = 0; j < col_count; j++)
		{
			pg_wcsformat((const unsigned char *) ptr[j], strlen(ptr[j]), encoding,
						 col_lineptrs[j], max_nl_lines[j]);
			curr_nl_line[j] = 0;
		}

		memset(bytes_output, 0, col_count * sizeof(int));

		/*
		 * Each time through this loop, one display line is output. It can
		 * either be a full value or a partial value if embedded newlines
		 * exist or if 'format=wrapping' mode is enabled.
		 */
		do
		{
			more_lines = false;

			/* left border */
			if (opt_border == 2)
				fputs(dformat->leftvrule, fout);

			/* for each column */
			for (j = 0; j < col_count; j++)
			{
				/* We have a valid array element, so index it */
				struct lineptr *this_line = &col_lineptrs[j][curr_nl_line[j]];
				int			bytes_to_output;
				int			chars_to_output = width_wrap[j];
				bool		finalspaces = (opt_border == 2 || j < col_count - 1);

				/* Print left-hand wrap or newline mark */
				if (opt_border != 0)
				{
					if (wrap[j] == PRINT_LINE_WRAP_WRAP)
						fputs(format->wrap_left, fout);
					else if (wrap[j] == PRINT_LINE_WRAP_NEWLINE)
						fputs(format->nl_left, fout);
					else
						fputc(' ', fout);
				}

				if (!this_line->ptr)
				{
					/* Past newline lines so just pad for other columns */
					if (finalspaces)
						fprintf(fout, "%*s", chars_to_output, "");
				}
				else
				{
					/* Get strlen() of the characters up to width_wrap */
					bytes_to_output =
						strlen_max_width(this_line->ptr + bytes_output[j],
										 &chars_to_output, encoding);

					/*
					 * If we exceeded width_wrap, it means the display width
					 * of a single character was wider than our target width.
					 * In that case, we have to pretend we are only printing
					 * the target display width and make the best of it.
					 */
					if (chars_to_output > width_wrap[j])
						chars_to_output = width_wrap[j];

					if (cont->aligns[j] == 'r') /* Right aligned cell */
					{
						/* spaces first */
						fprintf(fout, "%*s", width_wrap[j] - chars_to_output, "");
						fputnbytes(fout,
								 (char *) (this_line->ptr + bytes_output[j]),
								   bytes_to_output);
					}
					else	/* Left aligned cell */
					{
						/* spaces second */
						fputnbytes(fout,
								 (char *) (this_line->ptr + bytes_output[j]),
								   bytes_to_output);
					}

					bytes_output[j] += bytes_to_output;

					/* Do we have more text to wrap? */
					if (*(this_line->ptr + bytes_output[j]) != '\0')
						more_lines = true;
					else
					{
						/* Advance to next newline line */
						curr_nl_line[j]++;
						if (col_lineptrs[j][curr_nl_line[j]].ptr != NULL)
							more_lines = true;
						bytes_output[j] = 0;
					}
				}

				/* Determine next line's wrap status for this column */
				wrap[j] = PRINT_LINE_WRAP_NONE;
				if (col_lineptrs[j][curr_nl_line[j]].ptr != NULL)
				{
					if (bytes_output[j] != 0)
						wrap[j] = PRINT_LINE_WRAP_WRAP;
					else if (curr_nl_line[j] != 0)
						wrap[j] = PRINT_LINE_WRAP_NEWLINE;
				}

				/*
				 * If left-aligned, pad out remaining space if needed (not
				 * last column, and/or wrap marks required).
				 */
				if (cont->aligns[j] != 'r')		/* Left aligned cell */
				{
					if (finalspaces ||
						wrap[j] == PRINT_LINE_WRAP_WRAP ||
						wrap[j] == PRINT_LINE_WRAP_NEWLINE)
						fprintf(fout, "%*s",
								width_wrap[j] - chars_to_output, "");
				}

				/* Print right-hand wrap or newline mark */
				if (wrap[j] == PRINT_LINE_WRAP_WRAP)
					fputs(format->wrap_right, fout);
				else if (wrap[j] == PRINT_LINE_WRAP_NEWLINE)
					fputs(format->nl_right, fout);
				else if (opt_border == 2 || j < col_count - 1)
					fputc(' ', fout);

				/* Print column divider, if not the last column */
				if (opt_border != 0 && j < col_count - 1)
				{
					if (wrap[j + 1] == PRINT_LINE_WRAP_WRAP)
						fputs(format->midvrule_wrap, fout);
					else if (wrap[j + 1] == PRINT_LINE_WRAP_NEWLINE)
						fputs(format->midvrule_nl, fout);
					else if (col_lineptrs[j + 1][curr_nl_line[j + 1]].ptr == NULL)
						fputs(format->midvrule_blank, fout);
					else
						fputs(dformat->midvrule, fout);
				}
			}

			/* end-of-row border */
			if (opt_border == 2)
				fputs(dformat->rightvrule, fout);
			fputc('\n', fout);

		} while (more_lines);
	}

	if (cont->opt->stop_table)
	{
		printTableFooter *footers = footers_with_default(cont);

		if (opt_border == 2 && !cancel_pressed)
			_print_horizontal_line(col_count, width_wrap, opt_border,
								   PRINT_RULE_BOTTOM, format, fout);

		/* print footers */
		if (footers && !opt_tuples_only && !cancel_pressed)
		{
			printTableFooter *f;

			for (f = footers; f; f = f->next)
				fprintf(fout, "%s\n", f->data);
		}

		fputc('\n', fout);
	}

cleanup:
	/* clean up */
	for (i = 0; i < col_count; i++)
	{
		free(col_lineptrs[i]);
		free(format_buf[i]);
	}
	free(width_header);
	free(width_average);
	free(max_width);
	free(width_wrap);
	free(max_nl_lines);
	free(curr_nl_line);
	free(col_lineptrs);
	free(max_bytes);
	free(format_buf);
	free(header_done);
	free(bytes_output);
	free(wrap);

	if (is_pager)
		ClosePager(fout);
}


static void
print_aligned_vertical_line(const printTableContent *cont,
							unsigned long record,
							unsigned int hwidth,
							unsigned int dwidth,
							printTextRule pos,
							FILE *fout)
{
	const printTextFormat *format = get_line_style(cont->opt);
	const printTextLineFormat *lformat = &format->lrule[pos];
	unsigned short opt_border = cont->opt->border;
	unsigned int i;
	int			reclen = 0;

	if (opt_border == 2)
		fprintf(fout, "%s%s", lformat->leftvrule, lformat->hrule);
	else if (opt_border == 1)
		fputs(lformat->hrule, fout);

	if (record)
	{
		if (opt_border == 0)
			reclen = fprintf(fout, "* Record %lu", record);
		else
			reclen = fprintf(fout, "[ RECORD %lu ]", record);
	}
	if (opt_border != 2)
		reclen++;
	if (reclen < 0)
		reclen = 0;
	for (i = reclen; i < hwidth; i++)
		fputs(opt_border > 0 ? lformat->hrule : " ", fout);
	reclen -= hwidth;

	if (opt_border > 0)
	{
		if (reclen-- <= 0)
			fputs(lformat->hrule, fout);
		if (reclen-- <= 0)
			fputs(lformat->midvrule, fout);
		if (reclen-- <= 0)
			fputs(lformat->hrule, fout);
	}
	else
	{
		if (reclen-- <= 0)
			fputc(' ', fout);
	}
	if (reclen < 0)
		reclen = 0;
	for (i = reclen; i < dwidth; i++)
		fputs(opt_border > 0 ? lformat->hrule : " ", fout);
	if (opt_border == 2)
		fprintf(fout, "%s%s", lformat->hrule, lformat->rightvrule);
	fputc('\n', fout);
}

static void
print_aligned_vertical(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned short opt_border = cont->opt->border;
	const printTextFormat *format = get_line_style(cont->opt);
	const printTextLineFormat *dformat = &format->lrule[PRINT_RULE_DATA];
	int			encoding = cont->opt->encoding;
	unsigned long record = cont->opt->prior_records + 1;
	const char *const * ptr;
	unsigned int i,
				hwidth = 0,
				dwidth = 0,
				hheight = 1,
				dheight = 1,
				hformatsize = 0,
				dformatsize = 0;
	struct lineptr *hlineptr,
			   *dlineptr;
	bool		is_pager = false;

	if (cancel_pressed)
		return;

	if (opt_border > 2)
		opt_border = 2;

	if (cont->cells[0] == NULL && cont->opt->start_table &&
		cont->opt->stop_table)
	{
		fprintf(fout, _("(No rows)\n"));
		return;
	}

	/*
	 * Deal with the pager here instead of in printTable(), because we could
	 * get here via print_aligned_text() in expanded auto mode, and so we have
	 * to recalcuate the pager requirement based on vertical output.
	 */
	IsPagerNeeded(cont, 0, true, &fout, &is_pager);

	/* Find the maximum dimensions for the headers */
	for (i = 0; i < cont->ncolumns; i++)
	{
		int			width,
					height,
					fs;

		pg_wcssize((const unsigned char *) cont->headers[i], strlen(cont->headers[i]),
				   encoding, &width, &height, &fs);
		if (width > hwidth)
			hwidth = width;
		if (height > hheight)
			hheight = height;
		if (fs > hformatsize)
			hformatsize = fs;
	}

	/* find longest data cell */
	for (i = 0, ptr = cont->cells; *ptr; ptr++, i++)
	{
		int			width,
					height,
					fs;

		pg_wcssize((const unsigned char *) *ptr, strlen(*ptr), encoding,
				   &width, &height, &fs);
		if (width > dwidth)
			dwidth = width;
		if (height > dheight)
			dheight = height;
		if (fs > dformatsize)
			dformatsize = fs;
	}

	/*
	 * We now have all the information we need to setup the formatting
	 * structures
	 */
	dlineptr = pg_local_malloc((sizeof(*dlineptr)) * (dheight + 1));
	hlineptr = pg_local_malloc((sizeof(*hlineptr)) * (hheight + 1));

	dlineptr->ptr = pg_local_malloc(dformatsize);
	hlineptr->ptr = pg_local_malloc(hformatsize);

	if (cont->opt->start_table)
	{
		/* print title */
		if (!opt_tuples_only && cont->title)
			fprintf(fout, "%s\n", cont->title);
	}

	/* print records */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		printTextRule pos;
		int			line_count,
					dcomplete,
					hcomplete;

		if (cancel_pressed)
			break;

		if (i == 0)
			pos = PRINT_RULE_TOP;
		else if (!(*(ptr + 1)))
			pos = PRINT_RULE_BOTTOM;
		else
			pos = PRINT_RULE_MIDDLE;

		if (i % cont->ncolumns == 0)
		{
			if (!opt_tuples_only)
				print_aligned_vertical_line(cont, record++, hwidth, dwidth,
											pos, fout);
			else if (i != 0 || !cont->opt->start_table || opt_border == 2)
				print_aligned_vertical_line(cont, 0, hwidth, dwidth,
											pos, fout);
		}

		/* Format the header */
		pg_wcsformat((const unsigned char *) cont->headers[i % cont->ncolumns],
					 strlen(cont->headers[i % cont->ncolumns]),
					 encoding, hlineptr, hheight);
		/* Format the data */
		pg_wcsformat((const unsigned char *) *ptr, strlen(*ptr), encoding,
					 dlineptr, dheight);

		line_count = 0;
		dcomplete = hcomplete = 0;
		while (!dcomplete || !hcomplete)
		{
			if (opt_border == 2)
				fprintf(fout, "%s ", dformat->leftvrule);
			if (!hcomplete)
			{
				fprintf(fout, "%-s%*s", hlineptr[line_count].ptr,
						hwidth - hlineptr[line_count].width, "");

				if (!hlineptr[line_count + 1].ptr)
					hcomplete = 1;
			}
			else
				fprintf(fout, "%*s", hwidth, "");

			if (opt_border > 0)
				fprintf(fout, " %s ", dformat->midvrule);
			else
				fputc(' ', fout);

			if (!dcomplete)
			{
				if (opt_border < 2)
					fprintf(fout, "%s\n", dlineptr[line_count].ptr);
				else
					fprintf(fout, "%-s%*s %s\n", dlineptr[line_count].ptr,
							dwidth - dlineptr[line_count].width, "",
							dformat->rightvrule);

				if (!dlineptr[line_count + 1].ptr)
					dcomplete = 1;
			}
			else
			{
				if (opt_border < 2)
					fputc('\n', fout);
				else
					fprintf(fout, "%*s %s\n", dwidth, "", dformat->rightvrule);
			}
			line_count++;
		}
	}

	if (cont->opt->stop_table)
	{
		if (opt_border == 2 && !cancel_pressed)
			print_aligned_vertical_line(cont, 0, hwidth, dwidth,
										PRINT_RULE_BOTTOM, fout);

		/* print footers */
		if (!opt_tuples_only && cont->footers != NULL && !cancel_pressed)
		{
			printTableFooter *f;

			if (opt_border < 2)
				fputc('\n', fout);
			for (f = cont->footers; f; f = f->next)
				fprintf(fout, "%s\n", f->data);
		}

		fputc('\n', fout);
	}

	free(hlineptr->ptr);
	free(dlineptr->ptr);
	free(hlineptr);
	free(dlineptr);

	if (is_pager)
		ClosePager(fout);
}


/**********************/
/* HTML printing ******/
/**********************/


void
html_escaped_print(const char *in, FILE *fout)
{
	const char *p;
	bool		leading_space = true;

	for (p = in; *p; p++)
	{
		switch (*p)
		{
			case '&':
				fputs("&amp;", fout);
				break;
			case '<':
				fputs("&lt;", fout);
				break;
			case '>':
				fputs("&gt;", fout);
				break;
			case '\n':
				fputs("<br />\n", fout);
				break;
			case '"':
				fputs("&quot;", fout);
				break;
			case ' ':
				/* protect leading space, for EXPLAIN output */
				if (leading_space)
					fputs("&nbsp;", fout);
				else
					fputs(" ", fout);
				break;
			default:
				fputc(*p, fout);
		}
		if (*p != ' ')
			leading_space = false;
	}
}


static void
print_html_text(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned short opt_border = cont->opt->border;
	const char *opt_table_attr = cont->opt->tableAttr;
	unsigned int i;
	const char *const * ptr;

	if (cancel_pressed)
		return;

	if (cont->opt->start_table)
	{
		fprintf(fout, "<table border=\"%d\"", opt_border);
		if (opt_table_attr)
			fprintf(fout, " %s", opt_table_attr);
		fputs(">\n", fout);

		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs("  <caption>", fout);
			html_escaped_print(cont->title, fout);
			fputs("</caption>\n", fout);
		}

		/* print headers */
		if (!opt_tuples_only)
		{
			fputs("  <tr>\n", fout);
			for (ptr = cont->headers; *ptr; ptr++)
			{
				fputs("    <th align=\"center\">", fout);
				html_escaped_print(*ptr, fout);
				fputs("</th>\n", fout);
			}
			fputs("  </tr>\n", fout);
		}
	}

	/* print cells */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		if (i % cont->ncolumns == 0)
		{
			if (cancel_pressed)
				break;
			fputs("  <tr valign=\"top\">\n", fout);
		}

		fprintf(fout, "    <td align=\"%s\">", cont->aligns[(i) % cont->ncolumns] == 'r' ? "right" : "left");
		/* is string only whitespace? */
		if ((*ptr)[strspn(*ptr, " \t")] == '\0')
			fputs("&nbsp; ", fout);
		else
			html_escaped_print(*ptr, fout);

		fputs("</td>\n", fout);

		if ((i + 1) % cont->ncolumns == 0)
			fputs("  </tr>\n", fout);
	}

	if (cont->opt->stop_table)
	{
		printTableFooter *footers = footers_with_default(cont);

		fputs("</table>\n", fout);

		/* print footers */
		if (!opt_tuples_only && footers != NULL && !cancel_pressed)
		{
			printTableFooter *f;

			fputs("<p>", fout);
			for (f = footers; f; f = f->next)
			{
				html_escaped_print(f->data, fout);
				fputs("<br />\n", fout);
			}
			fputs("</p>", fout);
		}

		fputc('\n', fout);
	}
}


static void
print_html_vertical(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned short opt_border = cont->opt->border;
	const char *opt_table_attr = cont->opt->tableAttr;
	unsigned long record = cont->opt->prior_records + 1;
	unsigned int i;
	const char *const * ptr;

	if (cancel_pressed)
		return;

	if (cont->opt->start_table)
	{
		fprintf(fout, "<table border=\"%d\"", opt_border);
		if (opt_table_attr)
			fprintf(fout, " %s", opt_table_attr);
		fputs(">\n", fout);

		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs("  <caption>", fout);
			html_escaped_print(cont->title, fout);
			fputs("</caption>\n", fout);
		}
	}

	/* print records */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		if (i % cont->ncolumns == 0)
		{
			if (cancel_pressed)
				break;
			if (!opt_tuples_only)
				fprintf(fout,
						"\n  <tr><td colspan=\"2\" align=\"center\">Record %lu</td></tr>\n",
						record++);
			else
				fputs("\n  <tr><td colspan=\"2\">&nbsp;</td></tr>\n", fout);
		}
		fputs("  <tr valign=\"top\">\n"
			  "    <th>", fout);
		html_escaped_print(cont->headers[i % cont->ncolumns], fout);
		fputs("</th>\n", fout);

		fprintf(fout, "    <td align=\"%s\">", cont->aligns[i % cont->ncolumns] == 'r' ? "right" : "left");
		/* is string only whitespace? */
		if ((*ptr)[strspn(*ptr, " \t")] == '\0')
			fputs("&nbsp; ", fout);
		else
			html_escaped_print(*ptr, fout);

		fputs("</td>\n  </tr>\n", fout);
	}

	if (cont->opt->stop_table)
	{
		fputs("</table>\n", fout);

		/* print footers */
		if (!opt_tuples_only && cont->footers != NULL && !cancel_pressed)
		{
			printTableFooter *f;

			fputs("<p>", fout);
			for (f = cont->footers; f; f = f->next)
			{
				html_escaped_print(f->data, fout);
				fputs("<br />\n", fout);
			}
			fputs("</p>", fout);
		}

		fputc('\n', fout);
	}
}


/*************************/
/* LaTeX				 */
/*************************/


static void
latex_escaped_print(const char *in, FILE *fout)
{
	const char *p;

	for (p = in; *p; p++)
		switch (*p)
		{
			case '&':
				fputs("\\&", fout);
				break;
			case '%':
				fputs("\\%", fout);
				break;
			case '$':
				fputs("\\$", fout);
				break;
			case '_':
				fputs("\\_", fout);
				break;
			case '{':
				fputs("\\{", fout);
				break;
			case '}':
				fputs("\\}", fout);
				break;
			case '\\':
				fputs("\\backslash", fout);
				break;
			case '\n':
				fputs("\\\\", fout);
				break;
			default:
				fputc(*p, fout);
		}
}


static void
print_latex_text(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned short opt_border = cont->opt->border;
	unsigned int i;
	const char *const * ptr;

	if (cancel_pressed)
		return;

	if (opt_border > 2)
		opt_border = 2;

	if (cont->opt->start_table)
	{
		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs("\\begin{center}\n", fout);
			latex_escaped_print(cont->title, fout);
			fputs("\n\\end{center}\n\n", fout);
		}

		/* begin environment and set alignments and borders */
		fputs("\\begin{tabular}{", fout);

		if (opt_border == 2)
			fputs("| ", fout);
		for (i = 0; i < cont->ncolumns; i++)
		{
			fputc(*(cont->aligns + i), fout);
			if (opt_border != 0 && i < cont->ncolumns - 1)
				fputs(" | ", fout);
		}
		if (opt_border == 2)
			fputs(" |", fout);

		fputs("}\n", fout);

		if (!opt_tuples_only && opt_border == 2)
			fputs("\\hline\n", fout);

		/* print headers */
		if (!opt_tuples_only)
		{
			for (i = 0, ptr = cont->headers; i < cont->ncolumns; i++, ptr++)
			{
				if (i != 0)
					fputs(" & ", fout);
				fputs("\\textit{", fout);
				latex_escaped_print(*ptr, fout);
				fputc('}', fout);
			}
			fputs(" \\\\\n", fout);
			fputs("\\hline\n", fout);
		}
	}

	/* print cells */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		latex_escaped_print(*ptr, fout);

		if ((i + 1) % cont->ncolumns == 0)
		{
			fputs(" \\\\\n", fout);
			if (cancel_pressed)
				break;
		}
		else
			fputs(" & ", fout);
	}

	if (cont->opt->stop_table)
	{
		printTableFooter *footers = footers_with_default(cont);

		if (opt_border == 2)
			fputs("\\hline\n", fout);

		fputs("\\end{tabular}\n\n\\noindent ", fout);

		/* print footers */
		if (footers && !opt_tuples_only && !cancel_pressed)
		{
			printTableFooter *f;

			for (f = footers; f; f = f->next)
			{
				latex_escaped_print(f->data, fout);
				fputs(" \\\\\n", fout);
			}
		}

		fputc('\n', fout);
	}
}


static void
print_latex_vertical(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned short opt_border = cont->opt->border;
	unsigned long record = cont->opt->prior_records + 1;
	unsigned int i;
	const char *const * ptr;

	if (cancel_pressed)
		return;

	if (opt_border > 2)
		opt_border = 2;

	if (cont->opt->start_table)
	{
		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs("\\begin{center}\n", fout);
			latex_escaped_print(cont->title, fout);
			fputs("\n\\end{center}\n\n", fout);
		}

		/* begin environment and set alignments and borders */
		fputs("\\begin{tabular}{", fout);
		if (opt_border == 0)
			fputs("cl", fout);
		else if (opt_border == 1)
			fputs("c|l", fout);
		else if (opt_border == 2)
			fputs("|c|l|", fout);
		fputs("}\n", fout);
	}

	/* print records */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		/* new record */
		if (i % cont->ncolumns == 0)
		{
			if (cancel_pressed)
				break;
			if (!opt_tuples_only)
			{
				if (opt_border == 2)
				{
					fputs("\\hline\n", fout);
					fprintf(fout, "\\multicolumn{2}{|c|}{\\textit{Record %lu}} \\\\\n", record++);
				}
				else
					fprintf(fout, "\\multicolumn{2}{c}{\\textit{Record %lu}} \\\\\n", record++);
			}
			if (opt_border >= 1)
				fputs("\\hline\n", fout);
		}

		latex_escaped_print(cont->headers[i % cont->ncolumns], fout);
		fputs(" & ", fout);
		latex_escaped_print(*ptr, fout);
		fputs(" \\\\\n", fout);
	}

	if (cont->opt->stop_table)
	{
		if (opt_border == 2)
			fputs("\\hline\n", fout);

		fputs("\\end{tabular}\n\n\\noindent ", fout);

		/* print footers */
		if (cont->footers && !opt_tuples_only && !cancel_pressed)
		{
			printTableFooter *f;

			for (f = cont->footers; f; f = f->next)
			{
				latex_escaped_print(f->data, fout);
				fputs(" \\\\\n", fout);
			}
		}

		fputc('\n', fout);
	}
}


/*************************/
/* Troff -ms		 */
/*************************/


static void
troff_ms_escaped_print(const char *in, FILE *fout)
{
	const char *p;

	for (p = in; *p; p++)
		switch (*p)
		{
			case '\\':
				fputs("\\(rs", fout);
				break;
			default:
				fputc(*p, fout);
		}
}


static void
print_troff_ms_text(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned short opt_border = cont->opt->border;
	unsigned int i;
	const char *const * ptr;

	if (cancel_pressed)
		return;

	if (opt_border > 2)
		opt_border = 2;

	if (cont->opt->start_table)
	{
		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs(".LP\n.DS C\n", fout);
			troff_ms_escaped_print(cont->title, fout);
			fputs("\n.DE\n", fout);
		}

		/* begin environment and set alignments and borders */
		fputs(".LP\n.TS\n", fout);
		if (opt_border == 2)
			fputs("center box;\n", fout);
		else
			fputs("center;\n", fout);

		for (i = 0; i < cont->ncolumns; i++)
		{
			fputc(*(cont->aligns + i), fout);
			if (opt_border > 0 && i < cont->ncolumns - 1)
				fputs(" | ", fout);
		}
		fputs(".\n", fout);

		/* print headers */
		if (!opt_tuples_only)
		{
			for (i = 0, ptr = cont->headers; i < cont->ncolumns; i++, ptr++)
			{
				if (i != 0)
					fputc('\t', fout);
				fputs("\\fI", fout);
				troff_ms_escaped_print(*ptr, fout);
				fputs("\\fP", fout);
			}
			fputs("\n_\n", fout);
		}
	}

	/* print cells */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		troff_ms_escaped_print(*ptr, fout);

		if ((i + 1) % cont->ncolumns == 0)
		{
			fputc('\n', fout);
			if (cancel_pressed)
				break;
		}
		else
			fputc('\t', fout);
	}

	if (cont->opt->stop_table)
	{
		printTableFooter *footers = footers_with_default(cont);

		fputs(".TE\n.DS L\n", fout);

		/* print footers */
		if (footers && !opt_tuples_only && !cancel_pressed)
		{
			printTableFooter *f;

			for (f = footers; f; f = f->next)
			{
				troff_ms_escaped_print(f->data, fout);
				fputc('\n', fout);
			}
		}

		fputs(".DE\n", fout);
	}
}


static void
print_troff_ms_vertical(const printTableContent *cont, FILE *fout)
{
	bool		opt_tuples_only = cont->opt->tuples_only;
	unsigned short opt_border = cont->opt->border;
	unsigned long record = cont->opt->prior_records + 1;
	unsigned int i;
	const char *const * ptr;
	unsigned short current_format = 0;	/* 0=none, 1=header, 2=body */

	if (cancel_pressed)
		return;

	if (opt_border > 2)
		opt_border = 2;

	if (cont->opt->start_table)
	{
		/* print title */
		if (!opt_tuples_only && cont->title)
		{
			fputs(".LP\n.DS C\n", fout);
			troff_ms_escaped_print(cont->title, fout);
			fputs("\n.DE\n", fout);
		}

		/* begin environment and set alignments and borders */
		fputs(".LP\n.TS\n", fout);
		if (opt_border == 2)
			fputs("center box;\n", fout);
		else
			fputs("center;\n", fout);

		/* basic format */
		if (opt_tuples_only)
			fputs("c l;\n", fout);
	}
	else
		current_format = 2;		/* assume tuples printed already */

	/* print records */
	for (i = 0, ptr = cont->cells; *ptr; i++, ptr++)
	{
		/* new record */
		if (i % cont->ncolumns == 0)
		{
			if (cancel_pressed)
				break;
			if (!opt_tuples_only)
			{
				if (current_format != 1)
				{
					if (opt_border == 2 && record > 1)
						fputs("_\n", fout);
					if (current_format != 0)
						fputs(".T&\n", fout);
					fputs("c s.\n", fout);
					current_format = 1;
				}
				fprintf(fout, "\\fIRecord %lu\\fP\n", record++);
			}
			if (opt_border >= 1)
				fputs("_\n", fout);
		}

		if (!opt_tuples_only)
		{
			if (current_format != 2)
			{
				if (current_format != 0)
					fputs(".T&\n", fout);
				if (opt_border != 1)
					fputs("c l.\n", fout);
				else
					fputs("c | l.\n", fout);
				current_format = 2;
			}
		}

		troff_ms_escaped_print(cont->headers[i % cont->ncolumns], fout);
		fputc('\t', fout);
		troff_ms_escaped_print(*ptr, fout);

		fputc('\n', fout);
	}

	if (cont->opt->stop_table)
	{
		fputs(".TE\n.DS L\n", fout);

		/* print footers */
		if (cont->footers && !opt_tuples_only && !cancel_pressed)
		{
			printTableFooter *f;

			for (f = cont->footers; f; f = f->next)
			{
				troff_ms_escaped_print(f->data, fout);
				fputc('\n', fout);
			}
		}

		fputs(".DE\n", fout);
	}
}


/********************************/
/* Public functions		*/
/********************************/


/*
 * PageOutput
 *
 * Tests if pager is needed and returns appropriate FILE pointer.
 */
FILE *
PageOutput(int lines, unsigned short int pager)
{
	/* check whether we need / can / are supposed to use pager */
	if (pager && isatty(fileno(stdin)) && isatty(fileno(stdout)))
	{
		const char *pagerprog;
		FILE	   *pagerpipe;

#ifdef TIOCGWINSZ
		int			result;
		struct winsize screen_size;

		result = ioctl(fileno(stdout), TIOCGWINSZ, &screen_size);

		/* >= accounts for a one-line prompt */
		if (result == -1 || lines >= screen_size.ws_row || pager > 1)
		{
#endif
			pagerprog = getenv("PAGER");
			if (!pagerprog)
				pagerprog = DEFAULT_PAGER;
#ifndef WIN32
			pqsignal(SIGPIPE, SIG_IGN);
#endif
			pagerpipe = popen(pagerprog, "w");
			if (pagerpipe)
				return pagerpipe;
#ifdef TIOCGWINSZ
		}
#endif
	}

	return stdout;
}

/*
 * ClosePager
 *
 * Close previously opened pager pipe, if any
 */
void
ClosePager(FILE *pagerpipe)
{
	if (pagerpipe && pagerpipe != stdout)
	{
		/*
		 * If printing was canceled midstream, warn about it.
		 *
		 * Some pagers like less use Ctrl-C as part of their command set. Even
		 * so, we abort our processing and warn the user what we did.  If the
		 * pager quit as a result of the SIGINT, this message won't go
		 * anywhere ...
		 */
		if (cancel_pressed)
			fprintf(pagerpipe, _("Interrupted\n"));

		pclose(pagerpipe);
#ifndef WIN32
		pqsignal(SIGPIPE, SIG_DFL);
#endif
	}
}

/*
 * Initialise a table contents struct.
 *		Must be called before any other printTable method is used.
 *
 * The title is not duplicated; the caller must ensure that the buffer
 * is available for the lifetime of the printTableContent struct.
 *
 * If you call this, you must call printTableCleanup once you're done with the
 * table.
 */
void
printTableInit(printTableContent *const content, const printTableOpt *opt,
			   const char *title, const int ncolumns, const int nrows)
{
	content->opt = opt;
	content->title = title;
	content->ncolumns = ncolumns;
	content->nrows = nrows;

	content->headers = pg_local_calloc(ncolumns + 1,
									   sizeof(*content->headers));

	content->cells = pg_local_calloc(ncolumns * nrows + 1,
									 sizeof(*content->cells));

	content->cellmustfree = NULL;
	content->footers = NULL;

	content->aligns = pg_local_calloc(ncolumns + 1,
									  sizeof(*content->align));

	content->header = content->headers;
	content->cell = content->cells;
	content->footer = content->footers;
	content->align = content->aligns;
	content->cellsadded = 0;
}

/*
 * Add a header to the table.
 *
 * Headers are not duplicated; you must ensure that the header string is
 * available for the lifetime of the printTableContent struct.
 *
 * If translate is true, the function will pass the header through gettext.
 * Otherwise, the header will not be translated.
 *
 * align is either 'l' or 'r', and specifies the alignment for cells in this
 * column.
 */
void
printTableAddHeader(printTableContent *const content, char *header,
					const bool translate, const char align)
{
#ifndef ENABLE_NLS
	(void) translate;			/* unused parameter */
#endif

	if (content->header >= content->headers + content->ncolumns)
	{
		fprintf(stderr, _("Cannot add header to table content: "
						  "column count of %d exceeded.\n"),
				content->ncolumns);
		exit(EXIT_FAILURE);
	}

	*content->header = (char *) mbvalidate((unsigned char *) header,
										   content->opt->encoding);
#ifdef ENABLE_NLS
	if (translate)
		*content->header = _(*content->header);
#endif
	content->header++;

	*content->align = align;
	content->align++;
}

/*
 * Add a cell to the table.
 *
 * Cells are not duplicated; you must ensure that the cell string is available
 * for the lifetime of the printTableContent struct.
 *
 * If translate is true, the function will pass the cell through gettext.
 * Otherwise, the cell will not be translated.
 *
 * If mustfree is true, the cell string is freed by printTableCleanup().
 * Note: Automatic freeing of translatable strings is not supported.
 */
void
printTableAddCell(printTableContent *const content, char *cell,
				  const bool translate, const bool mustfree)
{
#ifndef ENABLE_NLS
	(void) translate;			/* unused parameter */
#endif

	if (content->cellsadded >= content->ncolumns * content->nrows)
	{
		fprintf(stderr, _("Cannot add cell to table content: "
						  "total cell count of %d exceeded.\n"),
				content->ncolumns * content->nrows);
		exit(EXIT_FAILURE);
	}

	*content->cell = (char *) mbvalidate((unsigned char *) cell,
										 content->opt->encoding);

#ifdef ENABLE_NLS
	if (translate)
		*content->cell = _(*content->cell);
#endif

	if (mustfree)
	{
		if (content->cellmustfree == NULL)
			content->cellmustfree = pg_local_calloc(
					   content->ncolumns * content->nrows + 1, sizeof(bool));

		content->cellmustfree[content->cellsadded] = true;
	}
	content->cell++;
	content->cellsadded++;
}

/*
 * Add a footer to the table.
 *
 * Footers are added as elements of a singly-linked list, and the content is
 * strdup'd, so there is no need to keep the original footer string around.
 *
 * Footers are never translated by the function.  If you want the footer
 * translated you must do so yourself, before calling printTableAddFooter.	The
 * reason this works differently to headers and cells is that footers tend to
 * be made of up individually translated components, rather than being
 * translated as a whole.
 */
void
printTableAddFooter(printTableContent *const content, const char *footer)
{
	printTableFooter *f;

	f = pg_local_calloc(1, sizeof(*f));
	f->data = pg_strdup(footer);

	if (content->footers == NULL)
		content->footers = f;
	else
		content->footer->next = f;

	content->footer = f;
}

/*
 * Change the content of the last-added footer.
 *
 * The current contents of the last-added footer are freed, and replaced by the
 * content given in *footer.  If there was no previous footer, add a new one.
 *
 * The content is strdup'd, so there is no need to keep the original string
 * around.
 */
void
printTableSetFooter(printTableContent *const content, const char *footer)
{
	if (content->footers != NULL)
	{
		free(content->footer->data);
		content->footer->data = pg_strdup(footer);
	}
	else
		printTableAddFooter(content, footer);
}

/*
 * Free all memory allocated to this struct.
 *
 * Once this has been called, the struct is unusable unless you pass it to
 * printTableInit() again.
 */
void
printTableCleanup(printTableContent *const content)
{
	if (content->cellmustfree)
	{
		int			i;

		for (i = 0; i < content->nrows * content->ncolumns; i++)
		{
			if (content->cellmustfree[i])
				free((char *) content->cells[i]);
		}
		free(content->cellmustfree);
		content->cellmustfree = NULL;
	}
	free(content->headers);
	free(content->cells);
	free(content->aligns);

	content->opt = NULL;
	content->title = NULL;
	content->headers = NULL;
	content->cells = NULL;
	content->aligns = NULL;
	content->header = NULL;
	content->cell = NULL;
	content->align = NULL;

	if (content->footers)
	{
		for (content->footer = content->footers; content->footer;)
		{
			printTableFooter *f;

			f = content->footer;
			content->footer = f->next;
			free(f->data);
			free(f);
		}
	}
	content->footers = NULL;
	content->footer = NULL;
}

/*
 * IsPagerNeeded
 *
 * Setup pager if required
 */
static void
IsPagerNeeded(const printTableContent *cont, const int extra_lines, bool expanded, FILE **fout,
			  bool *is_pager)
{
	if (*fout == stdout)
	{
		int			lines;

		if (expanded)
			lines = (cont->ncolumns + 1) * cont->nrows;
		else
			lines = cont->nrows + 1;

		if (!cont->opt->tuples_only)
		{
			printTableFooter *f;

			/*
			 * FIXME -- this is slightly bogus: it counts the number of
			 * footers, not the number of lines in them.
			 */
			for (f = cont->footers; f; f = f->next)
				lines++;
		}

		*fout = PageOutput(lines + extra_lines, cont->opt->pager);
		*is_pager = (*fout != stdout);
	}
	else
		*is_pager = false;
}

/*
 * Use this to print just any table in the supported formats.
 */
void
printTable(const printTableContent *cont, FILE *fout, FILE *flog)
{
	bool		is_pager = false;

	if (cancel_pressed)
		return;

	if (cont->opt->format == PRINT_NOTHING)
		return;

	/* print_aligned_*() handles the pager themselves */
	if (cont->opt->format != PRINT_ALIGNED &&
		cont->opt->format != PRINT_WRAPPED)
		IsPagerNeeded(cont, 0, (cont->opt->expanded == 1), &fout, &is_pager);

	/* print the stuff */

	if (flog)
		print_aligned_text(cont, flog);

	switch (cont->opt->format)
	{
		case PRINT_UNALIGNED:
			if (cont->opt->expanded == 1)
				print_unaligned_vertical(cont, fout);
			else
				print_unaligned_text(cont, fout);
			break;
		case PRINT_ALIGNED:
		case PRINT_WRAPPED:
			if (cont->opt->expanded == 1)
				print_aligned_vertical(cont, fout);
			else
				print_aligned_text(cont, fout);
			break;
		case PRINT_HTML:
			if (cont->opt->expanded == 1)
				print_html_vertical(cont, fout);
			else
				print_html_text(cont, fout);
			break;
		case PRINT_LATEX:
			if (cont->opt->expanded == 1)
				print_latex_vertical(cont, fout);
			else
				print_latex_text(cont, fout);
			break;
		case PRINT_TROFF_MS:
			if (cont->opt->expanded == 1)
				print_troff_ms_vertical(cont, fout);
			else
				print_troff_ms_text(cont, fout);
			break;
		default:
			fprintf(stderr, _("invalid output format (internal error): %d"),
					cont->opt->format);
			exit(EXIT_FAILURE);
	}

	if (is_pager)
		ClosePager(fout);
}

/*
 * Use this to print query results
 *
 * It calls printTable with all the things set straight.
 */
void
printQuery(const PGresult *result, const printQueryOpt *opt, FILE *fout, FILE *flog)
{
	printTableContent cont;
	int			i,
				r,
				c;

	if (cancel_pressed)
		return;

	printTableInit(&cont, &opt->topt, opt->title,
				   PQnfields(result), PQntuples(result));

	for (i = 0; i < cont.ncolumns; i++)
	{
		char		align;
		Oid			ftype = PQftype(result, i);

		switch (ftype)
		{
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case FLOAT4OID:
			case FLOAT8OID:
			case NUMERICOID:
			case OIDOID:
			case XIDOID:
			case CIDOID:
			case CASHOID:
				align = 'r';
				break;
			default:
				align = 'l';
				break;
		}

		printTableAddHeader(&cont, PQfname(result, i),
							opt->translate_header, align);
	}

	/* set cells */
	for (r = 0; r < cont.nrows; r++)
	{
		for (c = 0; c < cont.ncolumns; c++)
		{
			char	   *cell;
			bool		mustfree = false;
			bool		translate;

			if (PQgetisnull(result, r, c))
				cell = opt->nullPrint ? opt->nullPrint : "";
			else
			{
				cell = PQgetvalue(result, r, c);
				if (cont.aligns[c] == 'r' && opt->topt.numericLocale)
				{
					cell = format_numeric_locale(cell);
					mustfree = true;
				}
			}

			translate = (opt->translate_columns && opt->translate_columns[c]);
			printTableAddCell(&cont, cell, translate, mustfree);
		}
	}

	/* set footers */
	if (opt->footers)
	{
		char	  **footer;

		for (footer = opt->footers; *footer; footer++)
			printTableAddFooter(&cont, *footer);
	}

	printTable(&cont, fout, flog);
	printTableCleanup(&cont);
}


void
setDecimalLocale(void)
{
	struct lconv *extlconv;

	extlconv = localeconv();

	if (*extlconv->decimal_point)
		decimal_point = pg_strdup(extlconv->decimal_point);
	else
		decimal_point = ".";	/* SQL output standard */
	if (*extlconv->grouping && atoi(extlconv->grouping) > 0)
		grouping = pg_strdup(extlconv->grouping);
	else
		grouping = "3";			/* most common */

	/* similar code exists in formatting.c */
	if (*extlconv->thousands_sep)
		thousands_sep = pg_strdup(extlconv->thousands_sep);
	/* Make sure thousands separator doesn't match decimal point symbol. */
	else if (strcmp(decimal_point, ",") != 0)
		thousands_sep = ",";
	else
		thousands_sep = ".";
}

/* get selected or default line style */
const printTextFormat *
get_line_style(const printTableOpt *opt)
{
	/*
	 * Note: this function mainly exists to preserve the convention that a
	 * printTableOpt struct can be initialized to zeroes to get default
	 * behavior.
	 */
	if (opt->line_style != NULL)
		return opt->line_style;
	else
		return &pg_asciiformat;
}

/*
 * Compute the byte distance to the end of the string or *target_width
 * display character positions, whichever comes first.	Update *target_width
 * to be the number of display character positions actually filled.
 */
static int
strlen_max_width(unsigned char *str, int *target_width, int encoding)
{
	unsigned char *start = str;
	unsigned char *end = str + strlen((char *) str);
	int			curr_width = 0;

	while (str < end)
	{
		int			char_width = PQdsplen((char *) str, encoding);

		/*
		 * If the display width of the new character causes the string to
		 * exceed its target width, skip it and return.  However, if this is
		 * the first character of the string (curr_width == 0), we have to
		 * accept it.
		 */
		if (*target_width < curr_width + char_width && curr_width != 0)
			break;

		curr_width += char_width;

		str += PQmblen((char *) str, encoding);
	}

	*target_width = curr_width;

	return str - start;
}
