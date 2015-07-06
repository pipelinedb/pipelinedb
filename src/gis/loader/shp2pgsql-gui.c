/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2008 OpenGeo.org
 * Copyright 2010 LISAsoft
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 * Maintainer: Paul Ramsey <pramsey@opengeo.org>
 *             Mark Leslie <mark.leslie@lisasoft.com>
 *
 **********************************************************************/

#include "../postgis_config.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <gtk/gtk.h>
#include <gdk/gdk.h>
#include <sys/stat.h>
#include "libpq-fe.h"
#include "shp2pgsql-core.h"
#include "pgsql2shp-core.h"

#define GUI_RCSID "shp2pgsql-gui $Revision$"
#define SHAPEFIELDMAXWIDTH 60

static void pgui_log_va(const char *fmt, va_list ap);
static void pgui_seterr_va(const char *fmt, va_list ap);

static void update_conn_ui_from_conn_config(void);

/* If GTK+ version is < 2.14.0, define gtk_dialog_get_content_area() */
#if !GTK_CHECK_VERSION(2, 14, 0)
	#if !defined(gtk_dialog_get_content_area)
		#define gtk_dialog_get_content_area(dialog) GTK_DIALOG(dialog)->vbox
	#endif
#endif

/*
** Global variables for GUI only
*/

/* Main window */
static GtkWidget *window_main = NULL;

static GtkWidget *textview_log = NULL;
static GtkTextBuffer *textbuffer_log = NULL;

/* Main import window (listview) */
GtkListStore *import_file_list_store;
GtkWidget *import_tree;
GtkCellRenderer *import_filename_renderer;
GtkCellRenderer *import_schema_renderer;
GtkCellRenderer *import_table_renderer;
GtkCellRenderer *import_geom_column_renderer;
GtkCellRenderer *import_srid_renderer;
GtkCellRenderer *import_mode_renderer;
GtkCellRenderer *import_remove_renderer;

GtkTreeViewColumn *import_filename_column;
GtkTreeViewColumn *import_schema_column;
GtkTreeViewColumn *import_table_column;
GtkTreeViewColumn *import_geom_column;
GtkTreeViewColumn *import_srid_column;
GtkTreeViewColumn *import_mode_column;
GtkTreeViewColumn *import_remove_column;

static GtkWidget *add_file_button = NULL;

GtkWidget *loader_mode_combo = NULL;
GtkListStore *loader_mode_combo_list;

/* Main export window (listview) */
GtkListStore *export_table_list_store;
GtkWidget *export_tree;
GtkWidget *export_geom_column_combo;
GtkCellRenderer *export_schema_renderer;
GtkCellRenderer *export_table_renderer;
GtkCellRenderer *export_geom_column_renderer;
GtkCellRenderer *export_filename_renderer;
GtkCellRenderer *export_remove_renderer;

GtkTreeViewColumn *export_schema_column;
GtkTreeViewColumn *export_table_column;
GtkTreeViewColumn *export_geom_column;
GtkTreeViewColumn *export_filename_column;
GtkTreeViewColumn *export_remove_column;

static GtkWidget *add_table_button = NULL;

/* PostgreSQL database connection window */
static GtkWidget *window_conn = NULL;

static GtkWidget *entry_pg_user = NULL;
static GtkWidget *entry_pg_pass = NULL;
static GtkWidget *entry_pg_host = NULL;
static GtkWidget *entry_pg_port = NULL;
static GtkWidget *entry_pg_db = NULL;

/* Loader options window */
static GtkWidget *dialog_loader_options = NULL;
static GtkWidget *entry_options_encoding = NULL;
static GtkWidget *checkbutton_loader_options_preservecase = NULL;
static GtkWidget *checkbutton_loader_options_forceint = NULL;
static GtkWidget *checkbutton_loader_options_autoindex = NULL;
static GtkWidget *checkbutton_loader_options_dbfonly = NULL;
static GtkWidget *checkbutton_loader_options_dumpformat = NULL;
static GtkWidget *checkbutton_loader_options_geography = NULL;
static GtkWidget *checkbutton_loader_options_simplegeoms = NULL;

/* Dumper options window */
static GtkWidget *dialog_dumper_options = NULL;
static GtkWidget *checkbutton_dumper_options_includegid = NULL;
static GtkWidget *checkbutton_dumper_options_keep_fieldname_case = NULL;
static GtkWidget *checkbutton_dumper_options_unescapedattrs = NULL;

/* About dialog */
static GtkWidget *dialog_about = NULL;

/* File chooser */
static GtkWidget *dialog_filechooser = NULL;
static GtkWidget *dialog_folderchooser = NULL;

/* Progress dialog */
static GtkWidget *dialog_progress = NULL;
static GtkWidget *progress = NULL;
static GtkWidget *label_progress = NULL;

/* Table chooser dialog */
static GtkWidget *dialog_tablechooser = NULL;
GtkListStore *chooser_filtered_table_list_store;
GtkListStore *chooser_table_list_store;
GtkWidget *chooser_tree;
GtkCellRenderer *chooser_schema_renderer;
GtkCellRenderer *chooser_table_renderer;
GtkTreeViewColumn *chooser_schema_column;
GtkTreeViewColumn *chooser_table_column;
static GtkWidget *checkbutton_chooser_geoonly = NULL;

/* Other items */
static int valid_connection = 0;

/* Constants for the list view etc. */
enum
{
	IMPORT_POINTER_COLUMN,
	IMPORT_FILENAME_COLUMN,
	IMPORT_SCHEMA_COLUMN,
	IMPORT_TABLE_COLUMN,
	IMPORT_GEOMETRY_COLUMN,
	IMPORT_SRID_COLUMN,
	IMPORT_MODE_COLUMN,
	IMPORT_REMOVE_COLUMN,
	IMPORT_N_COLUMNS
};

enum
{
	LOADER_MODE_COMBO_TEXT,
	LOADER_MODE_COMBO_OPTION_CHAR,
	LOADER_MODE_COMBO_COLUMNS
};

enum
{
	CREATE_MODE,
	APPEND_MODE,
	DELETE_MODE,
	PREPARE_MODE
};

enum
{
	EXPORT_POINTER_COLUMN,
	EXPORT_SCHEMA_COLUMN,
	EXPORT_TABLE_COLUMN,
	EXPORT_GEOMETRY_COLUMN,
	EXPORT_GEOMETRY_LISTSTORE_COLUMN,
	EXPORT_FILENAME_COLUMN,
	EXPORT_REMOVE_COLUMN,
	EXPORT_N_COLUMNS
};

enum
{
	TABLECHOOSER_SCHEMA_COLUMN,
	TABLECHOOSER_TABLE_COLUMN,
	TABLECHOOSER_GEO_LISTSTORE_COLUMN,
	TABLECHOOSER_GEO_COLUMN,
	TABLECHOOSER_HASGEO_COLUMN,
	TABLECHOOSER_N_COLUMNS
};

enum
{
	TABLECHOOSER_GEOCOL_COMBO_TEXT,
	TABLECHOOSER_GEOCOL_COMBO_COLUMNS
};

/* Other */
#define GUIMSG_LINE_MAXLEN 256
static char pgui_errmsg[GUIMSG_LINE_MAXLEN+1];
static PGconn *pg_connection = NULL;
static SHPCONNECTIONCONFIG *conn = NULL;
static SHPLOADERCONFIG *global_loader_config = NULL;
static SHPDUMPERCONFIG *global_dumper_config = NULL;

static volatile int is_running = FALSE;

/* Local prototypes */
static void pgui_sanitize_connection_string(char *connection_string);


/*
** Write a message to the Import Log text area.
*/
void
pgui_log_va(const char *fmt, va_list ap)
{
	char msg[GUIMSG_LINE_MAXLEN+1];
	GtkTextIter iter;

	if ( -1 == vsnprintf (msg, GUIMSG_LINE_MAXLEN, fmt, ap) ) return;
	msg[GUIMSG_LINE_MAXLEN] = '\0';

	/* Append text to the end of the text area, scrolling if required to make it visible */
	gtk_text_buffer_get_end_iter(textbuffer_log, &iter);
	gtk_text_buffer_insert(textbuffer_log, &iter, msg, -1);
	gtk_text_buffer_insert(textbuffer_log, &iter, "\n", -1);

	gtk_text_view_scroll_to_iter(GTK_TEXT_VIEW(textview_log), &iter, 0.0, TRUE, 0.0, 1.0);

	/* Allow GTK to process events */
	while (gtk_events_pending())
		gtk_main_iteration();
}

/*
** Write a message to the Import Log text area.
*/
static void
pgui_logf(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	pgui_log_va(fmt, ap);

	va_end(ap);
	return;
}

/* Write an error message */
void
pgui_seterr_va(const char *fmt, va_list ap)
{
	if ( -1 == vsnprintf (pgui_errmsg, GUIMSG_LINE_MAXLEN, fmt, ap) ) return;
	pgui_errmsg[GUIMSG_LINE_MAXLEN] = '\0';
}

static void
pgui_seterr(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	
	pgui_seterr_va(fmt, ap);
	
	va_end(ap);
	return;
}

static void
pgui_raise_error_dialogue(void)
{
	GtkWidget *dialog, *label;

	label = gtk_label_new(pgui_errmsg);
	dialog = gtk_dialog_new_with_buttons(_("Error"), GTK_WINDOW(window_main),
	                                     GTK_DIALOG_MODAL & GTK_DIALOG_NO_SEPARATOR & GTK_DIALOG_DESTROY_WITH_PARENT,
	                                     GTK_STOCK_OK, GTK_RESPONSE_OK, NULL);
	gtk_dialog_set_has_separator(GTK_DIALOG(dialog), FALSE );
	gtk_container_set_border_width(GTK_CONTAINER(dialog), 5);
	gtk_container_set_border_width(GTK_CONTAINER (GTK_DIALOG(dialog)->vbox), 15);
	gtk_container_add(GTK_CONTAINER(GTK_DIALOG(dialog)->vbox), label);
	gtk_widget_show_all(dialog);
	gtk_dialog_run(GTK_DIALOG(dialog));
	gtk_widget_destroy(dialog);
	return;
}

/*
** Run a SQL command against the current connection.
*/
static int
pgui_exec(const char *sql)
{
	PGresult *res = NULL;
	ExecStatusType status;
	char sql_trunc[256];

	/* We need a connection to do anything. */
	if ( ! pg_connection ) return 0;
	if ( ! sql ) return 0;

	res = PQexec(pg_connection, sql);
	status = PQresultStatus(res);
	PQclear(res);

	/* Did something unexpected happen? */
	if ( ! ( status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK ) )
	{
		/* Log notices and return success. */
		if ( status == PGRES_NONFATAL_ERROR )
		{
			pgui_logf("%s", PQerrorMessage(pg_connection));
			return 1;
		}

		/* Log errors and return failure. */
		snprintf(sql_trunc, 255, "%s", sql);
		pgui_logf("Failed SQL begins: \"%s\"", sql_trunc);
		pgui_logf("Failed in pgui_exec(): %s", PQerrorMessage(pg_connection));
		return 0;
	}

	return 1;
}

/*
** Start the COPY process.
*/
static int
pgui_copy_start(const char *sql)
{
	PGresult *res = NULL;
	ExecStatusType status;
	char sql_trunc[256];

	/* We need a connection to do anything. */
	if ( ! pg_connection ) return 0;
	if ( ! sql ) return 0;

	res = PQexec(pg_connection, sql);
	status = PQresultStatus(res);
	PQclear(res);

	/* Did something unexpected happen? */
	if ( status != PGRES_COPY_IN )
	{
		/* Log errors and return failure. */
		snprintf(sql_trunc, 255, "%s", sql);
		pgui_logf("Failed SQL begins: \"%s\"", sql_trunc);
		pgui_logf("Failed in pgui_copy_start(): %s", PQerrorMessage(pg_connection));
		return 0;
	}

	return 1;
}

/*
** Send a line (row) of data into the COPY procedure.
*/
static int
pgui_copy_write(const char *line)
{
	char line_trunc[256];

	/* We need a connection to do anything. */
	if ( ! pg_connection ) return 0;
	if ( ! line ) return 0;

	/* Did something unexpected happen? */
	if ( PQputCopyData(pg_connection, line, strlen(line)) < 0 )
	{
		/* Log errors and return failure. */
		snprintf(line_trunc, 255, "%s", line);
		pgui_logf("Failed row begins: \"%s\"", line_trunc);
		pgui_logf("Failed in pgui_copy_write(): %s", PQerrorMessage(pg_connection));
		return 0;
	}

	/* Send linefeed to signify end of line */
	PQputCopyData(pg_connection, "\n", 1);

	return 1;
}

/*
** Finish the COPY process.
*/
static int
pgui_copy_end(const int rollback)
{
	char *errmsg = NULL;

	/* We need a connection to do anything. */
	if ( ! pg_connection ) return 0;

	if ( rollback ) errmsg = "Roll back the copy.";

	/* Did something unexpected happen? */
	if ( PQputCopyEnd(pg_connection, errmsg) < 0 )
	{
		/* Log errors and return failure. */
		pgui_logf("Failed in pgui_copy_end(): %s", PQerrorMessage(pg_connection));
		return 0;
	}

	return 1;
}

/*
 * Ensures that the filename field width is within the stated bounds, and
 * 'appropriately' sized, for some definition of 'appropriately'.
 */
static void
update_filename_field_width(void)
{
	GtkTreeIter iter;
	gboolean is_valid;
	gchar *filename;
	int max_width;
	
	/* Loop through the list store to find the maximum length of an entry */
	max_width = 0;
	is_valid = gtk_tree_model_get_iter_first(GTK_TREE_MODEL(import_file_list_store), &iter);
	while (is_valid)
	{
		/* Grab the length of the filename entry in characters */
		gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), &iter, IMPORT_FILENAME_COLUMN, &filename, -1);
		if (strlen(filename) > max_width)
			max_width = strlen(filename);
		
		/* Get next entry */
		is_valid = gtk_tree_model_iter_next(GTK_TREE_MODEL(import_file_list_store), &iter);
	}
	
	/* Note the layout manager will handle the minimum size for us; we just need to be concerned with
	   making sure we don't exceed a maximum limit */
	if (max_width > SHAPEFIELDMAXWIDTH)
		g_object_set(import_filename_renderer, "width-chars", SHAPEFIELDMAXWIDTH, NULL);
	else
		g_object_set(import_filename_renderer, "width-chars", -1, NULL);
	
	return;
}

/*
 * This will create a connection to the database, just to see if it can.
 * It cleans up after itself like a good little function and maintains
 * the status of the valid_connection parameter.
 */
static int
connection_test(void)
{
	char *connection_string = NULL;
	char *connection_sanitized = NULL;

	if (!(connection_string = ShpDumperGetConnectionStringFromConn(conn)))
	{
		pgui_raise_error_dialogue();
		valid_connection = 0;
		return 0;
	}

	connection_sanitized = strdup(connection_string);
	pgui_sanitize_connection_string(connection_sanitized);
	pgui_logf("Connecting: %s", connection_sanitized);
	free(connection_sanitized);

	pg_connection = PQconnectdb(connection_string);
	if (PQstatus(pg_connection) == CONNECTION_BAD)
	{
		pgui_logf( _("Database connection failed: %s"), PQerrorMessage(pg_connection));
		free(connection_string);
		PQfinish(pg_connection);
		pg_connection = NULL;
		valid_connection = 0;
		return 0;
	}
	PQfinish(pg_connection);
	pg_connection = NULL;
	free(connection_string);

	valid_connection = 1;
	return 1;
}


/* === Generic window functions === */

/* Delete event handler for popups that simply returns TRUE to prevent GTK from
   destroying the window and then hides it manually */
static gint
pgui_event_popup_delete(GtkWidget *widget, GdkEvent *event, gpointer data)
{
	gtk_widget_hide(GTK_WIDGET(widget));
	return TRUE;
}

/* === Progress window functions === */

static void
pgui_action_progress_cancel(GtkDialog *dialog, gint response_id, gpointer user_data) 
{
	/* Stop the current import */
	is_running = FALSE;

	return;
}

static gint
pgui_action_progress_delete(GtkWidget *widget, GdkEvent *event, gpointer data)
{
	/* Stop the current import */
	is_running = FALSE;

	return TRUE;
}


/* === Loader option Window functions === */

/* Update the specified SHPLOADERCONFIG with the global settings from the Options dialog */
static void
update_loader_config_globals_from_options_ui(SHPLOADERCONFIG *config)
{
	const char *entry_encoding = gtk_entry_get_text(GTK_ENTRY(entry_options_encoding));
	gboolean preservecase = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_preservecase));
	gboolean forceint = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_forceint));
	gboolean createindex = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_autoindex));
	gboolean dbfonly = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_dbfonly));
	gboolean dumpformat = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_dumpformat));
	gboolean geography = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_geography));
	gboolean simplegeoms = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_simplegeoms));
	
	if (geography)
	{
		config->geography = 1;
		
		if (config->geo_col)
			free(config->geo_col);
		
		config->geo_col = strdup(GEOGRAPHY_DEFAULT);
	}
	else
	{
		config->geography = 0;

		if (config->geo_col)
			free(config->geo_col);
		
		config->geo_col = strdup(GEOMETRY_DEFAULT);
	}

	/* Encoding */
	if (entry_encoding && strlen(entry_encoding) > 0)
	{
		if (config->encoding)
			free(config->encoding);

		config->encoding = strdup(entry_encoding);
	}

	/* Preserve case */
	if (preservecase)
		config->quoteidentifiers = 1;
	else
		config->quoteidentifiers = 0;

	/* No long integers in table */
	if (forceint)
		config->forceint4 = 1;
	else
		config->forceint4 = 0;

	/* Create spatial index after load */
	if (createindex)
		config->createindex = 1;
	else
		config->createindex = 0;

	/* Read the .shp file, don't ignore it */
	if (dbfonly)
	{
		config->readshape = 0;
		
		/* There will be no spatial column so don't create a spatial index */
		config->createindex = 0; 
	}
	else
		config->readshape = 1;

	/* Use COPY rather than INSERT format */
	if (dumpformat)
		config->dump_format = 1;
	else
		config->dump_format = 0;
	
	/* Simple geometries only */
	if (simplegeoms)
		config->simple_geometries = 1;
	else
		config->simple_geometries = 0;	
	
	return;
}

/* Update the loader options dialog with the current values from the global config */
static void
update_options_ui_from_loader_config_globals(void)
{
	gtk_entry_set_text(GTK_ENTRY(entry_options_encoding), global_loader_config->encoding);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_preservecase), global_loader_config->quoteidentifiers ? TRUE : FALSE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_forceint), global_loader_config->forceint4 ? TRUE : FALSE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_autoindex), global_loader_config->createindex ? TRUE : FALSE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_dbfonly), global_loader_config->readshape ? FALSE : TRUE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_dumpformat), global_loader_config->dump_format ? TRUE : FALSE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_geography), global_loader_config->geography ? TRUE : FALSE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_loader_options_simplegeoms), global_loader_config->simple_geometries ? TRUE : FALSE);
	
	return;
}
	
/* Set the global config variables controlled by the options dialogue */
static void
pgui_set_loader_configs_from_options_ui()
{
	GtkTreeIter iter;
	gboolean is_valid;
	gpointer gptr;
	SHPLOADERCONFIG *loader_file_config;
	
	/* First update the global (template) configuration */
	update_loader_config_globals_from_options_ui(global_loader_config);

	/* Now also update the same settings for any existing files already added. We
	   do this by looping through all entries and updating their config too. */
	is_valid = gtk_tree_model_get_iter_first(GTK_TREE_MODEL(import_file_list_store), &iter);
	while (is_valid)
	{
		/* Get the SHPLOADERCONFIG for this file entry */
		gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), &iter, IMPORT_POINTER_COLUMN, &gptr, -1);
		loader_file_config = (SHPLOADERCONFIG *)gptr;
		
		/* Update it */
		update_loader_config_globals_from_options_ui(loader_file_config);
		
		/* Get next entry */
		is_valid = gtk_tree_model_iter_next(GTK_TREE_MODEL(import_file_list_store), &iter);
	}
	
	return;
}


/* === Table selection dialog functions === */

/* Load the model with information from the database */
static void
update_table_chooser_from_database()
{
	PGresult *result, *geocol_result;
	GtkTreeIter iter, geocol_iter;
	GtkListStore *dumper_geocol_combo_list;
	char *connection_string, *sql_form, *query, *schema, *table, *geocol_query, *geocol_name=NULL;
	int hasgeo, i, j;
	
	/* Open a connection to the database */
	connection_string = ShpDumperGetConnectionStringFromConn(conn);
	pg_connection = PQconnectdb(connection_string);
	
	/* Here we find a list of all tables/views that not in a pg_* schema (or information_schema) and
	   we return the schema name, table name and whether or not the table/view contains any geo
	   columns */
	query = "SELECT tableoids.oid, n.nspname, tableoids.relname, COALESCE((SELECT 1 from pg_attribute WHERE attrelid = tableoids.oid AND atttypid IN (SELECT oid FROM pg_type WHERE typname in ('geometry', 'geography')) LIMIT 1), 0) hasgeo FROM (SELECT c.oid, c.relname, c.relnamespace FROM pg_class c WHERE c.relkind IN ('r', 'v') AND c.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname NOT ILIKE 'pg_%' AND nspname <> 'information_schema')) tableoids, pg_namespace n WHERE tableoids.relnamespace = n.oid ORDER BY n.nspname, tableoids.relname";

	result = PQexec(pg_connection, query);

	/* Free any existing entries in the model */
	gtk_list_store_clear(chooser_table_list_store);
	
	/* Now insert one row for each query result */
	for (i = 0; i < PQntuples(result); i++)
	{
		gtk_list_store_insert_before(chooser_table_list_store, &iter, NULL);

		/* Look up the geo columns; if there are none then we set the field to (None). If we have just one
		   column then we set the column name directly. If we have more than one then we create a combo
		   dropdown containing the column names */
		schema = PQgetvalue(result, i, PQfnumber(result, "nspname"));
		table = PQgetvalue(result, i, PQfnumber(result, "relname"));
		
		sql_form = "SELECT n.nspname, c.relname, a.attname FROM pg_class c, pg_namespace n, pg_attribute a WHERE c.relnamespace = n.oid AND n.nspname = '%s' AND c.relname = '%s' AND a.attrelid = c.oid AND a.atttypid IN (SELECT oid FROM pg_type WHERE typname in ('geometry', 'geography'))";
		
		geocol_query = malloc(strlen(sql_form) + strlen(schema) + strlen(table) + 1);
		sprintf(geocol_query, sql_form, schema, table);
		
		geocol_result = PQexec(pg_connection, geocol_query);

		/* Create a combo list loaded with the column names. Note that while we create the model and load
		   the data here, we don't actually display the geo column in this dialog. Instead we build the
		   list here so that we can pass to the export table list store when creating a new entry. This
		   is to ensure that the export table list model can directly represent a SHPDUMPERCONFIG. */
		dumper_geocol_combo_list = gtk_list_store_new(TABLECHOOSER_GEOCOL_COMBO_COLUMNS, G_TYPE_STRING);
		
		if (PQntuples(geocol_result) > 0)
		{
			/* Load the columns into the list store */
			for (j = 0; j < PQntuples(geocol_result); j++)
			{
				geocol_name = PQgetvalue(geocol_result, j, PQfnumber(geocol_result, "attname"));
			
				gtk_list_store_insert_before(dumper_geocol_combo_list, &geocol_iter, (GtkTreeIter *)TABLECHOOSER_GEOCOL_COMBO_TEXT);
				gtk_list_store_set(dumper_geocol_combo_list, &geocol_iter, 
						TABLECHOOSER_GEOCOL_COMBO_TEXT, geocol_name,
						-1);
			}
		}
		else
		{
			/* Add a "default" entry */
			geocol_name = NULL;
			
			gtk_list_store_insert_before(dumper_geocol_combo_list, &geocol_iter, (GtkTreeIter *)TABLECHOOSER_GEOCOL_COMBO_TEXT);
			gtk_list_store_set(dumper_geocol_combo_list, &geocol_iter, 
						TABLECHOOSER_GEOCOL_COMBO_TEXT, _("(None)"),
						-1);
		}	

		/* Free the query result */
		PQclear(geocol_result);

		/* Free the query string */
		free(geocol_query);
		
		/* Set the list store data */
		hasgeo = atoi(PQgetvalue(result, i, PQfnumber(result, "hasgeo")));
		gtk_list_store_set(chooser_table_list_store, &iter,
			   TABLECHOOSER_SCHEMA_COLUMN, schema,
	                   TABLECHOOSER_TABLE_COLUMN, table,
		           TABLECHOOSER_GEO_LISTSTORE_COLUMN, dumper_geocol_combo_list,
			   TABLECHOOSER_GEO_COLUMN, geocol_name,
	                   TABLECHOOSER_HASGEO_COLUMN, hasgeo,
	                   -1);
	}
	
	/* Clear up the result set */
	PQclear(result);
	
	/* Close the existing connection */
	PQfinish(pg_connection);
	pg_connection = NULL;
	
	return;
}

/* GtkTreeModelFilter visibility function */
static gboolean
table_chooser_visibility_func(GtkTreeModel *model, GtkTreeIter *iter, gpointer data)
{
	/* First determine whether the hasgeo tickbox is selected or not */
	gboolean geoonly = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_chooser_geoonly));
	int hasgeo;
	
	/* If unticked then we show all tables */
	if (!geoonly)
		return TRUE;
	else
	{
		/* Otherwise we only show the tables with geo columns */
		gtk_tree_model_get(GTK_TREE_MODEL(model), iter, TABLECHOOSER_HASGEO_COLUMN, &hasgeo, -1);
		if (hasgeo)
			return TRUE;
		else
			return FALSE;
	}
	
	return FALSE;
}

/* === Dumper option Window functions === */

/* Update the specified SHPDUMPERCONFIG with the global settings from the Options dialog */
static void
update_dumper_config_globals_from_options_ui(SHPDUMPERCONFIG *config)
{
	gboolean includegid = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_dumper_options_includegid));
	gboolean keep_fieldname_case = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_dumper_options_keep_fieldname_case));
	gboolean unescapedattrs = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(checkbutton_dumper_options_unescapedattrs));

	/* Include gid or not */
	if (includegid)
		config->includegid = 1;
	else
		config->includegid = 0;

	/* Keep fieldname case */
	if (keep_fieldname_case)
		config->keep_fieldname_case = 1;
	else
		config->keep_fieldname_case = 0;

	/* Escape column names or not */
	if (unescapedattrs)
		config->unescapedattrs = 1;
	else
		config->unescapedattrs = 0;

	return;
}

/* Update the options dialog with the current values from the global config */
static void
update_options_ui_from_dumper_config_globals(void)
{
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_dumper_options_includegid), global_dumper_config->includegid ? TRUE : FALSE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_dumper_options_keep_fieldname_case), global_dumper_config->keep_fieldname_case ? TRUE : FALSE);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_dumper_options_unescapedattrs), global_dumper_config->unescapedattrs ? TRUE : FALSE);
	
	return;
}

/* Set the global config variables controlled by the options dialogue */
static void
pgui_set_dumper_configs_from_options_ui()
{
	GtkTreeIter iter;
	gboolean is_valid;
	gpointer gptr;
	SHPDUMPERCONFIG *dumper_table_config;
	
	/* First update the global (template) configuration */
	update_dumper_config_globals_from_options_ui(global_dumper_config);

	/* Now also update the same settings for any existing tables already added. We
	   do this by looping through all entries and updating their config too. */
	is_valid = gtk_tree_model_get_iter_first(GTK_TREE_MODEL(export_table_list_store), &iter);
	while (is_valid)
	{
		/* Get the SHPDUMPERCONFIG for this file entry */
		gtk_tree_model_get(GTK_TREE_MODEL(export_table_list_store), &iter, EXPORT_POINTER_COLUMN, &gptr, -1);
		dumper_table_config = (SHPDUMPERCONFIG *)gptr;
		
		/* Update it */
		update_dumper_config_globals_from_options_ui(dumper_table_config);
		
		/* Get next entry */
		is_valid = gtk_tree_model_iter_next(GTK_TREE_MODEL(export_table_list_store), &iter);
	}

	return;
}

/* Signal handler for ticking/unticking the "only show geo columns" box */
static void
pgui_action_chooser_toggle_show_geocolumn(GtkToggleButton *togglebutton, gpointer user_data)
{
	/* Simply update the listview filter */
	gtk_tree_model_filter_refilter(GTK_TREE_MODEL_FILTER(chooser_filtered_table_list_store));
	
	return;
}

static void
pgui_action_dumper_options_open(GtkWidget *widget, gpointer data)
{
	update_options_ui_from_dumper_config_globals();
	gtk_widget_show_all(dialog_dumper_options);
	return;
}

static void
pgui_action_dumper_options_close(GtkWidget *widget, gint response, gpointer data)
{
	/* Only update the configuration if the user hit OK */
	if (response == GTK_RESPONSE_OK)
		pgui_set_dumper_configs_from_options_ui();
	
	/* Hide the dialog */
	gtk_widget_hide(dialog_dumper_options);
	
	return;
}

/* === Main window functions === */

/* Given a filename, generate a new loader configuration */
static SHPLOADERCONFIG *
create_new_file_config(const char *filename)
{
	SHPLOADERCONFIG *loader_file_config;
	char *table_start, *table_end;
	int i;
	
	/* Generate a new configuration by copying the global options first and then
	   adding in the specific values for this file */
	loader_file_config = malloc(sizeof(SHPLOADERCONFIG));
	memcpy(loader_file_config, global_loader_config, sizeof(SHPLOADERCONFIG));
	
	/* Note: we must copy the encoding here since it is the only pass-by-reference
	   type set in set_loader_config_defaults() and each config needs its own copy
	   of any referenced items */
	loader_file_config->encoding = strdup(global_loader_config->encoding);
	
	/* Copy the filename (we'll remove the .shp extension in a sec) */
	loader_file_config->shp_file = strdup(filename);
	
	/* Generate the default table name from the filename */
	table_start = loader_file_config->shp_file + strlen(loader_file_config->shp_file);
	while (*table_start != '/' && *table_start != '\\' && table_start > loader_file_config->shp_file)
		table_start--;
	
	/* Forward one to start of actual characters */
	table_start++;

	/* Roll back from end to first . character. */
	table_end = loader_file_config->shp_file + strlen(loader_file_config->shp_file);
	while (*table_end != '.' && table_end > loader_file_config->shp_file && table_end > table_start )
		table_end--;

	/* Copy the table name */
	loader_file_config->table = malloc(table_end - table_start + 1);
	memcpy(loader_file_config->table, table_start, table_end - table_start);
	loader_file_config->table[table_end - table_start] = '\0';

	/* Force the table name to lower case */
	for (i = 0; i < table_end - table_start; i++)
	{
		if (isupper(loader_file_config->table[i]) != 0)
			loader_file_config->table[i] = tolower(loader_file_config->table[i]);
	}

	/* Set the default schema to public */
	loader_file_config->schema = strdup("public");
	
	/* Set the default geo column name */
	if (global_loader_config->geography)
		loader_file_config->geo_col = strdup(GEOGRAPHY_DEFAULT);
	else
		loader_file_config->geo_col = strdup(GEOMETRY_DEFAULT);
	
	return loader_file_config;
}

/* Given the loader configuration, add a new row representing this file to the listview */
static void
add_loader_file_config_to_list(SHPLOADERCONFIG *loader_file_config)
{
	GtkTreeIter iter;
#define MAXLEN 16
	char srid[MAXLEN+1];
	
	/* Convert SRID into string */
	if ( MAXLEN+1 <= snprintf(srid, MAXLEN+1, "%d", loader_file_config->sr_id) )
	{
		pgui_logf("Invalid SRID requiring more than %d digits: %d", MAXLEN, loader_file_config->sr_id);
		pgui_raise_error_dialogue();
		srid[MAXLEN] = '\0';
	}
	
	gtk_list_store_insert_before(import_file_list_store, &iter, NULL);
	gtk_list_store_set(import_file_list_store, &iter,
			   IMPORT_POINTER_COLUMN, loader_file_config,
	                   IMPORT_FILENAME_COLUMN, loader_file_config->shp_file,
	                   IMPORT_SCHEMA_COLUMN, loader_file_config->schema,
	                   IMPORT_TABLE_COLUMN, loader_file_config->table,
	                   IMPORT_GEOMETRY_COLUMN, loader_file_config->geo_col,
	                   IMPORT_SRID_COLUMN, srid,
	                   IMPORT_MODE_COLUMN, _("Create"),
	                   -1);	
		   
	/* Update the filename field width */
	update_filename_field_width();
	
	return;
}

/* Free up the specified SHPLOADERCONFIG */
static void
free_loader_config(SHPLOADERCONFIG *config)
{

	if (config->table)
		free(config->table);

	if (config->schema)
		free(config->schema);
	
	if (config->geo_col)
		free(config->geo_col);
	
	if (config->shp_file)
		free(config->shp_file);

	if (config->encoding)
		free(config->encoding);

	if (config->tablespace)
		free(config->tablespace);
	
	if (config->idxtablespace)
		free(config->idxtablespace);
	
	/* Free the config itself */
	free(config);
}

/* Given a table selection, generate a new configuration */
static SHPDUMPERCONFIG *
create_new_table_config(GtkTreeIter *iter)
{
	SHPDUMPERCONFIG *dumper_table_config;
	gchar *schema, *table, *geocol;
	gint hasgeo;
	
	/* Generate a new configuration by copying the global options first and then
	   adding in the specific values for this table */
	dumper_table_config = malloc(sizeof(SHPDUMPERCONFIG));
	memcpy(dumper_table_config, global_dumper_config, sizeof(SHPDUMPERCONFIG));
	
	/* Grab the values from the current iter */
	gtk_tree_model_get(GTK_TREE_MODEL(chooser_filtered_table_list_store), iter, 
			   TABLECHOOSER_SCHEMA_COLUMN, &schema,
			   TABLECHOOSER_TABLE_COLUMN, &table,
			   TABLECHOOSER_GEO_COLUMN, &geocol, 
			   TABLECHOOSER_HASGEO_COLUMN, &hasgeo,
			   -1);	
	
	/* Set up the values in the SHPDUMPERCONFIG */
	dumper_table_config->schema = strdup(schema);
	dumper_table_config->table = strdup(table);
	
	/* We also set the filename the same as the table name */
	dumper_table_config->shp_file = strdup(table);
		
	if (hasgeo && geocol)
		dumper_table_config->geo_col_name = strdup(geocol);
	else
		dumper_table_config->geo_col_name = NULL;
	
	return dumper_table_config;
}

/* Given the dumper configuration, add a new row representing this file to the listview. The liststore and iter arguments
are optional, and enable the user to specify additional information to the view, e.g. geo column multi-choice. */
static void
add_dumper_table_config_to_list(SHPDUMPERCONFIG *dumper_table_config, GtkListStore *chooser_liststore, GtkTreeIter *chooser_iter)
{
	GtkTreeIter iter;
	GtkListStore *geocol_liststore;
	
	gtk_list_store_insert_before(export_table_list_store, &iter, NULL);
	gtk_list_store_set(export_table_list_store, &iter,
			   EXPORT_POINTER_COLUMN, dumper_table_config,
			   EXPORT_SCHEMA_COLUMN, dumper_table_config->schema,
			   EXPORT_TABLE_COLUMN, dumper_table_config->table,
			   EXPORT_GEOMETRY_COLUMN, dumper_table_config->geo_col_name,
			   EXPORT_FILENAME_COLUMN, dumper_table_config->shp_file,
			   -1);
			  
	/* If we have supplied the table_chooser store for additional information, use it */
	if (chooser_liststore)
	{
		/* Let's add a multi-choice geometry column to the table */
		gtk_tree_model_get(GTK_TREE_MODEL(chooser_liststore), chooser_iter,
				   TABLECHOOSER_GEO_LISTSTORE_COLUMN, &geocol_liststore,
				   -1);
				   
		gtk_list_store_set(export_table_list_store, &iter,
				   EXPORT_GEOMETRY_LISTSTORE_COLUMN, geocol_liststore,
				   -1);
	}
	
	return;
}

/* Free up the specified SHPDUMPERCONFIG */
static void
free_dumper_config(SHPDUMPERCONFIG *config)
{

	if (config->table)
		free(config->table);

	if (config->schema)
		free(config->schema);
	
	if (config->geo_col_name)
		free(config->geo_col_name);
	
	if (config->shp_file)
		free(config->shp_file);
	
	/* Free the config itself */
	free(config);
}

/* Validate a single DBF column type against a PostgreSQL type: return either TRUE or FALSE depending
   upon whether or not the type is (broadly) compatible */
static int
validate_shape_column_against_pg_column(int dbf_fieldtype, char *pg_fieldtype)
{
	switch (dbf_fieldtype)
	{
		case FTString:
			/* Only varchar */
			if (!strcmp(pg_fieldtype, "varchar"))
				return -1;
			break;
			
		case FTDate:
			/* Only date */
			if (!strcmp(pg_fieldtype, "date"))
				return -1;
			break;
			
		case FTInteger:
			/* Tentatively allow int2, int4 and numeric */
			if (!strcmp(pg_fieldtype, "int2") || !strcmp(pg_fieldtype, "int4") || !strcmp(pg_fieldtype, "numeric"))
				return -1;
			break;
			
		case FTDouble:
			/* Only float8/numeric */
			if (!strcmp(pg_fieldtype, "float8") || !strcmp(pg_fieldtype, "numeric"))
				return -1;
			break;
			
		case FTLogical:
			/* Only boolean */
			if (!strcmp(pg_fieldtype, "boolean"))
				return -1;
			break;
	}
	
	/* Otherwise we can't guarantee this (but this is just a warning anyway) */
	return 0;
}

/* Validate column compatibility for the given loader configuration against the table/column
   list returned in result */
static int
validate_remote_loader_columns(SHPLOADERCONFIG *config, PGresult *result)
{
	ExecStatusType status;
	SHPLOADERSTATE *state;
	int ntuples;
	char *pg_fieldname, *pg_fieldtype;
	int ret, i, j, found, response = SHPLOADEROK;
	
	/* Check the status of the result set */
	status = PQresultStatus(result);
	if (status == PGRES_TUPLES_OK)
	{
		ntuples = PQntuples(result);
	
		switch (config->opt)
		{
			case 'c':
				/* If we have a row matching the table given in the config, then it already exists */
				if (ntuples > 0)
				{
					pgui_seterr(_("ERROR: Create mode selected for existing table: %s.%s"), config->schema, config->table);
					response = SHPLOADERERR;
				}	
				break;
			
			case 'p':
				/* If we have a row matching the table given in the config, then it already exists */
				if (ntuples > 0)
				{
					pgui_seterr(_("ERROR: Prepare mode selected for existing table: %s.%s"), config->schema, config->table);
					response = SHPLOADERERR;
				}
				break;	

			case 'a':
				/* If we are trying to append to a table but it doesn't exist, emit a warning */
				if (ntuples == 0)
				{
					pgui_seterr(_("ERROR: Destination table %s.%s could not be found for appending"), config->schema, config->table);
					response = SHPLOADERERR;
				}
				else
				{
					/* If we have a row then lets do some simple column validation... */
					state = ShpLoaderCreate(config);
					ret = ShpLoaderOpenShape(state);
					if (ret != SHPLOADEROK)
					{
						pgui_logf(_("Warning: Could not load shapefile %s"), config->shp_file);
						ShpLoaderDestroy(state);
					}
					
					/* Find each column based upon its name and then validate type separately... */
					for (i = 0; i < state->num_fields; i++)
					{
						/* Make sure we find a column */
						found = 0;
						for (j = 0; j < ntuples; j++)
						{
							pg_fieldname = PQgetvalue(result, j, PQfnumber(result, "field"));
							pg_fieldtype = PQgetvalue(result, j, PQfnumber(result, "type"));
						
							if (!strcmp(state->field_names[i], pg_fieldname))
							{
								found = -1;
								
								ret = validate_shape_column_against_pg_column(state->types[i], pg_fieldtype);
								if (!ret)
								{
									pgui_logf(_("Warning: DBF Field '%s' is not compatible with PostgreSQL column '%s' in %s.%s"), state->field_names[i], pg_fieldname, config->schema, config->table);
									response = SHPLOADERWARN;
								}
							}
						}
						
						/* Flag a warning if we can't find a match */
						if (!found)
						{
							pgui_logf(_("Warning: DBF Field '%s' within file %s could not be matched to a column within table %s.%s"), 
								  state->field_names[i], config->shp_file, config->schema, config->table);
							response = SHPLOADERWARN;
						}
					}
					
					ShpLoaderDestroy(state);
				}
				
				break;
		}
	}
	else
	{
		pgui_seterr(_("ERROR: unable to process validation response from remote server"));
		response = SHPLOADERERR;
	}
	
	return response;	
}

/* Terminate the main loop and exit the application. */
static void
pgui_quit (GtkWidget *widget, gpointer data)
{
	gtk_main_quit();
}

static void
pgui_action_about_open()
{
	/* Display the dialog and hide it again upon exit */
	gtk_dialog_run(GTK_DIALOG(dialog_about));
	gtk_widget_hide(dialog_about);
}

static void
pgui_action_cancel(GtkWidget *widget, gpointer data)
{
	if (!is_running)
		pgui_quit(widget, data); /* quit if we're not running */
	else
		is_running = FALSE;
}

static void
pgui_action_loader_options_open(GtkWidget *widget, gpointer data)
{
	update_options_ui_from_loader_config_globals();
	gtk_widget_show_all(dialog_loader_options);
	return;
}

static void
pgui_action_loader_options_close(GtkWidget *widget, gint response, gpointer data)
{
	/* Only update the configuration if the user hit OK */
	if (response == GTK_RESPONSE_OK)
		pgui_set_loader_configs_from_options_ui();
	
	/* Hide the dialog */
	gtk_widget_hide(dialog_loader_options);
	
	return;
}

static void
pgui_action_open_file_dialog(GtkWidget *widget, gpointer data)
{
	SHPLOADERCONFIG *loader_file_config;
	GSList *filename_list, *filename_item;
	gchar *filename;

	/* Make sure we deselect any files from the last time */
	gtk_file_chooser_unselect_all(GTK_FILE_CHOOSER(dialog_filechooser));
	
	/* Run the dialog */
	if (gtk_dialog_run(GTK_DIALOG(dialog_filechooser)) == GTK_RESPONSE_ACCEPT)
	{
		/* Create the new file configuration based upon the each filename and add it to the listview */
		filename_list = gtk_file_chooser_get_filenames(GTK_FILE_CHOOSER(dialog_filechooser));
		
		filename_item = g_slist_nth(filename_list, 0);
		while (filename_item)
		{
			/* Add the configuration */
			filename = g_slist_nth_data(filename_item, 0);
			
			loader_file_config = create_new_file_config(filename);
			add_loader_file_config_to_list(loader_file_config);
			
			/* Grab the next filename */
			filename_item = g_slist_next(filename_item);
		}
		
		/* Free the list */
		g_slist_free(filename_list);
	}
	
	gtk_widget_hide(dialog_filechooser);
}

static void
pgui_action_open_table_dialog(GtkWidget *widget, gpointer data)
{
	SHPDUMPERCONFIG *dumper_table_config;
	GtkTreeSelection *chooser_selection;
	GtkTreeModel *model;
	GList *selected_rows_list, *selected_row;
	GtkTreeIter iter;
	GtkTreePath *tree_path;
	
	/* Make sure we can connect to the database first */	
	if (!connection_test())
	{
		pgui_seterr(_("Unable to connect to the database - please check your connection settings"));
		pgui_raise_error_dialogue();

		/* Open the connections UI for the user */
		update_conn_ui_from_conn_config();

		gtk_widget_show_all(GTK_WIDGET(window_conn));
		return;
	}
	
	/* Setup the form */
	update_table_chooser_from_database();
	gtk_tree_model_filter_refilter(GTK_TREE_MODEL_FILTER(chooser_filtered_table_list_store));

	/* Run the dialog */
	gtk_widget_show_all(dialog_tablechooser);
	if (gtk_dialog_run(GTK_DIALOG(dialog_tablechooser)) == GTK_RESPONSE_OK)
	{
		/* Create the new dumper configuration based upon the selected iters and add them to the listview */
		chooser_selection = gtk_tree_view_get_selection(GTK_TREE_VIEW(chooser_tree));

		selected_rows_list = gtk_tree_selection_get_selected_rows(chooser_selection, &model);
		selected_row = g_list_first(selected_rows_list);
		while (selected_row)
		{
			/* Get the tree iter */
			tree_path = (GtkTreePath *)g_list_nth_data(selected_row, 0);
			gtk_tree_model_get_iter(model, &iter, tree_path);
			
			/* Get the config and add it to the list */
			dumper_table_config = create_new_table_config(&iter);	
			add_dumper_table_config_to_list(dumper_table_config, chooser_filtered_table_list_store, &iter);			
			
			/* Get the next row */
			selected_row = g_list_next(selected_row);
		}
		
		/* Free the GList */
		g_list_foreach(selected_row, (GFunc)gtk_tree_path_free, NULL);
		g_list_free(selected_row);
	}
	
	gtk_widget_hide(dialog_tablechooser);
}

/*
 * Signal handler for the remove box.  Performs no user interaction, simply
 * removes the row from the table.
 */
static void
pgui_action_handle_table_remove(GtkCellRendererToggle *renderer,
                               gchar *path,
                               gpointer user_data)
{
	GtkTreeIter iter;
	SHPDUMPERCONFIG *dumper_table_config;
	gpointer gptr;
	
	/* Grab the SHPDUMPERCONFIG from the EXPORT_POINTER_COLUMN for the list store */
	gtk_tree_model_get_iter_from_string(GTK_TREE_MODEL(export_table_list_store), &iter, path);
	gtk_tree_model_get(GTK_TREE_MODEL(export_table_list_store), &iter, EXPORT_POINTER_COLUMN, &gptr, -1);
	dumper_table_config = (SHPDUMPERCONFIG *)gptr;
	
	/* Free the configuration from memory */
	free_dumper_config(dumper_table_config);
	
	/* Remove the row from the list */
	gtk_list_store_remove(export_table_list_store, &iter);
}

static void
pgui_action_import(GtkWidget *widget, gpointer data)
{	
	SHPLOADERCONFIG *loader_file_config;
	SHPLOADERSTATE *state;
	gint is_valid;
	gpointer gptr;
	GtkTreeIter iter;
	char *sql_form, *query, *connection_string, *progress_shapefile = NULL;
  char progress_text[GUIMSG_LINE_MAXLEN+1];
	PGresult *result;
	
	int ret, i = 0;
	char *header, *footer, *record;
	
	/* Get the first row of the import list */
	is_valid = gtk_tree_model_get_iter_first(GTK_TREE_MODEL(import_file_list_store), &iter);
	if (!is_valid)
	{
		pgui_seterr(_("ERROR: You haven't specified any files to import"));
		pgui_raise_error_dialogue();

		return;
	}

	/* Firstly make sure that we can connect to the database - if we can't then there isn't much
	   point doing anything else... */
	if (!connection_test())
	{
		pgui_seterr(_("Unable to connect to the database - please check your connection settings"));
		pgui_raise_error_dialogue();

		/* Open the connections UI for the user */
		update_conn_ui_from_conn_config();

		gtk_widget_show_all(GTK_WIDGET(window_conn));
		return;
	}

	/* Let's open a single connection to the remote DB for the duration of the validation pass;
	   note that we already know the connection string works, otherwise we would have bailed
	   out earlier in the function */
	connection_string = ShpDumperGetConnectionStringFromConn(conn);
	pg_connection = PQconnectdb(connection_string);
	
	/* Setup the table/column type discovery query */
	sql_form = "SELECT a.attnum, a.attname AS field, t.typname AS type, a.attlen AS length, a.atttypmod AS precision FROM pg_class c, pg_attribute a, pg_type t, pg_namespace n WHERE c.relname = '%s' AND n.nspname = '%s' AND a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid AND c.relnamespace = n.oid ORDER BY a.attnum";
	
	/* Validation: we loop through each of the files in order to validate them as a separate pass */
	while (is_valid)
	{
		/* Grab the SHPLOADERCONFIG for this row */
		gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), &iter, IMPORT_POINTER_COLUMN, &gptr, -1);
		loader_file_config = (SHPLOADERCONFIG *)gptr;
		
		/* For each entry, we execute a remote query in order to determine the column names
		   and types for the remote table if they actually exist */
		query = malloc(strlen(sql_form) + strlen(loader_file_config->schema) + strlen(loader_file_config->table) + 1);
		sprintf(query, sql_form, loader_file_config->table, loader_file_config->schema);
		result = PQexec(pg_connection, query);
		
		/* Call the validation function with the SHPLOADERCONFIG and the result set */
		ret = validate_remote_loader_columns(loader_file_config, result);
		if (ret == SHPLOADERERR)
		{
			pgui_raise_error_dialogue();
			
			PQclear(result);
			free(query);
			
			return;
		}	
		
		/* Free the SQL query */
		PQclear(result);
		free(query);

		/* Get next entry */
		is_valid = gtk_tree_model_iter_next(GTK_TREE_MODEL(import_file_list_store), &iter);
	}
	
	/* Close our database connection */
	PQfinish(pg_connection);	

	
	/* Once we've done the validation pass, now let's load the shapefile */
	is_valid = gtk_tree_model_get_iter_first(GTK_TREE_MODEL(import_file_list_store), &iter);
	while (is_valid)
	{
		/* Grab the SHPLOADERCONFIG for this row */
		gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), &iter, IMPORT_POINTER_COLUMN, &gptr, -1);
		loader_file_config = (SHPLOADERCONFIG *)gptr;
	
		pgui_logf("\n==============================");
		pgui_logf("Importing with configuration: %s, %s, %s, %s, mode=%c, dump=%d, simple=%d, geography=%d, index=%d, shape=%d, srid=%d", loader_file_config->table, loader_file_config->schema, loader_file_config->geo_col, loader_file_config->shp_file, loader_file_config->opt, loader_file_config->dump_format, loader_file_config->simple_geometries, loader_file_config->geography, loader_file_config->createindex, loader_file_config->readshape, loader_file_config->sr_id);
		
		/*
		 * Loop through the items in the shapefile
		 */
		is_running = TRUE;
		
		/* One connection per file, otherwise error handling becomes tricky... */
		pg_connection = PQconnectdb(connection_string);

		/* Disable the button to prevent multiple imports running at the same time */
		gtk_widget_set_sensitive(widget, FALSE);

		/* Allow GTK events to get a look in */
		while (gtk_events_pending())
			gtk_main_iteration();
				
		/* Create the shapefile state object */
		state = ShpLoaderCreate(loader_file_config);

		/* Open the shapefile */
		ret = ShpLoaderOpenShape(state);
		if (ret != SHPLOADEROK)
		{
			pgui_logf("%s", state->message);

			if (ret == SHPLOADERERR)
				goto import_cleanup;
		}

		/* For progress display, only show the "core" filename */
		for (i = strlen(loader_file_config->shp_file); i >= 0 
			&& loader_file_config->shp_file[i - 1] != '\\' && loader_file_config->shp_file[i - 1] != '/'; i--);

		progress_shapefile = malloc(strlen(loader_file_config->shp_file));
		strcpy(progress_shapefile, &loader_file_config->shp_file[i]);
		
		/* Display the progress dialog */
		/* lw_asprintf(&progress_text, _("Importing shapefile %s (%d records)..."), progress_shapefile, ShpLoaderGetRecordCount(state)); */
		snprintf(progress_text, GUIMSG_LINE_MAXLEN, _("Importing shapefile %s (%d records)..."), progress_shapefile, ShpLoaderGetRecordCount(state));
		progress_text[GUIMSG_LINE_MAXLEN] = '\0';
		gtk_label_set_text(GTK_LABEL(label_progress), progress_text);
		gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(progress), 0.0);
		gtk_widget_show_all(dialog_progress);

		/* If reading the whole shapefile, display its type */
		if (state->config->readshape)
		{
			pgui_logf("Shapefile type: %s", SHPTypeName(state->shpfiletype));
			pgui_logf("PostGIS type: %s[%d]", state->pgtype, state->pgdims);
		}

		/* Get the header */
		ret = ShpLoaderGetSQLHeader(state, &header);
		if (ret != SHPLOADEROK)
		{
			pgui_logf("%s", state->message);

			if (ret == SHPLOADERERR)
				goto import_cleanup;
		}

		/* Send the header to the remote server: if we are in COPY mode then the last
		   statement will be a COPY and so will change connection mode */
		ret = pgui_exec(header);
		free(header);

		if (!ret)
			goto import_cleanup;

		/* If we are in prepare mode, we need to skip the actual load. */
		if (state->config->opt != 'p')
		{
            int numrecords = ShpLoaderGetRecordCount(state);
            int records_per_tick = (numrecords / 200) - 1;
            
            if ( records_per_tick < 1 ) 
                records_per_tick = 1;
		    
			/* If we are in COPY (dump format) mode, output the COPY statement and enter COPY mode */
			if (state->config->dump_format)
			{
				ret = ShpLoaderGetSQLCopyStatement(state, &header);

				if (ret != SHPLOADEROK)
				{
					pgui_logf("%s", state->message);

					if (ret == SHPLOADERERR)
						goto import_cleanup;
				}

				/* Send the result to the remote server: this should put us in COPY mode */
				ret = pgui_copy_start(header);
				free(header);

				if (!ret)
					goto import_cleanup;
			}

			/* Main loop: iterate through all of the records and send them to stdout */
			for (i = 0; i < numrecords && is_running; i++)
			{
				ret = ShpLoaderGenerateSQLRowStatement(state, i, &record);

				switch (ret)
				{
				case SHPLOADEROK:
					/* Simply send the statement */
					if (state->config->dump_format)
						ret = pgui_copy_write(record);
					else
						ret = pgui_exec(record);

					/* Display a record number if we failed */
					if (!ret)
						pgui_logf(_("Import failed on record number %d"), i);

					free(record);
					break;

				case SHPLOADERERR:
					/* Display the error message then stop */
					pgui_logf("%s\n", state->message);
					goto import_cleanup;
					break;

				case SHPLOADERWARN:
					/* Display the warning, but continue */
					pgui_logf("%s\n", state->message);

					if (state->config->dump_format)
						ret = pgui_copy_write(record);
					else
						ret = pgui_exec(record);

					/* Display a record number if we failed */
					if (!ret)
						pgui_logf(_("Import failed on record number %d"), i);

					free(record);
					break;

				case SHPLOADERRECDELETED:
					/* Record is marked as deleted - ignore */
					break;

				case SHPLOADERRECISNULL:
					/* Record is NULL and should be ignored according to NULL policy */
					break;
				}

				/* Update the progress bar */
				if ( i % records_per_tick == 0 )
				    gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(progress), (float)i / numrecords);

				/* Allow GTK events to get a look in */
				while (gtk_events_pending())
					gtk_main_iteration();
			}

			/* If we are in COPY (dump format) mode, leave COPY mode */
			if (state->config->dump_format)
			{
				if (! pgui_copy_end(0) )
					goto import_cleanup;

				result = PQgetResult(pg_connection);
				if (PQresultStatus(result) != PGRES_COMMAND_OK)
				{
					pgui_logf(_("COPY failed with the following error: %s"), PQerrorMessage(pg_connection));
					ret = SHPLOADERERR;
					goto import_cleanup;
				}
			}
		} /* if (state->config->opt != 'p') */

		/* Only continue if we didn't abort part way through */
		if (is_running)
		{
			/* Get the footer */
			ret = ShpLoaderGetSQLFooter(state, &footer);
			if (ret != SHPLOADEROK)
			{
				pgui_logf("%s\n", state->message);

				if (ret == SHPLOADERERR)
					goto import_cleanup;
			}

			/* Just in case index creation takes a long time, update the progress text */
			if (state->config->createindex)
			{
				gtk_label_set_text(GTK_LABEL(label_progress), _("Creating spatial index..."));
				
				/* Allow GTK events to get a look in */
				while (gtk_events_pending())
					gtk_main_iteration();
			}

			/* Send the footer to the server */
			ret = pgui_exec(footer);
			free(footer);

			if (!ret)
				goto import_cleanup;
		}
		
import_cleanup:
		/* Import has definitely stopped running */
		is_running = FALSE;

		/* Close the existing connection */
		PQfinish(pg_connection);
		pg_connection = NULL;
		
		/* If we didn't finish inserting all of the items (and we expected to), an error occurred */
		if ((state->config->opt != 'p' && i != ShpLoaderGetRecordCount(state)) || !ret)
			pgui_logf(_("Shapefile import failed."));
		else
			pgui_logf(_("Shapefile import completed."));

		/* Free the state object */
		ShpLoaderDestroy(state);

		/* Tidy up */
		if (progress_shapefile)
			free(progress_shapefile);
		
		/* Get next entry */
		is_valid = gtk_tree_model_iter_next(GTK_TREE_MODEL(import_file_list_store), &iter);
	}
	
	/* Import has definitely finished */
	is_running = FALSE;

	/* Enable the button once again */
	gtk_widget_set_sensitive(widget, TRUE);

	/* Silly GTK bug means we have to hide and show the button for it to work again! */
	gtk_widget_hide(widget);
	gtk_widget_show(widget);

	/* Hide the progress dialog */
	gtk_widget_hide(dialog_progress);
		
	/* Allow GTK events to get a look in */
	while (gtk_events_pending())
		gtk_main_iteration();

	/* Tidy up */
	free(connection_string);

	return;
}

static void
pgui_action_export(GtkWidget *widget, gpointer data)
{	
	SHPDUMPERCONFIG *dumper_table_config;
	SHPDUMPERSTATE *state;
	gint is_valid;
	gpointer gptr;
	GtkTreeIter iter;
	char *output_shapefile, *orig_shapefile;
  char progress_text[GUIMSG_LINE_MAXLEN+1];
	gchar *folder_path;
	
	int ret, success = FALSE, i = 0;
	
	/* Get the first row of the import list */
	is_valid = gtk_tree_model_get_iter_first(GTK_TREE_MODEL(export_table_list_store), &iter);
	if (!is_valid)
	{
		pgui_seterr(_("ERROR: You haven't specified any tables to export"));
		pgui_raise_error_dialogue();

		return;
	}

	/* Firstly make sure that we can connect to the database - if we can't then there isn't much
	   point doing anything else... */
	if (!connection_test())
	{
		pgui_seterr(_("Unable to connect to the database - please check your connection settings"));
		pgui_raise_error_dialogue();

		/* Open the connections UI for the user */
		update_conn_ui_from_conn_config();

		gtk_widget_show_all(GTK_WIDGET(window_conn));
		return;
	}

	/* Now open the file selector dialog so the user can specify where they would like the output
	   files to reside */
	if (gtk_dialog_run(GTK_DIALOG(dialog_folderchooser)) != GTK_RESPONSE_ACCEPT)
	{
		gtk_widget_hide(dialog_folderchooser);
		
		return;
	}
	
	gtk_widget_hide(dialog_folderchooser);
	folder_path = gtk_file_chooser_get_current_folder(GTK_FILE_CHOOSER(dialog_folderchooser));
				
	/* Now everything is set up, let's extract the tables */
	is_valid = gtk_tree_model_get_iter_first(GTK_TREE_MODEL(export_table_list_store), &iter);
	while (is_valid)
	{
		/* Grab the SHPDUMPERCONFIG for this row */
		gtk_tree_model_get(GTK_TREE_MODEL(export_table_list_store), &iter, EXPORT_POINTER_COLUMN, &gptr, -1);
		dumper_table_config = (SHPDUMPERCONFIG *)gptr;
	
		pgui_logf("\n==============================");
		pgui_logf("Exporting with configuration: %s, %s, %s", dumper_table_config->table, dumper_table_config->schema, dumper_table_config->shp_file);
		
		/* Export is running */
		is_running = TRUE;
		success = FALSE;
		
		/* Disable the button to prevent multiple imports running at the same time */
		gtk_widget_set_sensitive(widget, FALSE);

		/* Allow GTK events to get a look in */
		while (gtk_events_pending())
			gtk_main_iteration();
	
		/* Create the state for each configuration */
		state = ShpDumperCreate(dumper_table_config);
		state->config->conn = conn;
		
		/* Save the original shapefile name, then create a temporary version containing the full path */
		orig_shapefile = dumper_table_config->shp_file;
		output_shapefile = malloc(strlen(folder_path) + strlen(dumper_table_config->shp_file) + 2);
		strcpy(output_shapefile, folder_path);
		strcat(output_shapefile, G_DIR_SEPARATOR_S);
		strcat(output_shapefile, dumper_table_config->shp_file);

		dumper_table_config->shp_file = output_shapefile;

		/* Connect to the database */
		ret = ShpDumperConnectDatabase(state);
		if (ret != SHPDUMPEROK)
		{
			pgui_seterr("%s", state->message);
			pgui_raise_error_dialogue();
			
			goto export_cleanup;
		}
	
		/* Display the progress dialog */
		gtk_label_set_text(GTK_LABEL(label_progress), _("Initialising..."));
		gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(progress), 0.0);
		gtk_widget_show_all(dialog_progress);
		
		ret = ShpDumperOpenTable(state);
		if (ret != SHPDUMPEROK)
		{
			pgui_logf("%s", state->message);

			if (ret == SHPDUMPERERR)
			{
				gtk_widget_hide(dialog_progress);
				
				pgui_seterr("%s", state->message);
				pgui_raise_error_dialogue();
				
				goto export_cleanup;
			}
		}

		/* Update the text */
		/* lw_asprintf(&progress_text, _("Exporting table %s (%d records)..."), dumper_table_config->table, ShpDumperGetRecordCount(state)); */
		snprintf(progress_text, GUIMSG_LINE_MAXLEN, _("Exporting table %s (%d records)..."), dumper_table_config->table, ShpDumperGetRecordCount(state));
		progress_text[GUIMSG_LINE_MAXLEN] = '\0';
		gtk_label_set_text(GTK_LABEL(label_progress), progress_text);

		/* Allow GTK events to get a look in */
		while (gtk_events_pending())
			gtk_main_iteration();

		pgui_logf(_("Done (postgis major version: %d)"), state->pgis_major_version);
		pgui_logf(_("Output shape: %s"), shapetypename(state->outshptype));
		
		for (i = 0; i < ShpDumperGetRecordCount(state) && is_running == TRUE; i++)
		{
			ret = ShpLoaderGenerateShapeRow(state);
			if (ret != SHPDUMPEROK)
			{
				pgui_logf("%s", state->message);
	
				if (ret == SHPDUMPERERR)
				{
					gtk_widget_hide(dialog_progress);
					
					pgui_seterr("%s", state->message);
					pgui_raise_error_dialogue();
					
					goto export_cleanup;
				}
			}
			
			/* Update the progress bar */
			gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(progress), (float)i / ShpDumperGetRecordCount(state));

			/* Allow GTK events to get a look in */
			while (gtk_events_pending())
				gtk_main_iteration();	
		}

		/* Finish the dump */
		ret = ShpDumperCloseTable(state);
		if (ret != SHPDUMPEROK)
		{
			pgui_logf("%s", state->message);
	
			if (ret == SHPDUMPERERR)
			{
				gtk_widget_hide(dialog_progress);
				
				pgui_seterr("%s", state->message);
				pgui_raise_error_dialogue();
			}
		}

		/* Indicate success */
		if (is_running)
			success = TRUE;
		
export_cleanup:

		/* Tidy up everything */
		ShpDumperDestroy(state);

		/* Reset shapefile back to original form (without full path) */
		dumper_table_config->shp_file = orig_shapefile;
		
		/* Get next entry */
		is_valid = gtk_tree_model_iter_next(GTK_TREE_MODEL(export_table_list_store), &iter);
	}
	
	/* Export has definitely finished */
	is_running = FALSE;
	if (!success)
		pgui_logf(_("Table export failed."));
	else
		pgui_logf(_("Table export completed."));
	
	/* Enable the button once again */
	gtk_widget_set_sensitive(widget, TRUE);

	/* Silly GTK bug means we have to hide and show the button for it to work again! */
	gtk_widget_hide(widget);
	gtk_widget_show(widget);

	/* Hide the progress dialog */
	gtk_widget_hide(dialog_progress);
		
	/* Allow GTK events to get a look in */
	while (gtk_events_pending())
		gtk_main_iteration();
	
	return;
}


/* === Import ListView functions and signal handlers === */

/* Creates a single file row in the list table given the URI of a file */
static void
process_single_uri(char *uri)
{
	SHPLOADERCONFIG *loader_file_config;
	char *filename = NULL;
	char *hostname;
	GError *error = NULL;

	if (uri == NULL)
	{
		pgui_logf(_("Unable to process drag URI."));
		return;
	}

	filename = g_filename_from_uri(uri, &hostname, &error);
	g_free(uri);

	if (filename == NULL)
	{
		pgui_logf(_("Unable to process filename: %s\n"), error->message);
		g_error_free(error);
		return;
	}

	/* Create a new row in the listview */
	loader_file_config = create_new_file_config(filename);
	add_loader_file_config_to_list(loader_file_config);	

	g_free(filename);
	g_free(hostname);

}

/* Update the SHPLOADERCONFIG to the values currently contained within the iter  */
static void
update_loader_file_config_from_listview_iter(GtkTreeIter *iter, SHPLOADERCONFIG *loader_file_config)
{
	gchar *schema, *table, *geo_col, *srid;
	
	/* Grab the main values for this file */
	gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), iter,
		IMPORT_SCHEMA_COLUMN, &schema,
		IMPORT_TABLE_COLUMN, &table,
		IMPORT_GEOMETRY_COLUMN, &geo_col,
		IMPORT_SRID_COLUMN, &srid,
		-1);
	
	/* Update the schema */
	if (loader_file_config->schema)
		free(loader_file_config->schema);
	
	loader_file_config->schema = strdup(schema);

	/* Update the table */
	if (loader_file_config->table)
		free(loader_file_config->table);
		
	loader_file_config->table = strdup(table);

	/* Update the geo column */
	if (loader_file_config->geo_col)
		free(loader_file_config->geo_col);
		
	loader_file_config->geo_col = strdup(geo_col);
	
	/* Update the SRID */
	loader_file_config->sr_id = atoi(srid);

	/* Free the values */
	return;
}


/*
 * Here lives the magic of the drag-n-drop of the app.  We really don't care
 * about much of the provided tidbits.  We only actually user selection_data
 * and extract a list of filenames from it.
 */
static void
pgui_action_handle_file_drop(GtkWidget *widget,
                             GdkDragContext *dc,
                             gint x, gint y,
                             GtkSelectionData *selection_data,
                             guint info, guint t, gpointer data)
{
	const gchar *p, *q;

	if (selection_data->data == NULL)
	{
		pgui_logf(_("Unable to process drag data."));
		return;
	}

	p = (char*)selection_data->data;
	while (p)
	{
		/* Only process non-comments */
		if (*p != '#')
		{
			/* Trim leading whitespace */
			while (g_ascii_isspace(*p))
				p++;
			q = p;
			/* Scan to the end of the string (null or newline) */
			while (*q && (*q != '\n') && (*q != '\r'))
				q++;
			if (q > p)
			{
				/* Ignore terminating character */
				q--;
				/* Trim trailing whitespace */
				while (q > p && g_ascii_isspace(*q))
					q--;
				if (q > p)
				{
					process_single_uri(g_strndup(p, q - p + 1));
				}
			}
		}
		/* Skip to the next entry */
		p = strchr(p, '\n');
		if (p)
			p++;
	}
}


/*
 * This function is a signal handler for the load mode combo boxes.
 */
static void
pgui_action_handle_tree_combo(GtkCellRendererCombo *combo,
                              gchar *path_string,
                              GtkTreeIter *new_iter,
                              gpointer user_data)
{
	GtkTreeIter iter;
	SHPLOADERCONFIG *loader_file_config;
	char opt;
	gchar *combo_text;
	gpointer gptr;
	
	/* Grab the SHPLOADERCONFIG from the IMPORT_POINTER_COLUMN for the list store */
	gtk_tree_model_get_iter_from_string(GTK_TREE_MODEL(import_file_list_store), &iter, path_string);
	gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), &iter, IMPORT_POINTER_COLUMN, &gptr, -1);
	loader_file_config = (SHPLOADERCONFIG *)gptr;

	/* Now grab the row selected within the combo box */
	gtk_tree_model_get(GTK_TREE_MODEL(loader_mode_combo_list), new_iter, LOADER_MODE_COMBO_OPTION_CHAR, &opt, -1);
	
	/* Update the configuration */
	
	/* Hack for index creation: we must disable it if we are appending, otherwise we
	   end up trying to generate the index again */
	loader_file_config->createindex = global_loader_config->createindex;
	
	switch (opt)
	{
		case 'a':
			loader_file_config->opt = 'a';
			
			/* Other half of index creation hack */
			loader_file_config->createindex = 0;
				
			break;
			
		case 'd':
			loader_file_config->opt = 'd';
			break;
			
		case 'p':
			loader_file_config->opt = 'p';
			break;
			
		case 'c':
			loader_file_config->opt = 'c';
			break;
	}
	
	/* Update the selection in the listview with the text from the combo */
	gtk_tree_model_get(GTK_TREE_MODEL(loader_mode_combo_list), new_iter, LOADER_MODE_COMBO_TEXT, &combo_text, -1); 
	gtk_list_store_set(import_file_list_store, &iter, IMPORT_MODE_COLUMN, combo_text, -1);
	
	return;	
}
	

/*
 * This method is a signal listener for all text renderers in the file
 * list table, including the empty ones.  Edits of the empty table are
 * passed to an appropriate function, while edits of existing file rows
 * are applied and the various validations called.
 */
static void
pgui_action_handle_loader_edit(GtkCellRendererText *renderer,
                             gchar *path,
                             gchar *new_text,
                             gpointer column)
{
	GtkTreeIter iter;
	gpointer gptr;
	gint columnindex;
	SHPLOADERCONFIG *loader_file_config;
#define MAXLEN 16
	char srid[MAXLEN+1];
	
	/* Empty doesn't fly */
	if (strlen(new_text) == 0)
		return;
	
	/* Update the model with the current edit change */
	columnindex = *(gint *)column;
	gtk_tree_model_get_iter_from_string(GTK_TREE_MODEL(import_file_list_store), &iter, path);
	gtk_list_store_set(import_file_list_store, &iter, columnindex, new_text, -1);
	
	/* Grab the SHPLOADERCONFIG from the IMPORT_POINTER_COLUMN for the list store */
	gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), &iter, IMPORT_POINTER_COLUMN, &gptr, -1);
	loader_file_config = (SHPLOADERCONFIG *)gptr;
	
	/* Update the configuration from the current UI data */
	update_loader_file_config_from_listview_iter(&iter, loader_file_config);
	
	/* Now refresh the listview UI row with the new configuration */
	if ( MAXLEN+1 <= snprintf(srid, MAXLEN+1, "%d", loader_file_config->sr_id) )
	{
		pgui_logf("Invalid SRID requiring more than %d digits: %d", MAXLEN, loader_file_config->sr_id);
		pgui_raise_error_dialogue();
		srid[MAXLEN] = '\0';
	}
	
	gtk_list_store_set(import_file_list_store, &iter,
	                   IMPORT_SCHEMA_COLUMN, loader_file_config->schema,
	                   IMPORT_TABLE_COLUMN, loader_file_config->table,
	                   IMPORT_GEOMETRY_COLUMN, loader_file_config->geo_col,
	                   IMPORT_SRID_COLUMN, srid,
	                   -1);

	return;
}

/*
 * Signal handler for the remove box.  Performs no user interaction, simply
 * removes the row from the table.
 */
static void
pgui_action_handle_file_remove(GtkCellRendererToggle *renderer,
                               gchar *path,
                               gpointer user_data)
{
	GtkTreeIter iter;
	SHPLOADERCONFIG *loader_file_config;
	gpointer gptr;
	
	/* Grab the SHPLOADERCONFIG from the IMPORT_POINTER_COLUMN for the list store */
	gtk_tree_model_get_iter_from_string(GTK_TREE_MODEL(import_file_list_store), &iter, path);
	gtk_tree_model_get(GTK_TREE_MODEL(import_file_list_store), &iter, IMPORT_POINTER_COLUMN, &gptr, -1);
	loader_file_config = (SHPLOADERCONFIG *)gptr;
	
	/* Free the configuration from memory */
	free_loader_config(loader_file_config);
	
	/* Remove the row from the list */
	gtk_list_store_remove(import_file_list_store, &iter);

	/* Update the filename field width */
	update_filename_field_width();
}


/* === Export ListView functions and signal handlers === */

/* Update the SHPDUMPERCONFIG to the values currently contained within the iter  */
static void
update_dumper_table_config_from_listview_iter(GtkTreeIter *iter, SHPDUMPERCONFIG *dumper_table_config)
{
	gchar *schema, *table, *geo_col, *filename;
	
	/* Grab the main values for this file */
	gtk_tree_model_get(GTK_TREE_MODEL(export_table_list_store), iter,
		EXPORT_SCHEMA_COLUMN, &schema,
		EXPORT_TABLE_COLUMN, &table,
		EXPORT_GEOMETRY_COLUMN, &geo_col,
		EXPORT_FILENAME_COLUMN, &filename,
		-1);
	
	/* Update the schema */
	if (dumper_table_config->schema)
		free(dumper_table_config->schema);
		
	dumper_table_config->schema = strdup(schema);
	
	/* Update the table */
	if (dumper_table_config->table)
		free(dumper_table_config->table);
		
	dumper_table_config->table = strdup(table);
	
	/* Update the geometry column */
	if (dumper_table_config->geo_col_name)
		free(dumper_table_config->geo_col_name);
		
	dumper_table_config->geo_col_name = strdup(geo_col);

	/* Update the filename column (default to table name) */
	if (dumper_table_config->shp_file)
		free(dumper_table_config->shp_file);
		
	dumper_table_config->shp_file = strdup(filename);

	return;
}

static void
pgui_action_handle_table_geocol_combo(GtkCellRendererCombo *combo,
                              gchar *path_string,
                              GtkTreeIter *new_iter,
                              gpointer user_data)
{
	SHPDUMPERCONFIG *dumper_table_config;
	gchar *geocol_name;
	GtkTreeIter iter;
	GtkListStore *model;
	gpointer gptr;
	
	/* Get the existing geo column name */	
	gtk_tree_model_get_iter_from_string(GTK_TREE_MODEL(export_table_list_store), &iter, path_string);
	gtk_tree_model_get(GTK_TREE_MODEL(export_table_list_store), &iter,
			   EXPORT_POINTER_COLUMN, &gptr,
			   EXPORT_GEOMETRY_COLUMN, &geocol_name,
			   EXPORT_GEOMETRY_LISTSTORE_COLUMN, &model,
			   -1);
	
	/* If the geocol_name is NULL then there was no geo column so exit */
	if (!geocol_name)
		return;
	
	/* Otherwise update the geo column name in the config and the model */
	gtk_tree_model_get(GTK_TREE_MODEL(model), new_iter, TABLECHOOSER_GEOCOL_COMBO_TEXT, &geocol_name, -1);
	dumper_table_config = (SHPDUMPERCONFIG *)gptr;
	
	if (dumper_table_config->geo_col_name)
	{
		free(dumper_table_config->geo_col_name);
		
		dumper_table_config->geo_col_name = strdup(geocol_name);
	}
	
	gtk_list_store_set(export_table_list_store, &iter, 
			   EXPORT_GEOMETRY_COLUMN, geocol_name,
			   -1);
	
	return;
}

static void
pgui_action_handle_dumper_edit(GtkCellRendererText *renderer,
                             gchar *path,
                             gchar *new_text,
                             gpointer column)
{
	GtkTreeIter iter;
	gpointer gptr;
	gint columnindex;
	SHPDUMPERCONFIG *dumper_table_config;
	
	/* Empty doesn't fly */
	if (strlen(new_text) == 0)
		return;
	
	/* Update the model with the current edit change */
	columnindex = *(gint *)column;
	gtk_tree_model_get_iter_from_string(GTK_TREE_MODEL(export_table_list_store), &iter, path);
	gtk_list_store_set(export_table_list_store, &iter, columnindex, new_text, -1);
	
	/* Grab the SHPDUMPERCONFIG from the EXPORT_POINTER_COLUMN for the list store */
	gtk_tree_model_get(GTK_TREE_MODEL(export_table_list_store), &iter, EXPORT_POINTER_COLUMN, &gptr, -1);
	dumper_table_config = (SHPDUMPERCONFIG *)gptr;
	
	/* Update the configuration from the current UI data */
	update_dumper_table_config_from_listview_iter(&iter, dumper_table_config);
	
	/* Now refresh the listview UI row with the new configuration */
	gtk_list_store_set(export_table_list_store, &iter,
	                   EXPORT_SCHEMA_COLUMN, dumper_table_config->schema,
	                   EXPORT_TABLE_COLUMN, dumper_table_config->table,
	                   EXPORT_GEOMETRY_COLUMN, dumper_table_config->geo_col_name,
	                   EXPORT_FILENAME_COLUMN, dumper_table_config->shp_file,
	                   -1);

	return;
}

/* === Connection Window functions === */

/* Set the connection details UI from the current configuration */
static void 
update_conn_ui_from_conn_config(void)
{
	if (conn->username)
		gtk_entry_set_text(GTK_ENTRY(entry_pg_user), conn->username);
	else
		gtk_entry_set_text(GTK_ENTRY(entry_pg_user), "");
		
	if (conn->password)
		gtk_entry_set_text(GTK_ENTRY(entry_pg_pass), conn->password);
	else
		gtk_entry_set_text(GTK_ENTRY(entry_pg_pass), "");
	
	if (conn->host)
		gtk_entry_set_text(GTK_ENTRY(entry_pg_host), conn->host);
	else
		gtk_entry_set_text(GTK_ENTRY(entry_pg_host), "");
	
	if (conn->port)
		gtk_entry_set_text(GTK_ENTRY(entry_pg_port), conn->port);
	else
		gtk_entry_set_text(GTK_ENTRY(entry_pg_port), "");
	
	if (conn->database)
		gtk_entry_set_text(GTK_ENTRY(entry_pg_db), conn->database);
	else
		gtk_entry_set_text(GTK_ENTRY(entry_pg_db), "");

	return;
}

/* Set the current connection configuration from the connection details UI */
static void
update_conn_config_from_conn_ui(void)
{
	const char *text;
	
	text = gtk_entry_get_text(GTK_ENTRY(entry_pg_user));
	if (conn->username)
		free(conn->username);
	
	if (strlen(text))
		conn->username = strdup(text);
	else
		conn->username = NULL;
	
	text = gtk_entry_get_text(GTK_ENTRY(entry_pg_pass));
	if (conn->password)
		free(conn->password);
	
	if (strlen(text))
		conn->password = strdup(text);
	else
		conn->password = NULL;
	
	text = gtk_entry_get_text(GTK_ENTRY(entry_pg_host));
	if (conn->host)
		free(conn->host);
	
	if (strlen(text))
		conn->host = strdup(text);
	else
		conn->host = NULL;

	text = gtk_entry_get_text(GTK_ENTRY(entry_pg_port));
	if (conn->port)
		free(conn->port);
	
	if (strlen(text))
		conn->port = strdup(text);
	else
		conn->port = NULL;

	text = gtk_entry_get_text(GTK_ENTRY(entry_pg_db));
	if (conn->database)
		free(conn->database);
	
	if (strlen(text))
		conn->database = strdup(text);
	else
		conn->database = NULL;

	return;
}

/*
 * Open the connection details dialog
 */
static void
pgui_action_connection_details(GtkWidget *widget, gpointer data)
{
	/* Update the UI with the current options */
	update_conn_ui_from_conn_config();
	
	gtk_widget_show_all(GTK_WIDGET(window_conn));
	return;
}

/* Validate the connection, returning true or false */
static int
pgui_validate_connection()
{
	int i;
	
	if (conn->port && strlen(conn->port))
	{
		for (i = 0; i < strlen(conn->port); i++)
		{
			if (!isdigit(conn->port[i]))
			{
				pgui_seterr(_("The connection port must be numeric!"));
				return 0;
			}
		}
	}
	
	return 1;
}

static void
pgui_sanitize_connection_string(char *connection_string)
{
	char *ptr = strstr(connection_string, "password");
	if ( ptr )
	{
		ptr += 10;
		while ( *ptr != '\'' && *ptr != '\0' )
		{
			/* If we find a \, hide both it and the next character */
			if ( *ptr == '\\' )
				*ptr++ = '*';
		
			*ptr++ = '*';
		}
	}
	return;
}

/*
 * We retain the ability to explicitly request a test of the connection
 * parameters.  This is the button signal handler to do so.
 */
static void
pgui_action_connection_okay(GtkWidget *widget, gpointer data)
{
	/* Update the configuration structure from the form */
	update_conn_config_from_conn_ui();
	
	/* Make sure have a valid connection first */
	if (!pgui_validate_connection())
	{
		pgui_raise_error_dialogue();
		return;
	}
	
	if (!connection_test())
	{
		pgui_logf(_("Connection failed."));
	
		/* If the connection failed, display a warning before closing */
		pgui_seterr(_("Unable to connect to the database - please check your connection settings"));
		pgui_raise_error_dialogue();
	}
	else
	{
		pgui_logf(_("Connection succeeded."));
	}
	
			
	/* Hide the window after the test */
	gtk_widget_hide(GTK_WIDGET(window_conn));
}


/* === Window creation functions === */

static void
pgui_create_about_dialog(void)
{
	const char *authors[] =
	{
		"Paul Ramsey <pramsey@opengeo.org>",
		"Mark Cave-Ayland <mark.cave-ayland@ilande.co.uk>",
		"Mark Leslie <mark.leslie@lisasoft.com>",
		NULL
	};
	
	

	dialog_about = gtk_about_dialog_new();
	gtk_about_dialog_set_name(GTK_ABOUT_DIALOG(dialog_about), _("PostGIS Shapefile Import/Export Manager"));
	gtk_about_dialog_set_comments(GTK_ABOUT_DIALOG(dialog_about), POSTGIS_LIB_VERSION);
	gtk_about_dialog_set_website(GTK_ABOUT_DIALOG(dialog_about), "http://postgis.net/");
	gtk_about_dialog_set_authors(GTK_ABOUT_DIALOG(dialog_about), authors);
}

static void
pgui_create_filechooser_dialog(void)
{
	GtkFileFilter *file_filter_shape;
	
	/* Create the dialog */
	dialog_filechooser = gtk_file_chooser_dialog_new( _("Select a Shape File"), GTK_WINDOW (window_main), 
		GTK_FILE_CHOOSER_ACTION_OPEN, GTK_STOCK_CANCEL, GTK_RESPONSE_CLOSE, GTK_STOCK_OPEN, GTK_RESPONSE_ACCEPT, NULL);

	/* Filter for .shp files */
	file_filter_shape = gtk_file_filter_new();
	gtk_file_filter_add_pattern(GTK_FILE_FILTER(file_filter_shape), "*.shp");
	gtk_file_filter_set_name(GTK_FILE_FILTER(file_filter_shape), _("Shape Files (*.shp)"));
	gtk_file_chooser_add_filter(GTK_FILE_CHOOSER(dialog_filechooser), file_filter_shape);

	/* Filter for .dbf files */
	file_filter_shape = gtk_file_filter_new();
	gtk_file_filter_add_pattern(GTK_FILE_FILTER(file_filter_shape), "*.dbf");
	gtk_file_filter_set_name(GTK_FILE_FILTER(file_filter_shape), _("DBF Files (*.dbf)"));
	gtk_file_chooser_add_filter(GTK_FILE_CHOOSER(dialog_filechooser), file_filter_shape);

	/* Allow multiple files to be selected */
	g_object_set(dialog_filechooser, "select-multiple", TRUE, NULL);
	
	return;
}

static void
pgui_create_folderchooser_dialog(void)
{
	GtkFileFilter *file_filter_shape;
	
	/* Create the dialog */
	dialog_folderchooser = gtk_file_chooser_dialog_new( _("Select an output folder"), GTK_WINDOW (window_main), 
		GTK_FILE_CHOOSER_ACTION_SELECT_FOLDER, GTK_STOCK_CANCEL, GTK_RESPONSE_CLOSE, GTK_STOCK_OPEN, GTK_RESPONSE_ACCEPT, NULL);

	/* Filter for .shp files */
	file_filter_shape = gtk_file_filter_new();
	gtk_file_filter_add_pattern(GTK_FILE_FILTER(file_filter_shape), "*.shp");
	gtk_file_filter_set_name(GTK_FILE_FILTER(file_filter_shape), _("Shape Files (*.shp)"));
	gtk_file_chooser_add_filter(GTK_FILE_CHOOSER(dialog_folderchooser), file_filter_shape);

	/* Filter for .dbf files */
	file_filter_shape = gtk_file_filter_new();
	gtk_file_filter_add_pattern(GTK_FILE_FILTER(file_filter_shape), "*.dbf");
	gtk_file_filter_set_name(GTK_FILE_FILTER(file_filter_shape), _("DBF Files (*.dbf)"));
	gtk_file_chooser_add_filter(GTK_FILE_CHOOSER(dialog_folderchooser), file_filter_shape);

	return;
}

static void
pgui_create_progress_dialog()
{
	GtkWidget *vbox_progress, *table_progress;
	
	dialog_progress = gtk_dialog_new_with_buttons(_("Working..."), GTK_WINDOW(window_main), GTK_DIALOG_DESTROY_WITH_PARENT, GTK_STOCK_CANCEL, GTK_RESPONSE_CANCEL, NULL);

	gtk_window_set_modal(GTK_WINDOW(dialog_progress), TRUE);
	gtk_window_set_keep_above(GTK_WINDOW(dialog_progress), TRUE);
	gtk_window_set_default_size(GTK_WINDOW(dialog_progress), 640, -1);

	/* Use a vbox as the base container */
	vbox_progress = gtk_dialog_get_content_area(GTK_DIALOG(dialog_progress));
	gtk_box_set_spacing(GTK_BOX(vbox_progress), 15);
	
	/* Create a table within the vbox */
	table_progress = gtk_table_new(2, 1, TRUE);
	gtk_container_set_border_width (GTK_CONTAINER (table_progress), 12);
	gtk_table_set_row_spacings(GTK_TABLE(table_progress), 5);
	gtk_table_set_col_spacings(GTK_TABLE(table_progress), 10);
	
	/* Text for the progress bar */
	label_progress = gtk_label_new("");
	gtk_table_attach_defaults(GTK_TABLE(table_progress), label_progress, 0, 1, 0, 1);
	
	/* Progress bar for the import */
	progress = gtk_progress_bar_new();
	gtk_progress_bar_set_orientation(GTK_PROGRESS_BAR(progress), GTK_PROGRESS_LEFT_TO_RIGHT);
	gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(progress), 0.0);
	gtk_table_attach_defaults(GTK_TABLE(table_progress), progress, 0, 1, 1, 2);
	
	/* Add the table to the vbox */
	gtk_box_pack_start(GTK_BOX(vbox_progress), table_progress, FALSE, FALSE, 0);
	
	/* Add signal for cancel button */
	g_signal_connect(dialog_progress, "response", G_CALLBACK(pgui_action_progress_cancel), dialog_progress);
	
	/* Make sure we catch a delete event too */
	gtk_signal_connect(GTK_OBJECT(dialog_progress), "delete_event", GTK_SIGNAL_FUNC(pgui_action_progress_delete), NULL);
	
	return;
}

static void
pgui_create_options_dialog_add_label(GtkWidget *table, const char *str, gfloat alignment, int row)
{
	GtkWidget *align = gtk_alignment_new(alignment, 0.5, 0.0, 1.0);
	GtkWidget *label = gtk_label_new(str);
	gtk_table_attach_defaults(GTK_TABLE(table), align, 1, 3, row, row + 1);
	gtk_container_add(GTK_CONTAINER (align), label);
}

static void
pgui_create_loader_options_dialog()
{
	GtkWidget *table_options;
	GtkWidget *align_options_center;
	static int text_width = 12;

	dialog_loader_options = gtk_dialog_new_with_buttons(_("Import Options"), GTK_WINDOW(window_main), GTK_DIALOG_DESTROY_WITH_PARENT, GTK_STOCK_OK, GTK_RESPONSE_OK, NULL);

	gtk_window_set_modal (GTK_WINDOW(dialog_loader_options), TRUE);
	gtk_window_set_keep_above (GTK_WINDOW(dialog_loader_options), TRUE);
	gtk_window_set_default_size (GTK_WINDOW(dialog_loader_options), 180, -1);

	table_options = gtk_table_new(7, 3, TRUE);
	gtk_container_set_border_width (GTK_CONTAINER (table_options), 12);
	gtk_table_set_row_spacings(GTK_TABLE(table_options), 5);
	gtk_table_set_col_spacings(GTK_TABLE(table_options), 10);

	pgui_create_options_dialog_add_label(table_options, _("DBF file character encoding"), 0.0, 0);
	entry_options_encoding = gtk_entry_new();
	gtk_entry_set_width_chars(GTK_ENTRY(entry_options_encoding), text_width);
	gtk_table_attach_defaults(GTK_TABLE(table_options), entry_options_encoding, 0, 1, 0, 1 );

	pgui_create_options_dialog_add_label(table_options, _("Preserve case of column names"), 0.0, 1);
	checkbutton_loader_options_preservecase = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 1, 2 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_loader_options_preservecase);

	pgui_create_options_dialog_add_label(table_options, _("Do not create 'bigint' columns"), 0.0, 2);
	checkbutton_loader_options_forceint = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 2, 3 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_loader_options_forceint);

	pgui_create_options_dialog_add_label(table_options, _("Create spatial index automatically after load"), 0.0, 3);
	checkbutton_loader_options_autoindex = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 3, 4 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_loader_options_autoindex);

	pgui_create_options_dialog_add_label(table_options, _("Load only attribute (dbf) data"), 0.0, 4);
	checkbutton_loader_options_dbfonly = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 4, 5 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_loader_options_dbfonly);

	pgui_create_options_dialog_add_label(table_options, _("Load data using COPY rather than INSERT"), 0.0, 5);
	checkbutton_loader_options_dumpformat = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 0.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 5, 6 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_loader_options_dumpformat);

	pgui_create_options_dialog_add_label(table_options, _("Load into GEOGRAPHY column"), 0.0, 6);
	checkbutton_loader_options_geography = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 6, 7 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_loader_options_geography);

	pgui_create_options_dialog_add_label(table_options, _("Generate simple geometries instead of MULTI geometries"), 0.0, 7);
	checkbutton_loader_options_simplegeoms = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 7, 8 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_loader_options_simplegeoms);
	
	/* Catch the response from the dialog */
	g_signal_connect(dialog_loader_options, "response", G_CALLBACK(pgui_action_loader_options_close), dialog_loader_options);
	gtk_box_pack_start(GTK_BOX(GTK_DIALOG(dialog_loader_options)->vbox), table_options, FALSE, FALSE, 0);
	
	/* Hook the delete event so we don't destroy the dialog (just hide) if cancelled */
	gtk_signal_connect(GTK_OBJECT(dialog_loader_options), "delete_event", GTK_SIGNAL_FUNC(pgui_event_popup_delete), NULL);
}

static void
pgui_create_dumper_options_dialog()
{
	GtkWidget *table_options;
	GtkWidget *align_options_center;

	dialog_dumper_options = gtk_dialog_new_with_buttons(_("Export Options"), GTK_WINDOW(window_main), GTK_DIALOG_DESTROY_WITH_PARENT, GTK_STOCK_OK, GTK_RESPONSE_OK, NULL);

	gtk_window_set_modal (GTK_WINDOW(dialog_dumper_options), TRUE);
	gtk_window_set_keep_above (GTK_WINDOW(dialog_dumper_options), TRUE);
	gtk_window_set_default_size (GTK_WINDOW(dialog_dumper_options), 180, -1);

	table_options = gtk_table_new(3, 3, TRUE);
	gtk_container_set_border_width (GTK_CONTAINER (table_options), 12);
	gtk_table_set_row_spacings(GTK_TABLE(table_options), 5);
	gtk_table_set_col_spacings(GTK_TABLE(table_options), 10);

	pgui_create_options_dialog_add_label(table_options, _("Include gid column in the exported table"), 0.0, 0);
	checkbutton_dumper_options_includegid = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 0, 1 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_dumper_options_includegid);
	
	pgui_create_options_dialog_add_label(table_options, _("Preserve case of column names"), 0.0, 1);
	checkbutton_dumper_options_keep_fieldname_case = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 1, 2 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_dumper_options_keep_fieldname_case);

	pgui_create_options_dialog_add_label(table_options, _("Escape column names"), 0.0, 2);
	checkbutton_dumper_options_unescapedattrs = gtk_check_button_new();
	align_options_center = gtk_alignment_new( 0.5, 0.5, 0.0, 1.0 );
	gtk_table_attach_defaults(GTK_TABLE(table_options), align_options_center, 0, 1, 2, 3 );
	gtk_container_add (GTK_CONTAINER (align_options_center), checkbutton_dumper_options_unescapedattrs);

	/* Catch the response from the dialog */
	g_signal_connect(dialog_dumper_options, "response", G_CALLBACK(pgui_action_dumper_options_close), dialog_dumper_options);
	gtk_box_pack_start(GTK_BOX(GTK_DIALOG(dialog_dumper_options)->vbox), table_options, FALSE, FALSE, 0);
	
	/* Hook the delete event so we don't destroy the dialog (just hide) if cancelled */
	gtk_signal_connect(GTK_OBJECT(dialog_dumper_options), "delete_event", GTK_SIGNAL_FUNC(pgui_event_popup_delete), NULL);
}

/*
 * This function creates the UI artefacts for the file list table and hooks
 * up all the pretty signals.
 */
static void
pgui_create_tablechooser_dialog()
{
	GtkWidget *vbox_tree, *table_progress;
	GtkWidget *sw, *label;
	GtkTreeSelection *chooser_selection;

	/* Create the main top level window with a 10px border */
	dialog_tablechooser = gtk_dialog_new_with_buttons(_("Table selection"), GTK_WINDOW(window_main),
				GTK_DIALOG_MODAL | GTK_DIALOG_DESTROY_WITH_PARENT, GTK_STOCK_OK, GTK_RESPONSE_OK, NULL);
				
	gtk_container_set_border_width(GTK_CONTAINER(dialog_tablechooser), 10);
	gtk_window_set_position(GTK_WINDOW(dialog_tablechooser), GTK_WIN_POS_CENTER);
	
	vbox_tree = gtk_dialog_get_content_area(GTK_DIALOG(dialog_tablechooser));

	/* Setup a model */
	chooser_table_list_store = gtk_list_store_new(TABLECHOOSER_N_COLUMNS,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
					 GTK_TYPE_TREE_MODEL,
					 G_TYPE_STRING,
	                                 G_TYPE_INT);
	
	/* Because we want to do selective filtering on the treeview content, we now implement a GtkTreeModel
	   filter on top of the original tree model */
	chooser_filtered_table_list_store = (GtkListStore *)gtk_tree_model_filter_new(GTK_TREE_MODEL(chooser_table_list_store), NULL);
	gtk_tree_model_filter_set_visible_func(GTK_TREE_MODEL_FILTER(chooser_filtered_table_list_store), 
					(GtkTreeModelFilterVisibleFunc)table_chooser_visibility_func, NULL, NULL);
					 
	/* Create the view and such */
	chooser_tree = gtk_tree_view_new_with_model(GTK_TREE_MODEL(chooser_filtered_table_list_store));
	chooser_selection = gtk_tree_view_get_selection(GTK_TREE_VIEW(chooser_tree));
	gtk_tree_selection_set_mode(chooser_selection, GTK_SELECTION_MULTIPLE);
	
	/* Make the tree view in a scrollable window */
	sw = gtk_scrolled_window_new(NULL, NULL);
	gtk_scrolled_window_set_policy (GTK_SCROLLED_WINDOW(sw), GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
	gtk_scrolled_window_set_shadow_type (GTK_SCROLLED_WINDOW(sw), GTK_SHADOW_ETCHED_IN);
	gtk_widget_set_size_request(sw, 320, 240);

	gtk_box_pack_start(GTK_BOX(vbox_tree), sw, FALSE, FALSE, 10);
	gtk_container_add(GTK_CONTAINER(sw), chooser_tree);
	
	/* Schema Field */
	chooser_schema_renderer = gtk_cell_renderer_text_new();
	g_object_set(chooser_schema_renderer, "editable", TRUE, NULL);
	g_signal_connect(G_OBJECT(chooser_schema_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), NULL);
	chooser_schema_column = gtk_tree_view_column_new_with_attributes(_("Schema"),
	                chooser_schema_renderer,
	                "text",
	                TABLECHOOSER_SCHEMA_COLUMN,
	                NULL);
	g_object_set(chooser_schema_column, "resizable", TRUE, "sizing", GTK_TREE_VIEW_COLUMN_AUTOSIZE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(chooser_tree), chooser_schema_column);

	/* Table Field */
	chooser_table_renderer = gtk_cell_renderer_text_new();
	g_object_set(chooser_table_renderer, "editable", FALSE, NULL);
	g_signal_connect(G_OBJECT(chooser_table_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), NULL);
	chooser_table_column = gtk_tree_view_column_new_with_attributes(_("Table"),
	               chooser_table_renderer,
	               "text",
	               TABLECHOOSER_TABLE_COLUMN,
	               NULL);
	g_object_set(chooser_table_column, "resizable", TRUE, "sizing", GTK_TREE_VIEW_COLUMN_AUTOSIZE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(chooser_tree), chooser_table_column);
	
	/* Create table to hold the tick-box and text */
	table_progress = gtk_table_new(1, 2, FALSE);
	gtk_container_set_border_width (GTK_CONTAINER (table_progress), 0);
	gtk_table_set_row_spacings(GTK_TABLE(table_progress), 0);
	gtk_table_set_col_spacings(GTK_TABLE(table_progress), 0);
	
	checkbutton_chooser_geoonly = gtk_check_button_new();
	gtk_table_attach(GTK_TABLE(table_progress), checkbutton_chooser_geoonly, 0, 1, 0, 1, GTK_SHRINK, GTK_FILL, 0, 0);
	label = gtk_label_new(_("Only show tables with geo columns"));
	gtk_table_attach(GTK_TABLE(table_progress), label, 1, 2, 0, 1, GTK_FILL, GTK_FILL, 5, 0);
	g_signal_connect(G_OBJECT(checkbutton_chooser_geoonly), "toggled", G_CALLBACK(pgui_action_chooser_toggle_show_geocolumn), NULL);
	gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(checkbutton_chooser_geoonly), TRUE);
	
	/* Attach table to the vbox */
	gtk_box_pack_start(GTK_BOX(vbox_tree), table_progress, FALSE, FALSE, 10);
		
	return;
}


/*
 * This function creates the UI artefacts for the file list table and hooks
 * up all the pretty signals.
 */
static void
pgui_create_import_file_table(GtkWidget *import_list_frame)
{
	GtkWidget *vbox_tree;
	GtkWidget *sw;
	GtkTreeIter iter;
	gint *column_indexes;
	
	gtk_container_set_border_width (GTK_CONTAINER (import_list_frame), 0);

	vbox_tree = gtk_vbox_new(FALSE, 15);
	gtk_container_set_border_width(GTK_CONTAINER(vbox_tree), 5);
	gtk_container_add(GTK_CONTAINER(import_list_frame), vbox_tree);

	/* Setup a model */
	import_file_list_store = gtk_list_store_new(IMPORT_N_COLUMNS,
					 G_TYPE_POINTER,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
	                                 G_TYPE_BOOLEAN);
	
	/* Create the view and such */
	import_tree = gtk_tree_view_new_with_model(GTK_TREE_MODEL(import_file_list_store));
	
	/* GTK has a slightly brain-dead API in that you can't directly find
	   the column being used by a GtkCellRenderer when using the same
	   callback to handle multiple fields; hence we manually store this
	   information here and pass a pointer to the column index into
	   the signal handler */
	column_indexes = g_malloc(sizeof(gint) * IMPORT_N_COLUMNS);
	
	/* Make the tree view in a scrollable window */
	sw = gtk_scrolled_window_new(NULL, NULL);
	gtk_scrolled_window_set_policy (GTK_SCROLLED_WINDOW(sw), GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
	gtk_scrolled_window_set_shadow_type (GTK_SCROLLED_WINDOW(sw), GTK_SHADOW_ETCHED_IN);
	gtk_widget_set_size_request(sw, -1, 150);
	
	gtk_box_pack_start(GTK_BOX(vbox_tree), sw, TRUE, TRUE, 0);
	gtk_container_add(GTK_CONTAINER (sw), import_tree);

	/* Place the "Add File" button below the list view */
	add_file_button = gtk_button_new_with_label(_("Add File"));
	gtk_container_add (GTK_CONTAINER (vbox_tree), add_file_button);
	
	/* Filename Field */
	import_filename_renderer = gtk_cell_renderer_text_new();
	g_object_set(import_filename_renderer, "editable", FALSE, NULL);
	column_indexes[IMPORT_FILENAME_COLUMN] = IMPORT_FILENAME_COLUMN;
	g_signal_connect(G_OBJECT(import_filename_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), &column_indexes[IMPORT_FILENAME_COLUMN]);
	import_filename_column = gtk_tree_view_column_new_with_attributes(_("Shapefile"),
	                  import_filename_renderer,
	                  "text",
	                  IMPORT_FILENAME_COLUMN,
	                  NULL);
	g_object_set(import_filename_column, "resizable", TRUE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(import_tree), import_filename_column);

	/* Schema Field */
	import_schema_renderer = gtk_cell_renderer_text_new();
	g_object_set(import_schema_renderer, "editable", TRUE, NULL);
	column_indexes[IMPORT_SCHEMA_COLUMN] = IMPORT_SCHEMA_COLUMN;
	g_signal_connect(G_OBJECT(import_schema_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), &column_indexes[IMPORT_SCHEMA_COLUMN]);
	import_schema_column = gtk_tree_view_column_new_with_attributes(_("Schema"),
	                import_schema_renderer,
	                "text",
	                IMPORT_SCHEMA_COLUMN,
	                NULL);
	g_object_set(import_schema_column, "resizable", TRUE, "expand", TRUE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(import_tree), import_schema_column);

	/* Table Field */
	import_table_renderer = gtk_cell_renderer_text_new();
	g_object_set(import_table_renderer, "editable", TRUE, NULL);
	column_indexes[IMPORT_TABLE_COLUMN] = IMPORT_TABLE_COLUMN;
	g_signal_connect(G_OBJECT(import_table_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), &column_indexes[IMPORT_TABLE_COLUMN]);
	import_table_column = gtk_tree_view_column_new_with_attributes(_("Table"),
	               import_table_renderer,
	               "text",
	               IMPORT_TABLE_COLUMN,
	               NULL);
	g_object_set(import_table_column, "resizable", TRUE, "expand", TRUE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(import_tree), import_table_column);

	/* Geo column field */
	import_geom_column_renderer = gtk_cell_renderer_text_new();
	g_object_set(import_geom_column_renderer, "editable", TRUE, NULL);
	column_indexes[IMPORT_GEOMETRY_COLUMN] = IMPORT_GEOMETRY_COLUMN;
	g_signal_connect(G_OBJECT(import_geom_column_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), &column_indexes[IMPORT_GEOMETRY_COLUMN]);
	import_geom_column = gtk_tree_view_column_new_with_attributes(_("Geo Column"),
	              import_geom_column_renderer,
	              "text",
	              IMPORT_GEOMETRY_COLUMN,
	              NULL);
	g_object_set(import_geom_column, "resizable", TRUE, "expand", TRUE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(import_tree), import_geom_column);

	/* SRID Field */
	import_srid_renderer = gtk_cell_renderer_text_new();
	g_object_set(import_srid_renderer, "editable", TRUE, NULL);
	column_indexes[IMPORT_SRID_COLUMN] = IMPORT_SRID_COLUMN;
	g_signal_connect(G_OBJECT(import_srid_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), &column_indexes[IMPORT_SRID_COLUMN]);
	import_srid_column = gtk_tree_view_column_new_with_attributes("SRID",
	              import_srid_renderer,
	              "text",
	              IMPORT_SRID_COLUMN,
	              NULL);
	g_object_set(import_srid_column, "resizable", TRUE, "expand", TRUE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(import_tree), import_srid_column);

	/* Mode Combo Field */
	loader_mode_combo_list = gtk_list_store_new(LOADER_MODE_COMBO_COLUMNS, 
					G_TYPE_STRING,
					G_TYPE_CHAR);
	
	gtk_list_store_insert(loader_mode_combo_list, &iter, CREATE_MODE);
	gtk_list_store_set(loader_mode_combo_list, &iter,
	                   LOADER_MODE_COMBO_TEXT, _("Create"), 
			   LOADER_MODE_COMBO_OPTION_CHAR, 'c',			   
			   -1);
	gtk_list_store_insert(loader_mode_combo_list, &iter, APPEND_MODE);
	gtk_list_store_set(loader_mode_combo_list, &iter,
	                   LOADER_MODE_COMBO_TEXT, _("Append"), 
			   LOADER_MODE_COMBO_OPTION_CHAR, 'a', 
			   -1);
	gtk_list_store_insert(loader_mode_combo_list, &iter, DELETE_MODE);
	gtk_list_store_set(loader_mode_combo_list, &iter,
	                   LOADER_MODE_COMBO_TEXT, _("Delete"), 
			   LOADER_MODE_COMBO_OPTION_CHAR, 'd', 
			   -1);
	gtk_list_store_insert(loader_mode_combo_list, &iter, PREPARE_MODE);
	gtk_list_store_set(loader_mode_combo_list, &iter,
	                   LOADER_MODE_COMBO_TEXT, _("Prepare"), 
			   LOADER_MODE_COMBO_OPTION_CHAR, 'p', 
			   -1);
	loader_mode_combo = gtk_combo_box_new_with_model(GTK_TREE_MODEL(loader_mode_combo_list));
	import_mode_renderer = gtk_cell_renderer_combo_new();
	gtk_cell_layout_pack_start(GTK_CELL_LAYOUT(loader_mode_combo),
	                           import_mode_renderer, TRUE);
	gtk_cell_layout_add_attribute(GTK_CELL_LAYOUT(loader_mode_combo),
	                              import_mode_renderer, "text", 0);
	g_object_set(import_mode_renderer,
	             "model", loader_mode_combo_list,
	             "editable", TRUE,
	             "has-entry", FALSE,
	             "text-column", LOADER_MODE_COMBO_TEXT,
	             NULL);
	import_mode_column = gtk_tree_view_column_new_with_attributes(_("Mode"),
	              import_mode_renderer,
	              "text",
	              IMPORT_MODE_COLUMN,
	              NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(import_tree), import_mode_column);
	gtk_combo_box_set_active(GTK_COMBO_BOX(loader_mode_combo), 1);
	g_object_set(import_mode_column, "resizable", TRUE, "expand", TRUE, "sizing", GTK_TREE_VIEW_COLUMN_AUTOSIZE, NULL);
	
	g_signal_connect (G_OBJECT(import_mode_renderer), "changed", G_CALLBACK(pgui_action_handle_tree_combo), NULL);

	/* Remove Field */
	import_remove_renderer = gtk_cell_renderer_toggle_new();
	g_object_set(import_remove_renderer, "activatable", TRUE, NULL);
	g_signal_connect(G_OBJECT(import_remove_renderer), "toggled", G_CALLBACK (pgui_action_handle_file_remove), NULL);
	import_remove_column = gtk_tree_view_column_new_with_attributes("Rm",
	                import_remove_renderer, NULL);
	g_object_set(import_remove_column, "resizable", TRUE, "expand", FALSE, "fixed-width", 64, "sizing", GTK_TREE_VIEW_COLUMN_FIXED, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(import_tree), import_remove_column);

	g_signal_connect (G_OBJECT (add_file_button), "clicked", G_CALLBACK (pgui_action_open_file_dialog), NULL);

	/* Drag n Drop wiring */
	GtkTargetEntry drop_types[] =
	{
		{ "text/uri-list", 0, 0}
	};
	
	gint n_drop_types = sizeof(drop_types)/sizeof(drop_types[0]);
	gtk_drag_dest_set(GTK_WIDGET(import_tree),
	                  GTK_DEST_DEFAULT_ALL,
	                  drop_types, n_drop_types,
	                  GDK_ACTION_COPY);
	g_signal_connect(G_OBJECT(import_tree), "drag_data_received",
	                 G_CALLBACK(pgui_action_handle_file_drop), NULL);
}

/*
 * This function creates the UI artefacts for the file list table and hooks
 * up all the pretty signals.
 */
static void
pgui_create_export_table_table(GtkWidget *export_list_frame)
{
	GtkWidget *vbox_tree;
	GtkWidget *sw;
	gint *column_indexes;
	
	gtk_container_set_border_width (GTK_CONTAINER (export_list_frame), 0);

	vbox_tree = gtk_vbox_new(FALSE, 15);
	gtk_container_set_border_width(GTK_CONTAINER(vbox_tree), 5);
	gtk_container_add(GTK_CONTAINER(export_list_frame), vbox_tree);

	/* Setup a model */
	export_table_list_store = gtk_list_store_new(EXPORT_N_COLUMNS,
					 G_TYPE_POINTER,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
	                                 G_TYPE_STRING,
					 GTK_TYPE_TREE_MODEL,
					 G_TYPE_STRING,
	                                 G_TYPE_BOOLEAN);
	
	/* Create the view and such */
	export_tree = gtk_tree_view_new_with_model(GTK_TREE_MODEL(export_table_list_store));
	
	/* GTK has a slightly brain-dead API in that you can't directly find
	   the column being used by a GtkCellRenderer when using the same
	   callback to handle multiple fields; hence we manually store this
	   information here and pass a pointer to the column index into
	   the signal handler */
	column_indexes = g_malloc(sizeof(gint) * EXPORT_N_COLUMNS);
	
	/* Make the tree view in a scrollable window */
	sw = gtk_scrolled_window_new(NULL, NULL);
	gtk_scrolled_window_set_policy (GTK_SCROLLED_WINDOW(sw), GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
	gtk_scrolled_window_set_shadow_type (GTK_SCROLLED_WINDOW(sw), GTK_SHADOW_ETCHED_IN);
	gtk_widget_set_size_request(sw, -1, 150);
	
	gtk_box_pack_start(GTK_BOX(vbox_tree), sw, TRUE, TRUE, 0);
	gtk_container_add(GTK_CONTAINER (sw), export_tree);

	/* Place the "Add Table" button below the list view */
	add_table_button = gtk_button_new_with_label(_("Add Table"));
	gtk_container_add (GTK_CONTAINER (vbox_tree), add_table_button);

	/* Schema Field */
	export_schema_renderer = gtk_cell_renderer_text_new();
	g_object_set(export_schema_renderer, "editable", FALSE, NULL);
	column_indexes[EXPORT_SCHEMA_COLUMN] = EXPORT_SCHEMA_COLUMN;
	g_signal_connect(G_OBJECT(export_schema_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), &column_indexes[EXPORT_SCHEMA_COLUMN]);
	export_schema_column = gtk_tree_view_column_new_with_attributes(_("Schema"),
	                export_schema_renderer,
	                "text",
	                EXPORT_SCHEMA_COLUMN,
	                NULL);
	g_object_set(export_schema_column, "resizable", TRUE, "expand", TRUE, "sizing", GTK_TREE_VIEW_COLUMN_AUTOSIZE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(export_tree), export_schema_column);

	/* Table Field */
	export_table_renderer = gtk_cell_renderer_text_new();
	g_object_set(export_table_renderer, "editable", FALSE, NULL);
	column_indexes[EXPORT_TABLE_COLUMN] = EXPORT_TABLE_COLUMN;
	g_signal_connect(G_OBJECT(export_table_renderer), "edited", G_CALLBACK(pgui_action_handle_loader_edit), &column_indexes[EXPORT_TABLE_COLUMN]);
	export_table_column = gtk_tree_view_column_new_with_attributes(_("Table"),
	               export_table_renderer,
	               "text",
	               EXPORT_TABLE_COLUMN,
	               NULL);
	g_object_set(export_table_column, "resizable", TRUE, "expand", TRUE, "sizing", GTK_TREE_VIEW_COLUMN_AUTOSIZE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(export_tree), export_table_column);

	/* Geo column field */
	export_geom_column_combo = gtk_combo_box_new();
	export_geom_column_renderer = gtk_cell_renderer_combo_new();
	gtk_cell_layout_pack_start(GTK_CELL_LAYOUT(export_geom_column_combo),
	                           export_geom_column_renderer, TRUE);
	g_object_set(export_geom_column_renderer, 
		     "editable", TRUE,
		     "has-entry", FALSE,
	             "text-column", TABLECHOOSER_GEOCOL_COMBO_TEXT, 
		     NULL);
	export_geom_column = gtk_tree_view_column_new_with_attributes(_("Geo Column"),
	               export_geom_column_renderer,
		       "model",
		       EXPORT_GEOMETRY_LISTSTORE_COLUMN,
		       "text",
		       EXPORT_GEOMETRY_COLUMN,
	               NULL);
	g_object_set(export_geom_column, "resizable", TRUE, "sizing", GTK_TREE_VIEW_COLUMN_AUTOSIZE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(export_tree), export_geom_column);
	g_signal_connect (G_OBJECT(export_geom_column_renderer), "changed", G_CALLBACK(pgui_action_handle_table_geocol_combo), NULL);

	/* Filename Field */
	export_filename_renderer = gtk_cell_renderer_text_new();
	g_object_set(export_filename_renderer, "editable", TRUE, NULL);
	column_indexes[EXPORT_FILENAME_COLUMN] = EXPORT_FILENAME_COLUMN;
	g_signal_connect(G_OBJECT(export_filename_renderer), "edited", G_CALLBACK(pgui_action_handle_dumper_edit), &column_indexes[EXPORT_FILENAME_COLUMN]);
	export_filename_column = gtk_tree_view_column_new_with_attributes(_("Filename"),
	               export_filename_renderer,
	               "text",
	               EXPORT_FILENAME_COLUMN,
	               NULL);
	g_object_set(export_filename_column, "resizable", TRUE, "expand", TRUE, "sizing", GTK_TREE_VIEW_COLUMN_AUTOSIZE, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(export_tree), export_filename_column);
	
	/* Remove Field */
	export_remove_renderer = gtk_cell_renderer_toggle_new();
	g_object_set(export_remove_renderer, "activatable", TRUE, NULL);
	g_signal_connect(G_OBJECT(export_remove_renderer), "toggled", G_CALLBACK (pgui_action_handle_table_remove), NULL);
	export_remove_column = gtk_tree_view_column_new_with_attributes("Rm",
	                export_remove_renderer, NULL);
	g_object_set(export_remove_column, "resizable", TRUE, "expand", FALSE, "fixed-width", 64, "sizing", GTK_TREE_VIEW_COLUMN_FIXED, NULL);
	gtk_tree_view_append_column(GTK_TREE_VIEW(export_tree), export_remove_column);

	g_signal_connect (G_OBJECT (add_table_button), "clicked", G_CALLBACK (pgui_action_open_table_dialog), NULL);
}

static void
pgui_create_connection_window()
{
	/* Default text width */
	static int text_width = 12;
	
	/* Vbox container */
	GtkWidget *vbox;
	
	/* Reusable label handle */
	GtkWidget *label;

	/* PgSQL section */
	GtkWidget *frame_pg, *table_pg;
	
	/* OK button */
	GtkWidget *button_okay;

	/* Create the main top level window with a 10px border */
	window_conn = gtk_window_new(GTK_WINDOW_TOPLEVEL);
	gtk_container_set_border_width(GTK_CONTAINER(window_conn), 10);
	gtk_window_set_title(GTK_WINDOW(window_conn), _("PostGIS connection"));
	gtk_window_set_position(GTK_WINDOW(window_conn), GTK_WIN_POS_CENTER);
	gtk_window_set_modal(GTK_WINDOW(window_conn), TRUE);
	
	/* Use a vbox as the base container */
	vbox = gtk_vbox_new(FALSE, 15);
	
	/*
	** PostGIS info in a table
	*/
	frame_pg = gtk_frame_new(_("PostGIS Connection"));
	table_pg = gtk_table_new(5, 3, TRUE);
	gtk_container_set_border_width (GTK_CONTAINER (table_pg), 8);
	gtk_table_set_col_spacings(GTK_TABLE(table_pg), 7);
	gtk_table_set_row_spacings(GTK_TABLE(table_pg), 3);

	/* User name row */
	label = gtk_label_new(_("Username:"));
	entry_pg_user = gtk_entry_new();
	gtk_table_attach_defaults(GTK_TABLE(table_pg), label, 0, 1, 0, 1 );
	gtk_table_attach_defaults(GTK_TABLE(table_pg), entry_pg_user, 1, 3, 0, 1 );

	/* Password row */
	label = gtk_label_new(_("Password:"));
	entry_pg_pass = gtk_entry_new();
	gtk_entry_set_visibility( GTK_ENTRY(entry_pg_pass), FALSE);
	gtk_table_attach_defaults(GTK_TABLE(table_pg), label, 0, 1, 1, 2 );
	gtk_table_attach_defaults(GTK_TABLE(table_pg), entry_pg_pass, 1, 3, 1, 2 );

	/* Host and port row */
	label = gtk_label_new(_("Server Host:"));
	entry_pg_host = gtk_entry_new();
	gtk_entry_set_width_chars(GTK_ENTRY(entry_pg_host), text_width);
	gtk_table_attach_defaults(GTK_TABLE(table_pg), label, 0, 1, 2, 3 );
	gtk_table_attach_defaults(GTK_TABLE(table_pg), entry_pg_host, 1, 2, 2, 3 );

	entry_pg_port = gtk_entry_new();
	gtk_entry_set_width_chars(GTK_ENTRY(entry_pg_port), 8);
	gtk_table_attach_defaults(GTK_TABLE(table_pg), entry_pg_port, 2, 3, 2, 3 );

	/* Database row */
	label = gtk_label_new(_("Database:"));
	entry_pg_db   = gtk_entry_new();
	gtk_table_attach_defaults(GTK_TABLE(table_pg), label, 0, 1, 3, 4 );
	gtk_table_attach_defaults(GTK_TABLE(table_pg), entry_pg_db, 1, 3, 3, 4 );
			 
	/* Add table into containing frame */
	gtk_container_add(GTK_CONTAINER(frame_pg), table_pg);

	/* Add frame into containing vbox */
	gtk_container_add(GTK_CONTAINER(window_conn), vbox);
	
	/* Add the vbox into the window */
	gtk_container_add(GTK_CONTAINER(vbox), frame_pg);
	
	/* Create a simple "OK" button for the dialog */
	button_okay = gtk_button_new_with_label(_("OK"));
	gtk_container_add(GTK_CONTAINER(vbox), button_okay);
	g_signal_connect(G_OBJECT(button_okay), "clicked", G_CALLBACK(pgui_action_connection_okay), NULL);
	
	/* Hook the delete event so we don't destroy the dialog (only hide it) if cancelled */
	gtk_signal_connect(GTK_OBJECT(window_conn), "delete_event", GTK_SIGNAL_FUNC(pgui_event_popup_delete), NULL);
	
	return;
}	

static void
pgui_create_main_window(const SHPCONNECTIONCONFIG *conn)
{
	/* Main widgets */
	GtkWidget *vbox_main, *vbox_loader, *vbox_dumper;
	
	/* PgSQL section */
	GtkWidget *frame_pg, *import_list_frame, *export_list_frame, *frame_log;
	GtkWidget *button_pg_conn;
	
	/* Notebook */
	GtkWidget *notebook;
	
	/* Button section */
	GtkWidget *loader_hbox_buttons, *loader_button_options, *loader_button_import, *loader_button_cancel, *loader_button_about;
	GtkWidget *dumper_hbox_buttons, *dumper_button_options, *dumper_button_export, *dumper_button_cancel, *dumper_button_about;
	
	/* Log section */
	GtkWidget *scrolledwindow_log;

	/* Create the main top level window with a 10px border */
	window_main = gtk_window_new(GTK_WINDOW_TOPLEVEL);
	gtk_container_set_border_width(GTK_CONTAINER(window_main), 10);
	gtk_window_set_title(GTK_WINDOW(window_main), _("PostGIS Shapefile Import/Export Manager"));
	gtk_window_set_position(GTK_WINDOW(window_main), GTK_WIN_POS_CENTER_ALWAYS);
	gtk_window_set_resizable(GTK_WINDOW(window_main), FALSE);
	
	/* Open it a bit wider so that both the label and title show up */
	gtk_window_set_default_size(GTK_WINDOW(window_main), 180, 500);
	
	/* Connect the destroy event of the window with our pgui_quit function
	*  When the window is about to be destroyed we get a notificaiton and
	*  stop the main GTK loop
	*/
	g_signal_connect(G_OBJECT(window_main), "destroy", G_CALLBACK(pgui_quit), NULL);

	/* Connection row */
	frame_pg = gtk_frame_new(_("PostGIS Connection"));
	
	/* Test button row */
	button_pg_conn = gtk_button_new_with_label(_("View connection details..."));
	g_signal_connect(G_OBJECT(button_pg_conn), "clicked", G_CALLBACK(pgui_action_connection_details), NULL);
	gtk_container_set_border_width(GTK_CONTAINER(button_pg_conn), 10);
	gtk_container_add(GTK_CONTAINER(frame_pg), button_pg_conn);
	
	/*
	 * GTK Notebook for selecting import/export
	 */
	notebook = gtk_notebook_new();
	
	/*
	** Shape file selector
	*/
	import_list_frame = gtk_frame_new(_("Import List"));
	pgui_create_import_file_table(import_list_frame);

	/*
	** Row of action buttons
	*/
	loader_hbox_buttons = gtk_hbox_new(TRUE, 15);
	gtk_container_set_border_width (GTK_CONTAINER (loader_hbox_buttons), 0);

	/* Create the buttons themselves */
	loader_button_options = gtk_button_new_with_label(_("Options..."));
	loader_button_import = gtk_button_new_with_label(_("Import"));
	loader_button_cancel = gtk_button_new_with_label(_("Cancel"));
	loader_button_about = gtk_button_new_with_label(_("About"));

	/* Add actions to the buttons */
	g_signal_connect (G_OBJECT (loader_button_import), "clicked", G_CALLBACK (pgui_action_import), NULL);
	g_signal_connect (G_OBJECT (loader_button_options), "clicked", G_CALLBACK (pgui_action_loader_options_open), NULL);
	g_signal_connect (G_OBJECT (loader_button_cancel), "clicked", G_CALLBACK (pgui_action_cancel), NULL);
	g_signal_connect (G_OBJECT (loader_button_about), "clicked", G_CALLBACK (pgui_action_about_open), NULL);

	/* And insert the buttons into the hbox */
	gtk_box_pack_start(GTK_BOX(loader_hbox_buttons), loader_button_options, TRUE, TRUE, 0);
	gtk_box_pack_end(GTK_BOX(loader_hbox_buttons), loader_button_cancel, TRUE, TRUE, 0);
	gtk_box_pack_end(GTK_BOX(loader_hbox_buttons), loader_button_about, TRUE, TRUE, 0);
	gtk_box_pack_end(GTK_BOX(loader_hbox_buttons), loader_button_import, TRUE, TRUE, 0);

	/*
	** Table selector
	*/
	export_list_frame = gtk_frame_new(_("Export List"));
	pgui_create_export_table_table(export_list_frame);

	/*
	** Row of action buttons
	*/
	dumper_hbox_buttons = gtk_hbox_new(TRUE, 15);
	gtk_container_set_border_width (GTK_CONTAINER (dumper_hbox_buttons), 0);

	/* Create the buttons themselves */
	dumper_button_options = gtk_button_new_with_label(_("Options..."));
	dumper_button_export = gtk_button_new_with_label(_("Export"));
	dumper_button_cancel = gtk_button_new_with_label(_("Cancel"));
	dumper_button_about = gtk_button_new_with_label(_("About"));
	
	/* Add actions to the buttons */
	g_signal_connect (G_OBJECT (dumper_button_export), "clicked", G_CALLBACK (pgui_action_export), NULL);
	g_signal_connect (G_OBJECT (dumper_button_options), "clicked", G_CALLBACK (pgui_action_dumper_options_open), NULL);
	g_signal_connect (G_OBJECT (dumper_button_cancel), "clicked", G_CALLBACK (pgui_action_cancel), NULL);
	g_signal_connect (G_OBJECT (dumper_button_about), "clicked", G_CALLBACK (pgui_action_about_open), NULL);

	/* And insert the buttons into the hbox */
	gtk_box_pack_start(GTK_BOX(dumper_hbox_buttons), dumper_button_options, TRUE, TRUE, 0);
	gtk_box_pack_end(GTK_BOX(dumper_hbox_buttons), dumper_button_cancel, TRUE, TRUE, 0);
	gtk_box_pack_end(GTK_BOX(dumper_hbox_buttons), dumper_button_about, TRUE, TRUE, 0);
	gtk_box_pack_end(GTK_BOX(dumper_hbox_buttons), dumper_button_export, TRUE, TRUE, 0);
	
	/*
	** Log window
	*/
	frame_log = gtk_frame_new(_("Log Window"));
	gtk_container_set_border_width (GTK_CONTAINER (frame_log), 0);
	gtk_widget_set_size_request(frame_log, -1, 200);
	textview_log = gtk_text_view_new();
	textbuffer_log = gtk_text_buffer_new(NULL);
	scrolledwindow_log = gtk_scrolled_window_new(NULL, NULL);
	gtk_scrolled_window_set_policy( GTK_SCROLLED_WINDOW(scrolledwindow_log), GTK_POLICY_AUTOMATIC, GTK_POLICY_ALWAYS);
	gtk_text_view_set_buffer(GTK_TEXT_VIEW(textview_log), textbuffer_log);
	gtk_container_set_border_width (GTK_CONTAINER (textview_log), 5);
	gtk_text_view_set_editable(GTK_TEXT_VIEW(textview_log), FALSE);
	gtk_text_view_set_cursor_visible(GTK_TEXT_VIEW(textview_log), FALSE);
	gtk_text_view_set_wrap_mode(GTK_TEXT_VIEW(textview_log), GTK_WRAP_WORD);
	gtk_container_add (GTK_CONTAINER (scrolledwindow_log), textview_log);
	gtk_container_add (GTK_CONTAINER (frame_log), scrolledwindow_log);

	/*
	** Main window
	*/
	vbox_main = gtk_vbox_new(FALSE, 10);
	gtk_container_set_border_width (GTK_CONTAINER (vbox_main), 0);

	/* Add the loader frames into the notebook page */
	vbox_loader = gtk_vbox_new(FALSE, 10);
	gtk_container_set_border_width(GTK_CONTAINER(vbox_loader), 10);
	
	gtk_box_pack_start(GTK_BOX(vbox_loader), import_list_frame, FALSE, TRUE, 0);
	gtk_box_pack_start(GTK_BOX(vbox_loader), loader_hbox_buttons, FALSE, FALSE, 0);
	gtk_notebook_append_page(GTK_NOTEBOOK(notebook), vbox_loader, gtk_label_new(_("Import")));
	
	/* Add the dumper frames into the notebook page */
	vbox_dumper = gtk_vbox_new(FALSE, 10);
	gtk_container_set_border_width(GTK_CONTAINER(vbox_dumper), 10);
	
	gtk_box_pack_start(GTK_BOX(vbox_dumper), export_list_frame, FALSE, TRUE, 0);
	gtk_box_pack_start(GTK_BOX(vbox_dumper), dumper_hbox_buttons, FALSE, FALSE, 0);
	gtk_notebook_append_page(GTK_NOTEBOOK(notebook), vbox_dumper, gtk_label_new(_("Export")));	
	
	/* Add the frames into the main vbox */
	gtk_box_pack_start(GTK_BOX(vbox_main), frame_pg, FALSE, TRUE, 0);
	gtk_box_pack_start(GTK_BOX(vbox_main), notebook, FALSE, TRUE, 0);
	gtk_box_pack_start(GTK_BOX(vbox_main), frame_log, TRUE, TRUE, 0);
	
	/* and insert the vbox into the main window  */
	gtk_container_add(GTK_CONTAINER(window_main), vbox_main);
	
	/* make sure that everything, window and label, are visible */
	gtk_widget_show_all(window_main);

	return;
}

static void
usage()
{
	printf("RCSID: %s RELEASE: %s\n", S2P_RCSID, POSTGIS_VERSION);
	printf("USAGE: shp2pgsql-gui [options]\n");
	printf("OPTIONS:\n");
	printf("  -U <username>\n");
	printf("  -W <password>\n");
	printf("  -h <host>\n");
	printf("  -p <port>\n");
	printf("  -d <database>\n");
	printf("  -? Display this help screen\n");
}

int
main(int argc, char *argv[])
{
	int c;

#ifdef ENABLE_NLS
	setlocale (LC_ALL, "");
	bindtextdomain (PACKAGE, PGSQL_LOCALEDIR);
	textdomain (PACKAGE);
#endif

	/* Parse command line options and set configuration */
	global_loader_config = malloc(sizeof(SHPLOADERCONFIG));
	set_loader_config_defaults(global_loader_config);
	global_dumper_config = malloc(sizeof(SHPDUMPERCONFIG));
	set_dumper_config_defaults(global_dumper_config);
	
	/* Here we override any defaults for the GUI */
	global_loader_config->createindex = 1;
	global_loader_config->geo_col = strdup(GEOMETRY_DEFAULT);
	global_loader_config->dump_format = 1;
	
	conn = malloc(sizeof(SHPCONNECTIONCONFIG));
	memset(conn, 0, sizeof(SHPCONNECTIONCONFIG));
	
	/* Here we override any defaults for the connection */
	conn->host = strdup("localhost");
	conn->port = strdup("5432");

	while ((c = pgis_getopt(argc, argv, "U:p:W:d:h:")) != -1)
	{
		switch (c)
		{
		case 'U':
			conn->username = strdup(pgis_optarg);
			break;
		case 'p':
			conn->port = strdup(pgis_optarg);
			break;
		case 'W':
			conn->password = strdup(pgis_optarg);
			break;
		case 'd':
			conn->database = strdup(pgis_optarg);
			break;
		case 'h':
			conn->host = strdup(pgis_optarg);
			break;
		default:
			usage();
			free(conn);
			free(global_loader_config);
			exit(0);
		}
	}

	/* initialize the GTK stack */
	gtk_init(&argc, &argv);
	
	/* set up the user interface */
	pgui_create_main_window(conn);
	pgui_create_connection_window();
	pgui_create_loader_options_dialog();
	pgui_create_dumper_options_dialog();
	pgui_create_about_dialog();
	pgui_create_filechooser_dialog();
	pgui_create_progress_dialog();
	pgui_create_tablechooser_dialog();
	pgui_create_folderchooser_dialog();
	
	/* start the main loop */
	gtk_main();

	/* Free the configuration */
	free(conn);
	free(global_loader_config);

	return 0;
}
