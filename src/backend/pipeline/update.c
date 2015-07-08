/*-------------------------------------------------------------------------
 *
 * update.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/update.c
 *
 *-------------------------------------------------------------------------
 */
#include <curl/curl.h>
#include <sys/utsname.h>

#include "postgres.h"

#include "pgstat.h"
#include "funcapi.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_stream.h"
#include "lib/stringinfo.h"
#include "pipeline/update.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pipelinefuncs.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define PDB_VERSION "0.7.7"
#define UPDATE_AVAILABLE 201
#define NO_UPDATES 200

static const char *update_url = "http://anonymous.pipelinedb.com/check";
static const char *payload_format = "data={ \"e\": \"%s\", \"v\": \"%s\", \"s\": \"%s\", \"sr\": \"%s\", \"sv\": \"%s\", \"n\": %u, \"ri\": %ld, \"bi\": %ld, \"ro\": %ld, \"bo\": %ld }";

/* guc */
bool anonymous_update_checks;

static size_t
silent(void *buffer, size_t size, size_t nmemb, void *userp)
{
   return size * nmemb;
}

/*
 * UpdateCheck
 *
 * Check if any software updates are available and anonymously report the following information:
 *
 * 0) Operating system
 * 1) Number of continuous views
 * 2) Aggregate input/output rows/bytes
 */
void
UpdateCheck(HTAB *all_dbs, bool startup)
{
	PgStat_StatDBEntry *db_entry;
	CQStatEntry *cq_entry;
	HASH_SEQ_STATUS db_iter;
	HASH_SEQ_STATUS cq_iter;
	StringInfoData payload;
  CURL *curl;
  CURLcode res;
	int cvs = 0;
	long rows_in = 0;
	long bytes_in = 0;
	long rows_out = 0;
	long bytes_out = 0;
	static bool initialized = false;
	struct utsname mname;
	char name[64];

	uname(&mname);
	strncpy(name, mname.sysname, 64);

	hash_seq_init(&db_iter, all_dbs);
	while ((db_entry = (PgStat_StatDBEntry *) hash_seq_search(&db_iter)) != NULL)
	{
		hash_seq_init(&cq_iter, db_entry->cont_queries);
		while ((cq_entry = (CQStatEntry *) hash_seq_search(&cq_iter)) != NULL)
		{
			Oid viewid = GetCQStatView(cq_entry->key);

			/* keep scanning if it's a proc-level stats entry */
			if (!viewid)
				continue;

			if ((GetCQStatProcType(cq_entry->key) == CQ_STAT_WORKER))
			{
				cvs++;
				rows_in += cq_entry->input_rows;
				bytes_in += cq_entry->input_bytes;
			}
			else
			{
				rows_out += cq_entry->output_rows;
				bytes_out += cq_entry->output_bytes;
			}
		}
	}

	if (cvs == 0)
		return;

	initStringInfo(&payload);
	appendStringInfo(&payload, payload_format, startup ? "startup" : "hourly",
			PDB_VERSION, name, mname.release, mname.version,
			cvs, rows_in, bytes_in, rows_out, bytes_out);

	/* now check if there's a newer version available */
  if (!initialized)
  {
		curl_global_init(CURL_GLOBAL_ALL);
		initialized = true;
  }

  curl = curl_easy_init();
  if(curl)
  {
    curl_easy_setopt(curl, CURLOPT_URL, update_url);
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.data);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, silent);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
    res = curl_easy_perform(curl);
    if(res == CURLE_OK)
    {
    	long code = 0;
    	curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &code);
    	if (code == UPDATE_AVAILABLE)
    		elog(NOTICE, "a newer version of PipelineDB is available");
    }
    else
    {
			/* don't be noisy about errors */
    }
    curl_easy_cleanup(curl);
  }
}
