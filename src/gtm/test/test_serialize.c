#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "gtm/gtm_c.h"
#include "gtm/elog.h"
#include "gtm/palloc.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/assert.h"
#include "gtm/stringinfo.h"
#include "gtm/register.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"

#include "test_common.h"

pthread_key_t   threadinfo_key;

void
setUp()
{
}

void
tearDown()
{
}

int
test_snapshotdata_1(void)
{
  GTM_SnapshotData *data, *data2;
  char *buf;
  int buflen;

  SETUP();

  /* build a dummy GTM_SnapshotData data. */
  data = (GTM_SnapshotData *)malloc(sizeof(GTM_SnapshotData));

  data->sn_xmin = 128;
  data->sn_xmax = 256;
  data->sn_recent_global_xmin = 512;
  data->sn_xcnt = 1024;
  data->sn_xip  = 2048;

  printf("sn_xmin=%d, sn_xmax=%d, sn_recent_global_xmin=%d, sn_xcnt=%d, sn_xip=%d\n",
	 data->sn_xmin, data->sn_xmax, data->sn_recent_global_xmin, data->sn_xcnt, data->sn_xip);

  /* serialize */
  buflen = sizeof(GTM_SnapshotData);
  buf = (char *)malloc(buflen);
  gtm_serialize_snapshotdata(data, buf, buflen);

  /* destroy old buf */
  memset(data, 0, sizeof(GTM_SnapshotData));
  free(data);

  /* deserialize */
  data2 = (GTM_SnapshotData *)malloc(sizeof(GTM_SnapshotData));
  gtm_deserialize_snapshotdata(data2, buf, buflen);
  printf("sn_xmin=%d, sn_xmax=%d, sn_recent_global_xmin=%d, sn_xcnt=%d, sn_xip=%d\n",
	 data2->sn_xmin, data2->sn_xmax, data2->sn_recent_global_xmin, data2->sn_xcnt, data2->sn_xip);

  TEARDOWN();

  return 0;
}


GTM_TransactionInfo *
build_dummy_gtm_transactioninfo()
{
  GTM_TransactionInfo *data;

  data = (GTM_TransactionInfo *)malloc( sizeof(GTM_TransactionInfo) );

  data->gti_handle = 3;
  data->gti_backend_id = 13;
  data->gti_datanodecount = 0;
  data->gti_datanodes = NULL;
  data->gti_coordcount = 0;
  data->gti_coordinators = NULL;

  data->gti_gid = "hoge";

  data->gti_current_snapshot.sn_xmin = 128;
  data->gti_current_snapshot.sn_xmax = 256;
  data->gti_current_snapshot.sn_recent_global_xmin = 512;
  data->gti_current_snapshot.sn_xcnt = 1024;
  data->gti_current_snapshot.sn_xip  = 2048;

  return data;
}


int
test_transactioninfo_1(void)
{
  GTM_TransactionInfo *data,*data2;
  char *buf;
  int buflen;

  int k;
  char datanode[3][NI_MAXHOST];
  char coordnode[5][NI_MAXHOST];

  k = 0;
  strcpy(datanode[k++], "DN_1");
  strcpy(datanode[k++], "DN_2");
  strcpy(datanode[k++], "DN_3");

  k = 0;
  strcpy(coordnode[k++], "CN_1");
  strcpy(coordnode[k++], "CN_2");
  strcpy(coordnode[k++], "CN_3");
  strcpy(coordnode[k++], "CN_4");
  strcpy(coordnode[k++], "CN_5");

  SETUP();

  /* build a dummy GTM_SnapshotData data. */
  data = build_dummy_gtm_transactioninfo();
  data->gti_datanodecount = 3;
  data->gti_datanodes = datanode;
  data->gti_coordcount = 5;
  data->gti_coordinators = coordnode;

  /* serialize */
  buflen = sizeof( GTM_TransactionInfo );
  buf = (char *)malloc(buflen);
  gtm_serialize_transactioninfo(data, buf, buflen);

  /* destroy old buf */
  memset(data, 0, sizeof(GTM_TransactionInfo));
  free(data);

  /* deserialize */
  data2 = (GTM_TransactionInfo *)malloc(sizeof(GTM_TransactionInfo));
  gtm_deserialize_transactioninfo(data2, buf, buflen);

  printf("deserialized.\n");

  _ASSERT(data2->gti_handle==3);
  _ASSERT(data2->gti_backend_id==13);
  _ASSERT(data2->gti_datanodecount==3);
  _ASSERT(data2->gti_coordcount==5);
  _ASSERT(data2->gti_current_snapshot.sn_xmin==128);
  _ASSERT(data2->gti_current_snapshot.sn_xmax==256);
  _ASSERT(data2->gti_current_snapshot.sn_recent_global_xmin==512);
  _ASSERT(data2->gti_current_snapshot.sn_xcnt==1024);
  _ASSERT(data2->gti_current_snapshot.sn_xip==2048);

  TEARDOWN();

  return 0;
}

int
test_transactions_1(void)
{
  GTM_Transactions *data,*data2;
  GTM_TransactionInfo *d;
  char *buf;
  int buflen;
  int k;
  char datanode[3][NI_MAXHOST];
  char coordnode[5][NI_MAXHOST];

  k = 0;
  strcpy(datanode[k++], "DN_1");
  strcpy(datanode[k++], "DN_2");
  strcpy(datanode[k++], "DN_3");

  k = 0;
  strcpy(coordnode[k++], "CN_1");
  strcpy(coordnode[k++], "CN_2");
  strcpy(coordnode[k++], "CN_3");
  strcpy(coordnode[k++], "CN_4");
  strcpy(coordnode[k++], "CN_5");

  SETUP();

  data = (GTM_Transactions *)malloc( sizeof(GTM_Transactions) );
  data->gt_lastslot = 13;

  /* build a dummy GTM_TransactionInfo data. */
  d = build_dummy_gtm_transactioninfo();
  d->gti_datanodecount = 3;
  d->gti_datanodes = datanode;
  d->gti_coordcount = 5;
  d->gti_coordinators = coordnode;

  memcpy(&data->gt_transactions_array[0], d, sizeof(data->gt_transactions_array[0]));

  printf("gt_lastslot=%d\n",
	 data->gt_lastslot);

  /* serialize */
  buflen = gtm_get_transactions_size( data );
  buf = (char *)malloc(buflen);

  if ( !gtm_serialize_transactions(data, buf, buflen) )
  {
    printf("error.\n");
    exit(1);
  }

  /* destroy old buf */
  memset(data, 0, sizeof(GTM_Transactions));
  free(data);

  /* deserialize */
  data2 = (GTM_Transactions *)malloc(sizeof(GTM_Transactions));
  gtm_deserialize_transactions(data2, buf, buflen);

  printf("deserialized.\n");

  printf("gt_lastslot=%d\n",
	 data2->gt_lastslot);

  printf("gti_handle=%d, gti_backend_id=%d\n", 
	 data2->gt_transactions_array[0].gti_handle,
	 data2->gt_transactions_array[0].gti_backend_id);

  TEARDOWN();

  return 0;
}


void
test_pgxcnodeinfo_1()
{
  GTM_PGXCNodeInfo *data,*data2;
  char *buf;
  size_t buflen;

  SETUP();

  data = (GTM_PGXCNodeInfo *)malloc( sizeof(GTM_PGXCNodeInfo) );
  data->type = 2;
  data->nodename = "three";
  data->port = 7;
  data->ipaddress = "foo";
  data->datafolder = "bar";

  printf("type=%d, nodename=%s, port=%d, ipaddress=%s, datafolder=%s\n",
	 data->type, data->nodename, data->port,
	 data->ipaddress, data->datafolder);

  /* serialize */
  buflen = gtm_get_pgxcnodeinfo_size( data );
  buf = (char *)malloc(buflen);

  if ( !gtm_serialize_pgxcnodeinfo(data, buf, buflen) )
  {
    printf("error.\n");
    exit(1);
  }

  /* destroy old buf */
  memset(data, 0, sizeof(GTM_PGXCNodeInfo));
  free(data);

  /* deserialize */
  data2 = (GTM_PGXCNodeInfo *)malloc(sizeof(GTM_PGXCNodeInfo));
  gtm_deserialize_pgxcnodeinfo(data2, buf, buflen);

  printf("deserialized.\n");

  printf("type=%d, nodename=%s, port=%d, ipaddress=%s, datafolder=%s\n",
	 data2->type, data2->nodename, data2->port,
	 data2->ipaddress, data2->datafolder);

  TEARDOWN();

  return 0;
}

int
test_seqinfo_1(void)
{
  GTM_SeqInfo *d1, *d2;
  char *buf;
  int buflen;
  GTM_SequenceKeyData gs_key;

  SETUP();

  /* build a dummy GTM_SnapshotData data. */
  d1 = (GTM_SeqInfo *)malloc(sizeof(GTM_SeqInfo));

  gs_key.gsk_keylen = 0;
  gs_key.gsk_key = "";
  gs_key.gsk_type = GTM_SEQ_DB_NAME;

  d1->gs_key = &gs_key;
  d1->gs_value = 3;
  d1->gs_init_value = 5;
  d1->gs_last_value = 7;
  d1->gs_increment_by = 11;
  d1->gs_min_value = 13;
  d1->gs_max_value = 17;
  d1->gs_cycle = true;
  d1->gs_called = true;
  d1->gs_ref_count = 19;
  d1->gs_state = 23;

  /* serialize */
  buflen = gtm_get_sequence_size(d1);
  buf = (char *)malloc(buflen);
  gtm_serialize_sequence(d1, buf, buflen);

  /* destroy old buf */
  memset(d1, 0, sizeof(GTM_SeqInfo));
  free(d1);

  /* deserialize */
  //  d2 = (GTM_SeqInfo *)malloc(sizeof(GTM_SeqInfo));
  d2 = gtm_deserialize_sequence(buf, buflen);

  _ASSERT( d2->gs_key->gsk_keylen==0 );
  _ASSERT( strcmp(d2->gs_key->gsk_key,"")==0 );
  _ASSERT( d2->gs_key->gsk_type==GTM_SEQ_DB_NAME );

  TEARDOWN();

  return 0;
}

int
main(void)
{
  test_snapshotdata_1();
  test_transactioninfo_1();
  test_transactions_1();
  test_pgxcnodeinfo_1();
  test_seqinfo_1();

  return 0;
}
