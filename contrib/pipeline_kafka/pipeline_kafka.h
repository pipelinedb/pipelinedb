/*-------------------------------------------------------------------------
 *
 * pipeline_kafka.h
 *
 *	  PipelineDB support for Kafka
 *
 * Copyright (c) 2015, PipelineDB
 *
 * contrib/pipeline_kafka.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_KAFKA
#define PIPELINE_KAFKA

#include "fmgr.h"

extern void kafka_consume_main(Datum main_arg);

#endif /* PIPELINE_KAFKA */
