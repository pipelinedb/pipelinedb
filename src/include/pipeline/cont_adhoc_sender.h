#ifndef CONT_ADHOC_SENDER_H
#define CONT_ADHOC_SENDER_H

#include "access/tupdesc.h"
#include "executor/tuptable.h"

// CreateDestReceiver

extern struct AdhocSender* sender_create(void);

extern void sender_startup(struct AdhocSender *sender, TupleDesc tup_desc,
		bool is_agg,
		AttrNumber *keyColIdx,
		int	numCols);
extern void sender_shutdown(struct AdhocSender *sender);
extern void sender_insert(struct AdhocSender *sender, TupleTableSlot *slot);
extern void sender_update(struct AdhocSender *sender, TupleTableSlot *slot);

#endif
