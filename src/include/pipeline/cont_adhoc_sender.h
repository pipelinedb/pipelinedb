#ifndef CONT_ADHOC_SENDER_H
#define CONT_ADHOC_SENDER_H

#include "access/tupdesc.h"

extern struct AdhocSender* sender_create(void);

extern void sender_startup(struct AdhocSender *sender, TupleDesc tup_desc);
extern void sender_shutdown(struct AdhocSender *sender);
extern void sender_insert(struct AdhocSender *sender);
extern void sender_update(struct AdhocSender *sender);

#endif
