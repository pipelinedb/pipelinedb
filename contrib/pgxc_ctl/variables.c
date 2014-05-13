/*-------------------------------------------------------------------------
 *
 * varibales.c
 *
 *    Variable haneling module of Postgres-XC configuration and operation tool.
 *
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <string.h>
#include "variables.h"
#include "utils.h"
#include "pgxc_ctl_log.h"

pgxc_ctl_var *var_head = NULL;
pgxc_ctl_var *var_tail = NULL;

static void clear_var(pgxc_ctl_var *var);
/*
 * Hash bucket size is up to 256
 */
static int
hash_val(char *name)
{
	unsigned char *name_u = (unsigned char *)name;
	unsigned char v;
	
	for(v = 0; *name_u; name_u++)
		v += *name_u;
	return (v%NUM_HASH_BUCKET);
}

#define LIMIT_TO_DOUBLE 128
#define INCR_OVER_DOUBLE 10

static int
next_size(int sz)
{
	if (sz <= 0)
		return 1;
	if (sz <= LIMIT_TO_DOUBLE)
		return sz*2;
	else
		return sz + INCR_OVER_DOUBLE;
}

void
init_var_hash()
{
	int i;

	for (i = 0; i < NUM_HASH_BUCKET; i++)
	{
		var_hash[i].el_size = 1;
		var_hash[i].el_used = 0;
		var_hash[i].el = (pgxc_ctl_var **)Malloc(sizeof(pgxc_ctl_var *));
		var_hash[i].el[0] = NULL;
	}
}

static void
remove_from_hash(pgxc_ctl_var *var)
{
	int hash_v = hash_val(var->varname);
	int ii, jj;

	for(ii = 0; var_hash[hash_v].el[ii]; ii++)
	{
		if (var_hash[hash_v].el[ii] != var)
			continue;
		else
		{
			for(jj = ii; var_hash[hash_v].el[jj]; jj++)
				var_hash[hash_v].el[jj] = var_hash[hash_v].el[jj + 1];
			var_hash[hash_v].el_used--;
			return;
		}
	}
	return;
}

void
add_var_hash(pgxc_ctl_var *var)
{
	int	hash_v = hash_val(var->varname);
	if (var_hash[hash_v].el_used + 1 >= var_hash[hash_v].el_size)
	{
		var_hash[hash_v].el_size = next_size(var_hash[hash_v].el_size);
		var_hash[hash_v].el = (pgxc_ctl_var **)Realloc(var_hash[hash_v].el, sizeof(pgxc_ctl_var *) * var_hash[hash_v].el_size);
	}
	var_hash[hash_v].el[var_hash[hash_v].el_used++] = var;
	var_hash[hash_v].el[var_hash[hash_v].el_used] = NULL;
}

pgxc_ctl_var *
new_var(char *name)
{
	pgxc_ctl_var *newv;

	if (find_var(name))
	{
		elog(ERROR, "ERROR: Variable %s already defined. Check your configuration.\n", name);
		return NULL;
	}

	newv = (pgxc_ctl_var *)Malloc(sizeof(pgxc_ctl_var));
	if (var_head == NULL)
	{
		var_head = var_tail = newv;
		newv->prev = NULL;
	}
	else
	{
		newv->prev = var_tail;
		var_tail->next = newv;
		var_tail = newv;
	}
	newv->next = NULL;
	newv->varname = Strdup(name);
	newv->val_size = 1;
	newv->val_used = 0;
	newv->val = (char **)Malloc(sizeof(char *));
	newv->val[0] = NULL;
	add_var_hash(newv);
	return(newv);
}

void
remove_var(pgxc_ctl_var *var)
{
	if ((var_head == var_tail) && (var_head == var))
		var_head = var_tail = NULL;
	else if (var_head == var)
	{
		var_head = var_head->next;
		var_head->prev = NULL;
	}
	else if (var_tail == var)
	{
		var_tail->next = NULL;
		var_tail = var_tail->prev;
	}
	else
	{
		var->prev->next = var->next;
		var->next->prev = var->prev;
	}
	clear_var(var);
}

static void
clear_var(pgxc_ctl_var *var)
{
	int ii;

	remove_from_hash(var);
	for (ii = 0; var->val[ii]; ii++)
		free(var->val[ii]);
	free(var->varname);
	free(var);
			 
}		

void
add_val(pgxc_ctl_var *var, char *val)
{
	if (var->val_size <= var->val_used+1)
	{
		var->val_size = next_size(var->val_size);
		var->val = (char **)Realloc(var->val, sizeof(char *)*var->val_size);
	}
	var->val[var->val_used++] = Strdup(val);
	var->val[var->val_used] = NULL;
}

void
add_val_name(char *name, char *val)
{
	pgxc_ctl_var *var;
	if (!(var = find_var(name)))
		return;
	add_val(var, name);
	return;
}


pgxc_ctl_var *
find_var(char *name)
{
	pgxc_var_hash *hash = &var_hash[hash_val(name)];
	int	i;

	for (i = 0; i < hash->el_used; i++)
	{
		if (strcmp(hash->el[i]->varname, name) == 0)
			return hash->el[i];
	}
	return NULL;
}

char *
sval(char *name)
{
	pgxc_ctl_var *var = find_var(name);
	if (!var)
		return NULL;
	return var->val[0];
}

char **
aval(char *name)
{
	pgxc_ctl_var *var = find_var(name);
	if (!var)
		return NULL;
	return var->val;
}

void
reset_value(pgxc_ctl_var *var)
{
	int i;
	for (i = 0; var->val[i]; i++)
	{
		Free (var->val[i]);
		var->val[i] = NULL;
	}
	var->val_used = 0;
}

void
assign_val(char *destName, char *srcName)
{
	pgxc_ctl_var *dest = find_var(destName);
	pgxc_ctl_var *src = find_var(srcName);
	int ii;

	reset_value(dest);
	for (ii = 0; ii < src->val_used; ii++)
		add_val(dest, src->val[ii]);
}

void
assign_sval(char *destName, char *val)
{
	pgxc_ctl_var *dest = find_var(destName);

	reset_value(dest);
	add_val(dest, val);
}

void
reset_var(char *name)
{
	confirm_var(name);
	reset_value(find_var(name));
}

void
reset_var_val(char *name, char *val)
{
	reset_var(name);
	add_val(find_var(name), val);
}

pgxc_ctl_var *
confirm_var(char *name)
{
	pgxc_ctl_var *rc;
	if ((rc = find_var(name)))
		return rc;
	return new_var(name);
}

void
print_vars(void)
{
	pgxc_ctl_var *cur;

	lockLogFile();
	for(cur = var_head; cur; cur=cur->next)
		print_var(cur->varname);
	unlockLogFile();
}

void
print_var(char *vname)
{
	pgxc_ctl_var *var;
	char outBuf[MAXLINE + 1];

	outBuf[0] = 0;
	if ((var = find_var(vname)) == NULL)
	{
		elog(ERROR, "ERROR: Variable %s not found.\n", vname);
		return;
	}
	else
	{
		char **curv;
		char editbuf[MAXPATH];

		snprintf(editbuf, MAXPATH, "%s (", vname);
		strncat(outBuf, editbuf, MAXLINE);
		for (curv=var->val; *curv; curv++)
		{
			snprintf(editbuf, MAXPATH, " \"%s\" ", *curv);
			strncat(outBuf, editbuf, MAXLINE);
		}
		strncat(outBuf, ")", MAXLINE);
		elog(NOTICE, "%s\n", outBuf);
	}
	
}

void
log_var(char *varname)
{
	if (logFile)
		print_var(varname);
}

int
arraySizeName(char *name)
{
	pgxc_ctl_var *var;

	if ((var = find_var(name)) == NULL)
		return -1;
	return(arraySize(var));
}

int
arraySize(pgxc_ctl_var *var)
{
	return var->val_used;
}

char **
add_member(char **array, char *val)
{
	char **rv;
	int ii;

	for (ii = 0; array[ii]; ii++);
	rv = Realloc(array, sizeof(char *) * (ii + 2));
	rv[ii] = Strdup(val);
	rv[ii+1] = NULL;
	return(rv);
}

void
clean_array(char **array)
{
	int ii;
	if (array)
	{
		for(ii = 0; array[ii]; ii++)
			Free(array[ii]);
		Free(array);
	}
}

void
var_assign(char **dest, char *src)
{
	Free(*dest);
	*dest = src;
}

char *
listValue(char *name)
{
	pgxc_ctl_var *dest;
	int ii;
	char *buf;

	if ((dest = find_var(name)) == NULL)
		return Strdup("");
	buf = Malloc(MAXLINE+1);
	buf[0]=0;
	for(ii = 0; ii < dest->val_used; ii++)
	{
		strncat(buf, dest->val[ii], MAXLINE);
		strncat(buf, " ", MAXLINE);
	}
	return buf;
}

int
ifExists(char *name, char *value)
{
	pgxc_ctl_var *var = find_var(name);
	int ii;

	if (!var)
		return FALSE;
	for (ii = 0; ii < var->val_used; ii++)
		if (strcmp((var->val)[ii], value) == 0)
			return TRUE;
	return FALSE;
}
	
int
IfExists(char *name, char *value)
{
	pgxc_ctl_var *var = find_var(name);
	int ii;

	if (!var)
		return FALSE;
	for (ii = 0; ii < var->val_used; ii++)
		if (strcasecmp((var->val)[ii], value) == 0)
			return TRUE;
	return FALSE;
}

int
extendVar(char *name, int newSize, char *def_value)
{
	pgxc_ctl_var *target;
	char **old_val;
	int old_size;
	int ii;

	if ((target = find_var(name)) == NULL)
		return -1;
	if (def_value == NULL)
		def_value = "none";
	if (target->val_size < newSize)
	{
		old_val = target->val;
		old_size = target->val_size;
		target->val = Malloc0(sizeof(char *) * (newSize +1));
		memcpy(target->val, old_val, sizeof(char *) * old_size);
		target->val_size = newSize;
		Free(old_val);
		for (ii = target->val_used; ii < newSize; ii++)
			(target->val)[ii] = Strdup(def_value);
		target->val_used = newSize;
	}
	else if (target->val_used < newSize)
	{
		for (ii = target->val_used; ii < newSize; ii++)
			(target->val)[ii] = Strdup(def_value);
		target->val_used = newSize;
	}
	return 0;
}


/* 
 * If pad is NULL, then "none" will be padded.
 * Returns *val if success, NULL if failed
 */
void
assign_arrayEl(char *name, int idx, char *val, char *pad)
{
	pgxc_ctl_var *var = confirm_var(name);

	if (pad == NULL)
		pad = "none";
	/*
	 * Pad if needed
	 */
	extendVar(name, idx+1, pad);
	Free(var->val[idx]);
	var->val[idx] = Strdup(val);
}


int doesExist(char *name, int idx)
{
	pgxc_ctl_var *var;

	if (name == NULL)
		return 0;
	if ((var = find_var(name)) == NULL)
		return 0;
	if (var->val_used <= idx)
		return 0;
	return 1;
}
