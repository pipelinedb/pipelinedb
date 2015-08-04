#ifndef OLDROW_H_35A6D47D
#define OLDROW_H_35A6D47D

typedef struct OldField {

	int len;
	char* data;
} OldField;

typedef struct OldRow {

	char* ptr; // points to original malloced chunk.
	int num_fields;
	OldField* fields; // array of fields
} OldRow;

#endif
