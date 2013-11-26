# src/interfaces/ecpg/preproc/nls.mk
CATALOG_NAME     = ecpg
AVAIL_LANGUAGES  = de es fr it ja ko pt_BR tr zh_CN zh_TW
GETTEXT_FILES    = descriptor.c ecpg.c pgc.c preproc.c type.c variable.c
GETTEXT_TRIGGERS = mmerror:3
GETTEXT_FLAGS    = mmerror:3:c-format
