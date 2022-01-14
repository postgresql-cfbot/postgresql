# src/bin/pg_replslotdata/nls.mk
CATALOG_NAME     = pg_replslotdata
AVAIL_LANGUAGES  = cs de el es fr ja ko pl ru sv tr uk vi zh_CN
GETTEXT_FILES    = $(FRONTEND_COMMON_GETTEXT_FILES) pg_replslotdata.c
GETTEXT_TRIGGERS = $(FRONTEND_COMMON_GETTEXT_TRIGGERS)
GETTEXT_FLAGS    = $(FRONTEND_COMMON_GETTEXT_FLAGS)
