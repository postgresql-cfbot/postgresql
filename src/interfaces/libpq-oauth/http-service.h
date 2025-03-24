/*-------------------------------------------------------------------------
 *
 * http-service.
 *
 *	  Definitions for HTTP service file
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/interfaces/libpq-oauth/oauth-curl.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef HTTP_SERVICE_H
#define HTTP_SERVICE_H

#include "libpq-fe.h"

enum fcurl_type_e {
  CFTYPE_NONE = 0,
  CFTYPE_FILE = 1,
  CFTYPE_CURL = 2
};
struct fcurl_data
{
  enum fcurl_type_e type;     /* type of handle */
  union {
    CURL *curl;
    FILE *file;
  } handle;                   /* handle */

  char *buffer;               /* buffer to store cached data*/
  size_t buffer_len;          /* currently allocated buffers length */
  size_t buffer_pos;          /* end of data in buffer*/
  int still_running;          /* Is background url fetch still in progress */
};

typedef struct fcurl_data URL_FILE;

/* Exported flow callback. */

extern PGDLLEXPORT URL_FILE
*url_fopen(const char *url, const char *operation);

extern PGDLLEXPORT char
*url_fgets(char *ptr, size_t size, URL_FILE *file);

extern PGDLLEXPORT int
url_fclose(URL_FILE *file);

#endif							/* HTTP_SERVICE_H */
