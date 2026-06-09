#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "postgres_fe.h"
#include "pqexpbuffer.h"
#include <errno.h>
#include "oauth-utils.h"
#include "http-service.h"

/* we use a global one for convenience */
static CURLM *multi_handle;

/* curl calls this routine to get more data */
static size_t write_callback(char *buffer,
                             size_t size,
                             size_t nitems,
                             void *userp)
{
  char *newbuff;
  size_t rembuff;

  URL_FILE *url = (URL_FILE *)userp;
  size *= nitems;

  rembuff = url->buffer_len - url->buffer_pos; /* remaining space in buffer */

  if(size > rembuff) {
    /* not enough space in buffer */
    newbuff = realloc(url->buffer, url->buffer_len + (size - rembuff));
    if(!newbuff) {
      fprintf(stderr, "callback buffer grow failed\n");
      size = rembuff;
    }
    else {
      /* realloc succeeded increase buffer size*/
      url->buffer_len += size - rembuff;
      url->buffer = newbuff;
    }
  }

  memcpy(&url->buffer[url->buffer_pos], buffer, size);
  url->buffer_pos += size;

  return size;
}

/* use to attempt to fill the read buffer up to requested number of bytes */
static int fill_buffer(URL_FILE *file, size_t want)
{
  fd_set fdread;
  fd_set fdwrite;
  fd_set fdexcep;
  struct timeval timeout;
  int rc;
  CURLMcode mc; /* curl_multi_fdset() return code */

  /* only attempt to fill buffer if transactions still running and buffer
   * does not exceed required size already
   */
  if((!file->still_running) || (file->buffer_pos > want))
    return 0;

  /* attempt to fill buffer */
  do {
    int maxfd = -1;
    long curl_timeo = -1;

    FD_ZERO(&fdread);
    FD_ZERO(&fdwrite);
    FD_ZERO(&fdexcep);

    /* set a suitable timeout to fail on */
    timeout.tv_sec = 60; /* 1 minute */
    timeout.tv_usec = 0;

    curl_multi_timeout(multi_handle, &curl_timeo);
    if(curl_timeo >= 0) {
      timeout.tv_sec = curl_timeo / 1000;
      if(timeout.tv_sec > 1)
        timeout.tv_sec = 1;
      else
        timeout.tv_usec = (curl_timeo % 1000) * 1000;
    }

    /* get file descriptors from the transfers */
    mc = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd);

    if(mc != CURLM_OK) {
      fprintf(stderr, "curl_multi_fdset() failed, code %d.\n", mc);
      break;
    }

    /* On success the value of maxfd is guaranteed to be >= -1. We call
       select(maxfd + 1, ...); specially in case of (maxfd == -1) there are
       no fds ready yet so we call select(0, ...) --or Sleep() on Windows--
       to sleep 100ms, which is the minimum suggested value in the
       curl_multi_fdset() doc. */

    if(maxfd == -1) {
#ifdef _WIN32
      Sleep(100);
      rc = 0;
#else
      /* Portable sleep for platforms other than Windows. */
      struct timeval wait = { 0, 100 * 1000 }; /* 100ms */
      rc = select(0, NULL, NULL, NULL, &wait);
#endif
    }
    else {
      /* Note that on some platforms 'timeout' may be modified by select().
         If you need access to the original value save a copy beforehand. */
      rc = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
    }

    switch(rc) {
    case -1:
      /* select error */
      break;

    case 0:
    default:
      /* timeout or readable/writable sockets */
      curl_multi_perform(multi_handle, &file->still_running);
      break;
    }
  } while(file->still_running && (file->buffer_pos < want));
  return 1;
}

/* use to remove want bytes from the front of a files buffer */
static int use_buffer(URL_FILE *file, size_t want)
{
  /* sort out buffer */
  if(file->buffer_pos <= want) {
    /* ditch buffer - write will recreate */
    free(file->buffer);
    file->buffer = NULL;
    file->buffer_pos = 0;
    file->buffer_len = 0;
  }
  else {
    /* move rest down make it available for later */
    memmove(file->buffer,
            &file->buffer[want],
            (file->buffer_pos - want));

    file->buffer_pos -= want;
  }
  return 0;
}

URL_FILE *url_fopen(const char *url, const char *operation)
{
  /* this code could check for URLs or types in the 'url' and
     basically use the real fopen() for standard files */

  URL_FILE *file;
  (void)operation;

  file = calloc(1, sizeof(URL_FILE));
  if(!file)
    return NULL;

  file->handle.file = fopen(url, operation);
  if(file->handle.file)
    file->type = CFTYPE_FILE; /* marked as URL */

  else {
    file->type = CFTYPE_CURL; /* marked as URL */
    file->handle.curl = curl_easy_init();

    curl_easy_setopt(file->handle.curl, CURLOPT_URL, url);
    curl_easy_setopt(file->handle.curl, CURLOPT_WRITEDATA, file);
    curl_easy_setopt(file->handle.curl, CURLOPT_VERBOSE, 0L);
    curl_easy_setopt(file->handle.curl, CURLOPT_WRITEFUNCTION, write_callback);

    if(!multi_handle)
      multi_handle = curl_multi_init();

    curl_multi_add_handle(multi_handle, file->handle.curl);

    /* lets start the fetch */
    curl_multi_perform(multi_handle, &file->still_running);

    if((file->buffer_pos == 0) && (!file->still_running)) {
      /* if still_running is 0 now, we should return NULL */

      /* make sure the easy handle is not in the multi handle anymore */
      curl_multi_remove_handle(multi_handle, file->handle.curl);

      /* cleanup */
      curl_easy_cleanup(file->handle.curl);

      free(file);

      file = NULL;
    }
  }
  return file;
}

int url_fclose(URL_FILE *file)
{
  int ret = 0;/* default is good return */

  switch(file->type) {
  case CFTYPE_FILE:
    ret = fclose(file->handle.file); /* passthrough */
    break;

  case CFTYPE_CURL:
    /* make sure the easy handle is not in the multi handle anymore */
    curl_multi_remove_handle(multi_handle, file->handle.curl);

    /* cleanup */
    curl_easy_cleanup(file->handle.curl);
    break;

  default: /* unknown or supported type - oh dear */
    ret = EOF;
    errno = EBADF;
    break;
  }

  free(file->buffer);/* free any allocated buffer space */
  free(file);

  return ret;
}

char *url_fgets(char *ptr, size_t size, URL_FILE *file)
{
  size_t want = size - 1;/* always need to leave room for zero termination */
  size_t loop;

  switch(file->type) {
  case CFTYPE_FILE:
    ptr = fgets(ptr, (int)size, file->handle.file);
    break;

  case CFTYPE_CURL:
    fill_buffer(file, want);

    /* check if there's data in the buffer - if not fill either errored or
     * EOF */
    if(!file->buffer_pos)
      return NULL;

    /* ensure only available data is considered */
    if(file->buffer_pos < want)
      want = file->buffer_pos;

    /*buffer contains data */
    /* look for newline or eof */
    for(loop = 0; loop < want; loop++) {
      if(file->buffer[loop] == '\n') {
        want = loop + 1;/* include newline */
        break;
      }
    }

    /* xfer data to caller */
    memcpy(ptr, file->buffer, want);
    ptr[want] = 0;/* always null terminate */

    use_buffer(file, want);

    break;

  default: /* unknown or supported type - oh dear */
    ptr = NULL;
    errno = EBADF;
    break;
  }

  return ptr;/*success */
}

