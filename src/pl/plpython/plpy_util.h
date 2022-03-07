/*--------------------------
 * common utility functions
 *--------------------------
 */

#ifndef PLPY_UTIL_H
#define PLPY_UTIL_H

#include "plpython.h"

extern PGDLLEXPORT PyObject *PLyUnicode_Bytes(PyObject *unicode);
extern PGDLLEXPORT char *PLyUnicode_AsString(PyObject *unicode);

#if PY_MAJOR_VERSION >= 3
extern PGDLLEXPORT PyObject *PLyUnicode_FromString(const char *s);
extern PGDLLEXPORT PyObject *PLyUnicode_FromStringAndSize(const char *s, Py_ssize_t size);
#endif

#endif							/* PLPY_UTIL_H */
