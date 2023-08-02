/*
 * A set of macros for creating simple unit tests.
 */
#ifndef FILTER_UNITTESTINTERNAL_H
#define FILTER_UNITTESTINTERNAL_H

#include <stdio.h>
#include <stdlib.h>
extern void MemoryContextInit(void);

#define TEST_DIR "/tmp/pgtest/"

#define BEGIN do {
#define END   } while (0)

// static const char *expectFmt = "Expected '%s' but got '%s'";
#define expectFmt "Expected '%s' but got '%s'"

/* Is an integer expression signed? Note _Generic is a C11 feature.  Define as "false" if c99. */
#define isSigned(x) _Generic(x,  \
     char: true,                 \
	 int: true,                     \
	 long: true,                    \
	 long long: true,               \
	 float: (void)0,                   \
	 double: (void)0,                  \
	 default: false)


/* Verify two scaler values are equal */
#define PG_ASSERT_EQ(a,b)                                                                                              \
   BEGIN                                                                                                               \
       uint64_t _a=a; uint64_t _b = b;                                                                                 \
       char _bufa[16], _bufb[16];                                                                                      \
       if ( _a != _b || (isSigned(a) && (int64_t)_a < 0) != (isSigned(b) && (int64_t)_b < 0))                          \
           PG_ASSERT_FMT(expectFmt, PG_INT_TO_STR(a, _a, _bufa), PG_INT_TO_STR(b, _b, _bufb));                         \
   END


/* Verify two strings are equal */
#define PG_ASSERT_EQ_STR(stra, strb)                                                                                   \
    BEGIN                                                                                                              \
        if (strcmp(stra, strb) != 0)                                                                                   \
            PG_ASSERT_FMT(expectFmt, stra, strb);                                                                      \
    END


/*
 * Format a signed/unsigned integer as a string.
 * Since we only want to evalate an expression once, accepts both expression (for type) and value.
 */
#define PG_INT_TO_STR(expr, val, buf)                                                                                      \
    isSigned(expr)? (snprintf(buf, sizeof(buf), "%lld", (long long)(val)), buf)                                     \
           : (snprintf(buf, sizeof(buf), "%llu",   (unsigned long long)(val)), buf)


#define PG_ASSERT_ERRNO(_expectedErrno) \
    BEGIN                             \
		PG_ASSERT_EQ(_expectedErrno, errno) ; \
    END

/* Display a formatted message and exit */
#define PG_ASSERT_FMT(fmt, ...)                                                                                        \
    BEGIN                                                                                                              \
        char _buf[256];                                                                                                \
        snprintf(_buf, sizeof(_buf), fmt, __VA_ARGS__);                                                                \
        PG_ASSERT_MSG(_buf);                                                                                           \
    END

/* Display an unformatted message and exit */
#define PG_ASSERT_MSG(msg)                                                                                             \
    (fprintf(stderr, "FAILED: %s (%s:%d) %s\n", __func__, __FILE__, __LINE__, msg ), abort())

/* Verify the expression is true */
#define PG_ASSERT(expr)                                                                                                \
    BEGIN                                                                                                              \
        if (!(expr))                                                                                                   \
            PG_ASSERT_MSG("'" #expr "' is false");                                                                     \
    END

static inline void beginTestGroup(char *name) {fprintf(stderr, "Begin Testgroup %s\n", name);}
static inline void beginTest(char *name) {fprintf(stderr, "    Test %s\n", name);}

#endif //FILTER_UNITTESTINTERNAL_H
