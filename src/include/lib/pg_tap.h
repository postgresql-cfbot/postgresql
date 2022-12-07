/*
 * Simple macros for writing tests in C that print results in TAP format,
 * as consumed by "prove".
 *
 * Specification for the output format: https://testanything.org/
 */

#ifndef PG_TAP_H
#define PG_TAP_H

#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Counters are global, so we can break our tests into multiple functions. */
static int	pg_test_count;
static int	pg_fail_count;
static int	pg_todo_count;

/*
 * Require an expression to be true.  Used for set-up steps that are not
 * reported as a test.
 */
#define PG_REQUIRE(expr) \
if (!(expr)) { \
	printf("Bail out! requirement (" #expr ") failed at %s:%d\n", \
		__FILE__, __LINE__); \
	exit(EXIT_FAILURE); \
}

/*
 * Like PG_REQUIRE, but log strerror(errno) before bailing.
 */
#define PG_REQUIRE_SYS(expr) \
if (!(expr)) { \
	printf("Bail out! requirement (" #expr ") failed at %s:%d, error: %s\n", \
		__FILE__, __LINE__, strerror(errno)); \
	exit(EXIT_FAILURE); \
}

/*
 * Test that an expression is true.  An optional message can be provided,
 * defaulting to the expression itself if not provided.
 */
#define PG_EXPECT(expr, ...) \
do { \
	const char *messages[] = {#expr, __VA_ARGS__}; \
	const char *message = messages[lengthof(messages) - 1]; \
	pg_test_count++; \
	if (expr) { \
		printf("ok %d - %s\n", pg_test_count, message); \
	} else { \
		if (pg_todo_count == 0) \
			pg_fail_count++; \
		printf("not ok %d - %s (at %s:%d)%s\n", pg_test_count, \
			message, __FILE__, __LINE__, \
			pg_todo_count > 0 ? " # TODO" : ""); \
	} \
} while (0)

/*
 * Like PG_EXPECT(), but also log strerror(errno) on failure.
 */
#define PG_EXPECT_SYS(expr, ...) \
do { \
	const char *messages[] = {#expr, __VA_ARGS__}; \
	const char *message = messages[lengthof(messages) - 1]; \
	pg_test_count++; \
	if (expr) { \
		printf("ok %d - %s\n", pg_test_count, message); \
	} else { \
		if (pg_todo_count == 0) \
			pg_fail_count++; \
		printf("not ok %d - %s (at %s:%d), error: %s%s\n", pg_test_count, \
			message, __FILE__, __LINE__, strerror(errno), \
			pg_todo_count > 0 ? " # TODO" : ""); \
	} \
} while (0)


/*
 * Test that one int expression is equal to another, logging the values if not.
 */
#define PG_EXPECT_EQ(expr1, expr2, ...) \
do { \
	const char *messages[] = {#expr1 " == " #expr2, __VA_ARGS__}; \
	const char *message = messages[lengthof(messages) - 1]; \
	int expr1_val = (expr1); \
	int expr2_val = (expr2); \
	pg_test_count++; \
	if (expr1_val == expr2_val) { \
		printf("ok %d - %s\n", pg_test_count, message); \
	} else { \
		if (pg_todo_count == 0) \
			pg_fail_count++; \
		printf("not ok %d - %s: %d != %d (at %s:%d)%s\n", pg_test_count, \
			message, expr1_val, expr2_val, __FILE__, __LINE__, \
			pg_todo_count > 0 ? " # TODO" : ""); \
	} \
} while (0)

/*
 * Test that one C string expression is equal to another, logging the values if
 * not.
 */
#define PG_EXPECT_EQ_STR(expr1, expr2, ...) \
do { \
	const char *messages[] = {#expr1 " matches " #expr2, __VA_ARGS__}; \
	const char *message = messages[lengthof(messages) - 1]; \
	const char *expr1_val = (expr1); \
	const char *expr2_val = (expr2); \
	pg_test_count++; \
	if (strcmp(expr1_val, expr2_val) == 0) { \
		printf("ok %d - %s\n", pg_test_count, message); \
	} else { \
		if (pg_todo_count == 0) \
			pg_fail_count++; \
		printf("not ok %d - %s: \"%s\" vs \"%s\" (at %s:%d) %s\n", \
			pg_test_count, \
			message, expr1_val, expr2_val, __FILE__, __LINE__, \
			pg_todo_count > 0 ? " # TODO" : ""); \
	} \
} while (0)

/*
 * The main function of a test program should begin and end with these
 * functions.
 */
#define PG_BEGIN_TESTS() \
	setbuf(stdout, NULL);		/* disable buffering for Windows */

#define PG_END_TESTS() printf("1..%d\n", pg_test_count); \
	return EXIT_SUCCESS

#define PG_BEGIN_TODO() pg_todo_count++
#define PG_END_TODO() pg_todo_count--

#endif
