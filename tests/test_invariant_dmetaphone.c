#include <check.h>
#include <stdlib.h>
#include <string.h>
#include "contrib/fuzzystrmatch/dmetaphone.h"

START_TEST(test_dmetaphone_buffer_safety)
{
    // Invariant: Buffer reads never exceed the declared length
    const char *payloads[] = {
        "A",  // Valid input (boundary case)
        "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ",  // 52 chars (exceeds typical buffer)
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",  // 100 chars (extreme case)
        "1234567890!@#$%^&*()",  // Mixed characters
        NULL  // Edge case
    };
    int num_payloads = sizeof(payloads) / sizeof(payloads[0]) - 1;  // Exclude NULL

    for (int i = 0; i < num_payloads; i++) {
        char result[6];  // dmetaphone typically returns up to 4 chars + null terminator
        memset(result, 0, sizeof(result));
        
        // Direct call to production function
        dmetaphone(payloads[i], result);
        
        // Verify result is properly null-terminated and within bounds
        ck_assert_msg(strlen(result) < sizeof(result), 
                     "Buffer overflow detected for input: %s", payloads[i]);
        ck_assert_msg(result[sizeof(result)-1] == '\0', 
                     "Missing null terminator for input: %s", payloads[i]);
    }
}
END_TEST

Suite *security_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s = suite_create("Security");
    tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_dmetaphone_buffer_safety);
    suite_add_tcase(s, tc_core);

    return s;
}

int main(void)
{
    int number_failed;
    Suite *s;
    SRunner *sr;

    s = security_suite();
    sr = srunner_create(s);

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}