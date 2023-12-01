CREATE EXTENSION test_injection_points;

SELECT test_injection_points_attach('TestInjectionBooh', 'booh');
SELECT test_injection_points_attach('TestInjectionError', 'error');
SELECT test_injection_points_attach('TestInjectionLog', 'notice');
SELECT test_injection_points_attach('TestInjectionLog2', 'notice');

SELECT test_injection_points_run('TestInjectionBooh'); -- nothing
SELECT test_injection_points_run('TestInjectionLog2'); -- notice
SELECT test_injection_points_run('TestInjectionLog'); -- notice
SELECT test_injection_points_run('TestInjectionError'); -- error

-- Re-load and run again.
\c
SELECT test_injection_points_run('TestInjectionLog2'); -- notice
SELECT test_injection_points_run('TestInjectionLog'); -- notice
SELECT test_injection_points_run('TestInjectionError'); -- error

-- Remove one entry and check the other one.
SELECT test_injection_points_detach('TestInjectionError'); -- ok
SELECT test_injection_points_run('TestInjectionLog'); -- notice
SELECT test_injection_points_run('TestInjectionError'); -- nothing
-- All entries removed, nothing happens
SELECT test_injection_points_detach('TestInjectionLog'); -- ok
SELECT test_injection_points_run('TestInjectionLog'); -- nothing
SELECT test_injection_points_run('TestInjectionError'); -- nothing
SELECT test_injection_points_run('TestInjectionLog2'); -- notice

SELECT test_injection_points_detach('TestInjectionLog'); -- fails

SELECT test_injection_points_run('TestInjectionLog2'); -- notice

DROP EXTENSION test_injection_points;
