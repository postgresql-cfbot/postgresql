CREATE EXTENSION test_dsa;

SELECT test_dsa_random(3, 5, 16, 10240, 'random');
SELECT test_dsa_random(3, 5, 16, 10240, 'forwards');
SELECT test_dsa_random(3, 5, 16, 10240, 'backwards');

SELECT count(*) from test_dsa_random_parallel(3, 5, 16, 10240, 'random', 5);
SELECT count(*) from test_dsa_random_parallel(3, 5, 16, 10240, 'forwards', 5);
SELECT count(*) from test_dsa_random_parallel(3, 5, 16, 10240, 'backwards', 5);

SELECT test_dsa_oom(1024);
SELECT test_dsa_oom(8192);
SELECT test_dsa_oom(10240);