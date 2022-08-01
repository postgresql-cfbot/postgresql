setup
{   CREATE EXTENSION pgrowlocks;

    DROP TABLE IF EXISTS t1;
    CREATE TABLE t1(c1 integer);

    INSERT INTO t1 (c1) VALUES (1);
}

teardown
{
    DROP TABLE t1;
}

session "s1"

step "s1b" { BEGIN; }
step "s1u1" { UPDATE t1 SET c1 = 2 WHERE c1 = 1; }
step "s1c" { COMMIT; }

session "s2"

step "s2_rowlocks" { select locked_row, multi, modes from pgrowlocks('t1'); }


permutation "s1b" "s1u1" "s2_rowlocks" "s1c" "s2_rowlocks"

