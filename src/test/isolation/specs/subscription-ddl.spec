# Test sequence usage and concurrent sequence DDL

setup
{
    CREATE SUBSCRIPTION sub1 CONNECTION 'dbname=postgres' PUBLICATION pub1 WITH(connect=false, create_slot=false, slot_name = NONE);
    CREATE ROLE testsuperuser WITH SUPERUSER;
}

teardown
{
    DROP SUBSCRIPTION IF EXISTS sub1;
    DROP SUBSCRIPTION IF EXISTS sub2;
    DROP SUBSCRIPTION IF EXISTS sub3;
    DROP ROLE testsuperuser;
}

session s1
setup             { BEGIN; }
step s1alter      { ALTER SUBSCRIPTION sub1 DISABLE; }
step s1drop       { DROP SUBSCRIPTION sub1; }
step s1rename     { ALTER SUBSCRIPTION sub1 RENAME TO sub2; }
step s1owner      { ALTER SUBSCRIPTION sub1 OWNER TO testsuperuser; }
step s1recreate   { CREATE SUBSCRIPTION sub1 CONNECTION 'dbname=postgres' PUBLICATION pub1 WITH(connect=false, create_slot=false, slot_name = NONE); }
step s1commit     { COMMIT; }

session s2
step s2alter    { ALTER SUBSCRIPTION sub1 DISABLE; }
step s2drop     { DROP SUBSCRIPTION sub1; }
step s2rename   { ALTER SUBSCRIPTION sub1 RENAME TO sub3; }
step s2owner    { ALTER SUBSCRIPTION sub1 OWNER TO testsuperuser; }


permutation s1alter s2alter s1commit
permutation s1owner s2alter s1commit
permutation s1drop s2alter s1commit
permutation s1rename s2alter s1commit
permutation s1drop s1recreate s2alter s1commit
permutation s1rename s1recreate s2alter s1commit

permutation s1alter s2drop s1commit
permutation s1owner s2drop s1commit
permutation s1drop s2drop s1commit
permutation s1rename s2drop s1commit
permutation s1drop s1recreate s2drop s1commit
permutation s1rename s1recreate s2drop s1commit

permutation s1alter s2rename s1commit
permutation s1owner s2rename s1commit
permutation s1drop s2rename s1commit
permutation s1rename s2rename s1commit
permutation s1drop s1recreate s2rename s1commit
permutation s1rename s1recreate s2rename s1commit

permutation s1alter s2owner s1commit
permutation s1owner s2owner s1commit
permutation s1drop s2owner s1commit
permutation s1rename s2owner s1commit
permutation s1drop s1recreate s2owner s1commit
permutation s1rename s1recreate s2owner s1commit
