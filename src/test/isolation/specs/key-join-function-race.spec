# ALTER FUNCTION must serialize with key-join proof capture.
#
# The consumer view proves its key join from matched filters in v_p and
# v_c.  If ALTER FUNCTION changes f() to VOLATILE while CREATE VIEW is
# proving those filters, CREATE VIEW must either finish first and be seen
# by the ALTER FUNCTION revalidation pass, or wait for the function lock and
# reject the proof after observing the changed function.

setup
{
 CREATE SCHEMA key_join_function_race;
 CREATE TABLE key_join_function_race.p
 (
     region int NOT NULL,
     id int NOT NULL,
     PRIMARY KEY (region, id)
 );
 CREATE TABLE key_join_function_race.c
 (
     cid int PRIMARY KEY,
     region int NOT NULL,
     parent_id int NOT NULL,
     FOREIGN KEY (region, parent_id)
         REFERENCES key_join_function_race.p (region, id)
 );
 INSERT INTO key_join_function_race.p VALUES (1, 1), (2, 1);
 INSERT INTO key_join_function_race.c VALUES (10, 1, 1), (20, 2, 1);
 CREATE FUNCTION key_join_function_race.f() RETURNS int
     LANGUAGE sql STABLE AS 'SELECT 1';
 CREATE FUNCTION key_join_function_race.g() RETURNS int
     LANGUAGE sql STABLE AS 'SELECT 1';
 CREATE FUNCTION key_join_function_race.h() RETURNS int
     LANGUAGE sql STABLE AS 'SELECT 1';
 CREATE VIEW key_join_function_race.v_p AS
     SELECT * FROM key_join_function_race.p
     WHERE region = key_join_function_race.f();
 CREATE VIEW key_join_function_race.v_c AS
     SELECT * FROM key_join_function_race.c
     WHERE region = key_join_function_race.f();
}

teardown
{
 DROP SCHEMA key_join_function_race CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_alter
{
 CREATE OR REPLACE FUNCTION key_join_function_race.f() RETURNS int
     LANGUAGE sql VOLATILE AS 'SELECT 1';
}
step s1_lock_g { ALTER FUNCTION key_join_function_race.g() COST 1; }
step s1_drop_g { DROP FUNCTION key_join_function_race.g(); }
step s1_lock_h { ALTER FUNCTION key_join_function_race.h() COST 1; }
step s1_drop_h { DROP FUNCTION key_join_function_race.h(); }
step s1_commit { COMMIT; }

session s2
step s2_create_view
{
 CREATE VIEW key_join_function_race.v AS
 SELECT v_c.cid, v_p.id
 FROM key_join_function_race.v_p
 JOIN key_join_function_race.v_c
     FOR KEY (region, parent_id) -> v_p (region, id);
}
step s2_replace_g
{
 CREATE OR REPLACE FUNCTION key_join_function_race.g() RETURNS int
     LANGUAGE sql STABLE AS 'SELECT 2';
}
step s2_alter_h { ALTER FUNCTION key_join_function_race.h() COST 2; }

permutation s1_begin s1_alter s2_create_view s1_commit
permutation s1_begin s1_lock_g s2_replace_g s1_drop_g s1_commit
permutation s1_begin s1_lock_h s2_alter_h s1_drop_h s1_commit
