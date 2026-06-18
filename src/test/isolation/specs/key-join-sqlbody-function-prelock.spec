# SQL functions with stored key-join proofs must still serialize with
# ALTER FUNCTION on their proof dependencies.
#
# f() has a stored SQL body containing a FOR KEY proof and depends on g().
# Session s1 holds an ALTER FUNCTION lock on f().  Altering g() in session s2
# must wait for f(), because the stored proof in f() needs revalidation.

setup
{
 CREATE SCHEMA key_join_sqlbody_function_prelock;
 CREATE TABLE key_join_sqlbody_function_prelock.p
 (
     region int NOT NULL,
     id int NOT NULL,
     PRIMARY KEY (region, id)
 );
 CREATE TABLE key_join_sqlbody_function_prelock.c
 (
     cid int PRIMARY KEY,
     region int NOT NULL,
     parent_id int NOT NULL,
     FOREIGN KEY (region, parent_id)
         REFERENCES key_join_sqlbody_function_prelock.p (region, id)
 );
 INSERT INTO key_join_sqlbody_function_prelock.p VALUES (1, 1), (2, 1);
 INSERT INTO key_join_sqlbody_function_prelock.c VALUES (10, 1, 1), (20, 2, 1);
 CREATE FUNCTION key_join_sqlbody_function_prelock.g() RETURNS int
     LANGUAGE sql STABLE RETURN 1;
 CREATE FUNCTION key_join_sqlbody_function_prelock.f()
 RETURNS TABLE(cid int)
 LANGUAGE sql
 BEGIN ATOMIC
     SELECT c.cid
     FROM (SELECT * FROM key_join_sqlbody_function_prelock.p
           WHERE region = key_join_sqlbody_function_prelock.g()) p
     JOIN (SELECT * FROM key_join_sqlbody_function_prelock.c
           WHERE region = key_join_sqlbody_function_prelock.g()) c
         FOR KEY (region, parent_id) -> p (region, id);
 END;
}

teardown
{
 DROP SCHEMA key_join_sqlbody_function_prelock CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_alter_f { ALTER FUNCTION key_join_sqlbody_function_prelock.f() COST 11; }
step s1_commit { COMMIT; }

session s2
step s2_alter_g { ALTER FUNCTION key_join_sqlbody_function_prelock.g() COST 12; }

permutation s1_begin s1_alter_f s2_alter_g s1_commit
