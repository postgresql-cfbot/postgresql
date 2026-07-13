# Plain non-key-join queries must not compute key-join FK facts.
#
# Before key-join facts became demand-driven, SELECTing from the child table
# tried to inspect its FK and took AccessShareLock on the referenced parent.

setup
{
 CREATE SCHEMA key_join_lazy_facts_lock;
 CREATE TABLE key_join_lazy_facts_lock.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_lazy_facts_lock.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL REFERENCES key_join_lazy_facts_lock.p (id)
 );
 INSERT INTO key_join_lazy_facts_lock.p VALUES (1);
 INSERT INTO key_join_lazy_facts_lock.c VALUES (10, 1);
}

teardown
{
 DROP SCHEMA key_join_lazy_facts_lock CASCADE;
}

session s1
step s1_lock { BEGIN; LOCK TABLE key_join_lazy_facts_lock.p IN ACCESS EXCLUSIVE MODE; }
step s1_commit { COMMIT; }

session s2
step s2_select_child { SELECT id, parent_id FROM key_join_lazy_facts_lock.c; }

permutation s1_lock s2_select_child s1_commit
