# This test exercises behavior of foreign keys in the face of concurrent
# detach of partitions in the referenced table.
setup {
	drop table if exists d4_primary, d4_primary1, d4_fk, d4_pid;
	create table d4_primary (a int primary key) partition by list (a);
	create table d4_primary1 partition of d4_primary for values in (1);
	insert into d4_primary values (1);
	create table d4_fk (a int references d4_primary);
	create table d4_pid (pid int);
}

session "s1"
step "s1b"		{ begin; }
step "s1brr"	{ begin isolation level repeatable read; }
step "s1s"		{ select * from d4_primary; }
step "s1cancel" { select pg_cancel_backend(pid) from d4_pid; }
step "s1insert"	{ insert into d4_fk values (1); }
step "s1c"		{ commit; }

session "s2"
step "s2snitch" { insert into d4_pid select pg_backend_pid(); }
step "s2detach" { alter table d4_primary detach partition d4_primary1 concurrently; }

session "s3"
step "s3brr"	{ begin isolation level repeatable read; }
step "s3insert" { insert into d4_fk values (1); }
step "s3commit" { commit; }

# Trying to insert into a partially detached partition is rejected
permutation "s2snitch" "s1b"   "s1s" "s2detach" "s1cancel" "s1insert" "s1c"
# ... even under REPEATABLE READ mode.
permutation "s2snitch" "s1brr" "s1s" "s2detach" "s1cancel" "s1insert" "s1c"

# These other cases are fairly trivial.
permutation "s2snitch" "s1b" "s1s" "s2detach" "s3insert" "s1c"
permutation "s2snitch" "s1b" "s1s" "s2detach" "s3brr" "s3insert" "s3commit" "s1cancel"
