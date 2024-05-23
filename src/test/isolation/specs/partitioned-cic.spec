# Test the ability to drop/detach partitions while CREATE INDEX CONCURRENTLY is running.
# To achieve this, start a transaction that will pause CIC in progress by
# locking a partition in row exclusive mode, giving us a change to drop/detach another partition.
# Dropping/detaching is tested for each partition to test two scenarios:
# when the partition has already been indexed and when it's yet to be indexed.

setup {
  create table cictab(i int, j int) partition by range(i);
  create table cictab_part_1 partition of cictab for values from (0) to (10);
  create table cictab_part_2 partition of cictab for values from (10) to (20);

  insert into cictab values (1, 0), (11, 0);
}

teardown {
    drop table if exists cictab_part_1;
    drop table if exists cictab_part_2;
    drop table cictab;
}

session s1
setup {BEGIN;}
step lock_p1 { lock cictab_part_1 in row exclusive mode; }
step lock_p2 { lock cictab_part_2 in row exclusive mode; }
step commit { COMMIT; }

session s2
step cic { CREATE INDEX CONCURRENTLY ON cictab(i); }
step chk_content {
  set enable_seqscan to off;
  explain (costs off) select * from cictab where i > 0;
  select * from cictab where i > 0;
}

step chk_content_part1 {
  set enable_seqscan to off;
  explain (costs off) select * from cictab_part_1 where i > 0;
  select * from cictab_part_1 where i > 0;
}

step chk_content_part2 {
  set enable_seqscan to off;
  explain (costs off) select * from cictab_part_2 where i > 0;
  select * from cictab_part_2 where i > 0;
}

session s3
step detach1 { ALTER TABLE cictab DETACH PARTITION cictab_part_1; }
step detach2 { ALTER TABLE cictab DETACH PARTITION cictab_part_2; }
step drop1 { DROP TABLE cictab_part_1; }
step drop2 { DROP TABLE cictab_part_2; }
step insert { insert into cictab values (1, 1), (11, 1); }

permutation lock_p1 cic insert drop2 commit chk_content
permutation lock_p2 cic insert drop1 commit chk_content
permutation lock_p1 cic insert detach2 commit chk_content chk_content_part2
permutation lock_p2 cic insert detach1 commit chk_content chk_content_part1
