# Tests for global temporary relations

initialize
{
  CREATE GLOBAL TEMPORARY TABLE if not exists gtt_with_seq(c1 bigint, c2 bigserial) on commit PRESERVE rows;
}

destroy
{
  /* wait other backend exit */
  select pg_sleep(1);

  DROP TABLE gtt_with_seq;
}

# Session 1
session "s1"
step "s1_seq_next" { select nextval('gtt_with_seq_c2_seq'); }
step "s1_seq_restart" { alter sequence gtt_with_seq_c2_seq RESTART; }
step "s1_insert" { insert into gtt_with_seq values(1); }
step "s1_select" { select * from gtt_with_seq order by c1,c2; }
teardown
{
  TRUNCATE gtt_with_seq;
}

# Session 2
session "s2"
step "s2_seq_next" { select nextval('gtt_with_seq_c2_seq'); }
step "s2_insert" { insert into gtt_with_seq values(10); }
step "s2_select" { select * from gtt_with_seq order by c1,c2; }
teardown
{
  TRUNCATE gtt_with_seq RESTART IDENTITY;
}

permutation "s1_seq_next" "s2_seq_next" "s1_seq_next"
permutation "s1_select" "s2_select" "s1_insert" "s2_insert" "s1_select" "s2_select"

