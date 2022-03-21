# Tests for global temporary relations

initialize
{
  create global temp table gtt_on_commit_delete_row(a bigserial primary key, b text) on commit delete rows;
  create global temp table gtt_on_commit_preserve_row(a bigserial primary key, b text) on commit preserve rows;
  create global temp table gtt_test_createindex(a int, b char(1000)) on commit preserve rows;
}

destroy
{
  /* wait other backend exit */
  select pg_sleep(1);

  DROP TABLE gtt_on_commit_delete_row;
  DROP TABLE gtt_on_commit_preserve_row;
  DROP TABLE gtt_test_createindex;
}

# Session 1
session "s1"
step "s1_begin" {begin}
step "s1_commit" {commit}
step "s1_rollback" {rollback}
step "s1_insert_d" {insert into gtt_on_commit_delete_row (b) values('test1')}
step "s1_select_d" {select a,b from gtt_on_commit_delete_row order by a,b}
step "s1_insert_p" {insert into gtt_on_commit_preserve_row (b) values('test20')}
step "s1_select_p" {select a,b from gtt_on_commit_preserve_row order by a,b}
step "s1_truncate_d" {truncate gtt_on_commit_delete_row}
step "s1_truncate_p" {truncate gtt_on_commit_preserve_row}
step "s1_lock_p" {LOCK TABLE gtt_on_commit_preserve_row in ACCESS EXCLUSIVE MODE}
step "s1_update_d" {update gtt_on_commit_delete_row set b = 'update'}
step "s1_save_1" {SAVEPOINT save1}
step "s1_save_2" {SAVEPOINT save2}
step "s1_save_3" {SAVEPOINT save3}
step "s1_rollback_to_save_2" {rollback to savepoint save2}
step "s1_reindex_p" {reindex table gtt_on_commit_preserve_row}
step "s1_reindex_i_p" {reindex index gtt_on_commit_preserve_row_pkey}
step "s1_insert_c" {insert into gtt_test_createindex(a,b) values(generate_series(1,100000),'test create index')}
step "s1_select_c" {explain (costs off) select * from gtt_test_createindex where a = 1}
step "s1_analyze_c" {analyze gtt_test_createindex}
teardown
{
  TRUNCATE gtt_on_commit_delete_row RESTART IDENTITY;
  TRUNCATE gtt_on_commit_preserve_row RESTART IDENTITY;
}

# Session 2
session "s2"
step "s2_begin" {begin}
step "s2_commit" {commit}
step "s2_rollback" {rollback}
step "s2_insert_p" {insert into gtt_on_commit_preserve_row (b) values('test10')}
step "s2_select_p" {select a,b from gtt_on_commit_preserve_row order by a,b}
step "s2_insert_d" {insert into gtt_on_commit_delete_row (b) values('test1')}
step "s2_truncate_p" {truncate gtt_on_commit_preserve_row}
step "s2_update_p" {update gtt_on_commit_preserve_row set b = 'update'}
step "s2_lock_p" {LOCK TABLE gtt_on_commit_preserve_row in ACCESS EXCLUSIVE MODE}
step "s2_save_1" {SAVEPOINT save1}
step "s2_save_2" {SAVEPOINT save2}
step "s2_save_3" {SAVEPOINT save3}
step "s2_rollback_to_save_2" {rollback to savepoint save2}
step "s2_reindex_p" {reindex table gtt_on_commit_preserve_row}
step "s2_reindex_i_p" {reindex index gtt_on_commit_preserve_row_pkey}
step "s2_insert_c" {insert into gtt_test_createindex(a,b) values(generate_series(1,100000),'test create index')}
step "s2_select_c" {explain (costs off) select * from gtt_test_createindex where a = 1}
step "s2_analyze_c" {analyze gtt_test_createindex}
teardown
{
  TRUNCATE gtt_on_commit_delete_row RESTART IDENTITY;
  TRUNCATE gtt_on_commit_preserve_row RESTART IDENTITY;
}

session "s3"
step "s3_create_c" {create unique index idx_temp_table_a on gtt_test_createindex(a)}
step "s3_insert_c" {insert into gtt_test_createindex(a,b) values(generate_series(1,100000),'test create index')}
step "s3_select_c" {explain (costs off) select * from gtt_test_createindex where a = 1}
step "s3_analyze_c" {analyze gtt_test_createindex}


#
# test on commit delete temp table
#

# test update empty temp table
permutation "s1_update_d"
# test insert into temp table
permutation "s1_select_d" "s1_insert_d" "s1_select_d"
# test temp table in transaction(commit)
permutation "s1_select_d" "s1_begin"    "s1_insert_d" "s1_select_d"   "s1_commit"   "s1_select_d" 
# test temp table in transaction(rollback)
permutation "s1_select_d" "s1_begin"    "s1_insert_d" "s1_select_d"   "s1_rollback" "s1_select_d" 
# test truncate
permutation "s1_select_d" "s1_insert_d" "s1_select_d" "s1_truncate_d" "s1_select_d"
# test truncate in transaction block
permutation "s1_select_d" "s1_insert_d" "s1_begin"    "s1_insert_d"   "s1_select_d" "s1_truncate_d" "s1_select_d"   "s1_insert_d" "s1_select_d" "s1_commit"   "s1_select_d" 
permutation "s1_select_d" "s1_insert_d" "s1_begin"    "s1_insert_d"   "s1_select_d" "s1_truncate_d" "s1_select_d"   "s1_insert_d" "s1_select_d" "s1_rollback" "s1_select_d"
# test temp table with subtransaction or savepoint
permutation "s1_insert_d" "s1_select_d" "s1_begin"    "s1_insert_d"   "s1_select_d" "s1_save_1"     "s1_truncate_d" "s1_insert_d" "s1_select_d" "s1_save_2"   "s1_truncate_d" "s1_insert_d" "s1_select_d" "s1_save_3" "s1_rollback_to_save_2" "s1_select_d" "s1_insert_d" "s1_select_d" "s1_commit" "s1_select_d"
permutation "s1_insert_d" "s1_select_d" "s1_begin"    "s1_insert_d"   "s1_select_d" "s1_save_1"     "s1_truncate_d" "s1_insert_d" "s1_select_d" "s1_save_2"   "s1_truncate_d" "s1_insert_d" "s1_select_d" "s1_save_3" "s1_rollback_to_save_2" "s1_select_d" "s1_insert_d" "s1_select_d" "s1_rollback" "s1_select_d"

#
# test on commit preserve table
#

# same as test on commit delete temp table
permutation "s2_update_p"
permutation "s2_select_p" "s2_insert_p" "s2_select_p"
permutation "s2_select_p" "s2_begin"    "s2_insert_p" "s2_select_p"   "s2_commit"   "s2_select_p"
permutation "s2_select_p" "s2_begin"    "s2_insert_p" "s2_select_p"   "s2_rollback" "s2_select_p"
permutation "s2_select_p" "s2_insert_p" "s2_select_p" "s2_truncate_p" "s2_select_p"
permutation "s2_select_p" "s2_insert_p" "s2_begin"    "s2_insert_p"   "s2_select_p" "s2_truncate_p" "s2_select_p"   "s2_insert_p" "s2_select_p" "s2_commit"   "s2_select_p" 
permutation "s2_select_p" "s2_insert_p" "s2_begin"    "s2_insert_p"   "s2_select_p" "s2_truncate_p" "s2_select_p"   "s2_insert_p" "s2_select_p" "s2_rollback" "s2_select_p" 
permutation "s2_insert_p" "s2_select_p" "s2_begin"    "s2_insert_p"   "s2_select_p" "s2_save_1"     "s2_truncate_p" "s2_insert_p" "s2_select_p" "s2_save_2"   "s2_truncate_p" "s2_insert_p" "s2_select_p" "s2_save_3" "s2_rollback_to_save_2" "s2_select_p" "s2_insert_p" "s2_select_p" "s2_commit" "s2_select_p"
permutation "s2_insert_p" "s2_select_p" "s2_begin"    "s2_insert_p"   "s2_select_p" "s2_save_1"     "s2_truncate_p" "s2_insert_p" "s2_select_p" "s2_save_2"   "s2_truncate_p" "s2_insert_p" "s2_select_p" "s2_save_3" "s2_rollback_to_save_2" "s2_select_p" "s2_insert_p" "s2_select_p" "s2_rollback" "s2_select_p"

#
# test concurrent operation on temp table
#

#  test concurrent read
permutation "s1_insert_p" "s2_insert_p" "s1_select_p" "s2_select_p" 
#  test concurrent truncate
permutation "s1_begin" "s2_begin"    "s1_insert_p" "s2_insert_p"   "s1_truncate_p" "s2_truncate_p"  "s1_commit" "s2_commit" "s1_select_p" "s2_select_p" 
permutation "s1_begin" "s1_insert_d" "s2_insert_d" "s1_truncate_d" "s2_insert_d"   "s1_commit" 
#  test concurrent reindex table
permutation "s1_begin" "s2_begin" "s1_insert_p" "s2_insert_p" "s1_reindex_p"   "s2_reindex_p"   "s1_commit" "s2_commit" "s1_select_p" "s2_select_p" 
#  test concurrent reindex index
permutation "s1_begin" "s2_begin" "s1_insert_p" "s2_insert_p" "s1_reindex_i_p" "s2_reindex_i_p" "s1_commit" "s2_commit" "s1_select_p" "s2_select_p" 

# test create index
permutation "s2_insert_c" "s3_create_c" "s3_insert_c" "s1_insert_c" "s1_analyze_c" "s2_analyze_c" "s3_analyze_c" "s1_select_c" "s2_select_c" "s3_select_c"

# test lock gtt
permutation "s1_begin" "s2_begin" "s1_lock_p" "s2_lock_p" "s1_truncate_p" "s2_truncate_p" "s1_insert_p" "s2_insert_p" "s1_commit" "s2_commit" "s1_select_p" "s2_select_p"
