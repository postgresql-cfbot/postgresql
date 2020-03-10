# REINDEX CONCURRENTLY with invalid indexes
#
# Check that handling of dependencies with invalid indexes is correct.
# When the parent table is dropped, all the indexes are dropped.

setup
{
	CREATE TABLE reind_con_invalid (id serial primary key, data text);
	INSERT INTO reind_con_invalid (data) VALUES ('foo');
	INSERT INTO reind_con_invalid (data) VALUES ('bar');
}

# No need to drop the table at teardown phase here as each permutation
# takes care of it internally.

session "s1"
setup { BEGIN; }
step "s1_select" { SELECT data FROM reind_con_invalid FOR UPDATE; }
step "s1_commit" { COMMIT; }
step "s1_drop"   { DROP TABLE reind_con_invalid; }

session "s2"
step "s2_timeout" { SET statement_timeout = 1000; }
step "s2_reindex" { REINDEX TABLE CONCURRENTLY reind_con_invalid; }
step "s2_check" { SELECT regexp_replace(i.indexrelid::regclass::text, '(pg_toast_)([0-9+/=]+)(_index)', '\1<oid>\3'),
					         i.indisvalid
					    FROM pg_class c
					      JOIN pg_class t ON t.oid = c.reltoastrelid
					      JOIN pg_index i ON i.indrelid = t.oid
					    WHERE c.relname = 'reind_con_invalid'
					      ORDER BY c.relname; }

# A failed REINDEX CONCURRENTLY will leave an invalid index on the toast
# table of TOAST table.  Any following successful REINDEX should leave
# this index as invalid, otherwise we would end up with a useless and
# duplicated index that can't be dropped.  Any invalid index should be
# dropped once the parent table is dropped.
permutation "s1_select" "s2_timeout" "s2_reindex" "s2_check" "s1_commit" "s1_drop" "s2_check"
