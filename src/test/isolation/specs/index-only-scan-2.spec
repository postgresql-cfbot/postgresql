# Test the granularity of predicate locks on heap tuples, for index-only scans.

# Access the accounts through two different indexes, so that index predicate
# locks don't confuse matters; here we only want to test heap locking.  Since
# s1 and s2 access different heap tuples there is no cycle, so both
# transactions should be able to commit.  Before PostgreSQL 15, a serialization
# failure was reported because of a page-level SIREAD lock on the heap that
# produced a false conflict.

setup
{
  CREATE TABLE account (id1 int, id2 int, balance int);
  CREATE UNIQUE INDEX ON account(id1);
  CREATE UNIQUE INDEX ON account(id2);
  INSERT INTO account VALUES (1, 1, 100), (2, 2, 100);
}
setup
{
  VACUUM account;
}

teardown
{
  DROP TABLE account;
}

session "s1"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; SET enable_seqscan = off; }
step "r1" { SELECT id1 FROM account WHERE id1 = 1; }
step "w1" { UPDATE account SET balance = 200 WHERE id1 = 1; }
step "c1" { COMMIT; }

session "s2"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; SET enable_seqscan = off; }
step "r2" { SELECT id2 FROM account WHERE id2 = 2; }
step "w2" { UPDATE account SET balance = 200 WHERE id2 = 2; }
step "c2" { COMMIT; }

permutation "r1" "r2" "w1" "w2" "c1" "c2"
