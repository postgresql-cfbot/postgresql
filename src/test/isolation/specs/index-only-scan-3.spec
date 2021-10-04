# A variant of index-only-scan-2.spec that is a true conflict, detected via
# heap tuple locks only.

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
step "r1" { SELECT id1 FROM account WHERE id1 = 2; }
step "w1" { UPDATE account SET balance = 200 WHERE id1 = 1; }
step "c1" { COMMIT; }

session "s2"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; SET enable_seqscan = off; }
step "r2" { SELECT id2 FROM account WHERE id2 = 1; }
step "w2" { UPDATE account SET balance = 200 WHERE id2 = 2; }
step "c2" { COMMIT; }

permutation "r1" "r2" "w1" "w2" "c1" "c2"
