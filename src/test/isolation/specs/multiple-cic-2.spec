# Test multiple CREATE INDEX CONCURRENTLY working simultaneously

setup
{
  CREATE TABLE mcic_one (
	id int
  );
}
teardown
{
  DROP TABLE mcic_one;
}

session "s1"
step "s1l"	{ SELECT pg_advisory_lock(281458); }
step "s1i"	{
		CREATE INDEX CONCURRENTLY mcic_one_pkey ON mcic_one (id)
	}
step "s1ul"	{ SELECT pg_advisory_unlock_all(); }
teardown	{ SELECT pg_advisory_unlock_all(); }


session "s2"
step "s2l"	{ SELECT pg_advisory_lock(281458); }
step "s2i"	{
		CREATE INDEX CONCURRENTLY mcic_one_pkey2 ON mcic_one (id)
	}
teardown	{ SELECT pg_advisory_unlock_all(); }


permutation "s1l" "s2l" "s1i" "s1ul" "s2i"
permutation "s1l" "s1i" "s2l" "s1ul" "s2i"
permutation "s1l" "s2i" "s1i" "s1ul"
