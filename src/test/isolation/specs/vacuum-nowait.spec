# Test for the NOWAIT option for VACUUM and ANALYZE commands.
#
# This also verifies that log messages are not emitted for skipped relations
# that were not specified in the VACUUM or ANALYZE command.

setup
{
	CREATE TABLE parted (a INT) PARTITION BY LIST (a);
	CREATE TABLE part1 PARTITION OF parted FOR VALUES IN (1);
	CREATE TABLE part2 PARTITION OF parted FOR VALUES IN (2);
}

teardown
{
	DROP TABLE IF EXISTS parted;
}

session "s1"
step "lock"
{
	BEGIN;
	LOCK part1 IN SHARE MODE;
}
step "commit"
{
	COMMIT;
}

session "s2"
step "vac_specified"		{ VACUUM (NOWAIT) part1, part2; }
step "vac_all_parts"		{ VACUUM (NOWAIT) parted; }
step "analyze_specified"	{ ANALYZE (NOWAIT) part1, part2; }
step "analyze_all_parts"	{ ANALYZE (NOWAIT) parted; }
step "vac_analyze_specified"	{ VACUUM (ANALYZE, NOWAIT) part1, part2; }
step "vac_analyze_all_parts"	{ VACUUM (ANALYZE, NOWAIT) parted; }

permutation "lock" "vac_specified" "commit"
permutation "lock" "vac_all_parts" "commit"
permutation "lock" "analyze_specified" "commit"
permutation "lock" "analyze_all_parts" "commit"
permutation "lock" "vac_analyze_specified" "commit"
permutation "lock" "vac_analyze_all_parts" "commit"
