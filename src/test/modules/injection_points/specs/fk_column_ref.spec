# Test that column-level REFERENCES does not trigger a false recheck failure.
#
# When a user has only column-level REFERENCES (not table-level),
# checkFkeyPermissions() calls pg_class_aclcheck() which fails then falls
# through to pg_attribute_aclcheck() which succeeds. If catalog invalidations
# arrive before dependency recording, recheckAcl() must not spuriously fail on
# the tracked failed check (it should not be tracked).

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE fk_ref_target(id int PRIMARY KEY);
	INSERT INTO fk_ref_target VALUES (1);
	CREATE TABLE produce_inval(x int);
	CREATE ROLE role_fk_tester;
	GRANT REFERENCES(id) ON fk_ref_target TO role_fk_tester;
	GRANT CREATE ON SCHEMA public TO role_fk_tester;
	GRANT SELECT ON fk_ref_target TO role_fk_tester;
	SET ROLE role_fk_tester;
	CREATE TABLE fk_source(ref_id int);
	INSERT INTO fk_source VALUES (1);
	RESET ROLE;
}

teardown
{
	DROP TABLE IF EXISTS fk_source;
	DROP TABLE IF EXISTS fk_ref_target;
	DROP TABLE IF EXISTS produce_inval;
	REVOKE CREATE ON SCHEMA public FROM role_fk_tester;
	DROP ROLE role_fk_tester;
	DROP EXTENSION injection_points;
}

session "s1"
step "s1_attach" {
	SELECT injection_points_set_local();
	SELECT injection_points_attach('checkFkeyPermissions-after-table-acl-fail', 'wait');
}
step "s1_add_fk" {
	SET ROLE role_fk_tester;
	ALTER TABLE fk_source ADD FOREIGN KEY (ref_id) REFERENCES fk_ref_target(id);
	RESET ROLE;
}

session "s2"
step "s2_invalidate_and_wakeup" {
	GRANT SELECT ON produce_inval TO role_fk_tester;
	REVOKE SELECT ON produce_inval FROM role_fk_tester;
	SELECT injection_points_wakeup('checkFkeyPermissions-after-table-acl-fail');
}

# s1 attaches the wait point, then starts the FK creation which blocks
# after the failed pg_class_aclcheck. s2 generates invalidations and
# wakes s1. The FK creation must succeed despite the invalidations.
permutation "s1_attach" "s1_add_fk" "s2_invalidate_and_wakeup"
