# Key-join proof race while acquiring equality-operator proof locks.
#
# The parser pauses after copying FK relcache state but before locking the FK
# equality operator.  DROP OPERATOR ... CASCADE can take the operator deletion
# lock first, then wait for relation locks held by the in-flight CREATE VIEW.
# When CREATE VIEW resumes, it blocks on the operator lock; deadlock detection
# resolves the race.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_operator_prelock;
	SET search_path = key_join_proof_race_operator_prelock;

	CREATE FUNCTION bucket_cmp(a integer, b integer) RETURNS integer
		LANGUAGE sql IMMUTABLE STRICT AS $$
		SELECT CASE WHEN a / 10 < b / 10 THEN -1
					WHEN a / 10 > b / 10 THEN 1
					ELSE 0 END
		$$;
	CREATE FUNCTION bucket_eq(a integer, b integer) RETURNS boolean
		LANGUAGE sql IMMUTABLE STRICT AS $$
		SELECT a / 10 = b / 10
		$$;
	CREATE OPERATOR =# (
		LEFTARG = integer,
		RIGHTARG = integer,
		FUNCTION = bucket_eq,
		COMMUTATOR = =#,
		RESTRICT = eqsel,
		JOIN = eqjoinsel,
		MERGES
	);
	CREATE OPERATOR CLASS bucket_int4_ops
		FOR TYPE integer USING btree AS
		OPERATOR 3 =# (integer, integer),
		FUNCTION 1 bucket_cmp(integer, integer);

	CREATE TABLE parent
	(
		id integer NOT NULL
	);
	CREATE UNIQUE INDEX parent_bucket_idx
		ON parent USING btree (id bucket_int4_ops);
	CREATE TABLE child
	(
		parent_id integer NOT NULL REFERENCES parent (id)
	);
	CREATE VIEW parent_v AS SELECT id FROM parent;
	CREATE VIEW child_v AS SELECT parent_id FROM child;
}

teardown
{
	DROP SCHEMA key_join_proof_race_operator_prelock CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_operator_prelock, public;
	/*
	 * The creator closes the deadlock cycle after wakeup.  Give it
	 * the shorter timeout so victim selection is deterministic.
	 */
	SET deadlock_timeout = '100ms';
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-fkey-list-copy', 'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT p.id AS parent_key, c.parent_id
	FROM parent_v p
	LEFT JOIN child_v c FOR KEY (parent_id) -> p (id);
}

session ddl
setup
{
	SET search_path = key_join_proof_race_operator_prelock;
	/* The DDL session must not win the deadlock-detection race. */
	SET deadlock_timeout = '10s';
}
step drop_operator
{
	DROP OPERATOR =# (integer, integer) CASCADE;
}

session ctl
step release
{
	SELECT FROM injection_points_wakeup('key-join-after-fkey-list-copy');
	SELECT FROM injection_points_detach('key-join-after-fkey-list-copy');
}

permutation create_view(drop_operator) drop_operator release
