# Key-join proof race while acquiring filter-operator proof locks.
#
# The parser pauses after copying FK relcache state, after parse analysis has
# resolved a filter operator in the current query but before key-join proof
# locks that operator.  DROP OPERATOR ... CASCADE can remove the operator
# before CREATE VIEW resumes, so proof validation must fail cleanly.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_filter_operator_prelock;
	SET search_path = key_join_proof_race_filter_operator_prelock;

	CREATE TABLE parent
	(
		id integer PRIMARY KEY
	);
	CREATE TABLE child
	(
		parent_id integer NOT NULL REFERENCES parent (id)
	);

	CREATE FUNCTION filter_eq(a integer, b bigint) RETURNS boolean
		LANGUAGE sql IMMUTABLE STRICT AS $$
		SELECT a = b::integer
		$$;
	CREATE OPERATOR =## (
		LEFTARG = integer,
		RIGHTARG = bigint,
		FUNCTION = filter_eq
	);
	CREATE OPERATOR FAMILY filter_hash_ops USING hash;
	CREATE OPERATOR CLASS filter_hash_int4_ops
		FOR TYPE integer USING hash FAMILY filter_hash_ops AS
		OPERATOR 1 = (integer, integer),
		FUNCTION 1 hashint4(integer);
	ALTER OPERATOR FAMILY filter_hash_ops USING hash ADD
		OPERATOR 1 =## (integer, bigint),
		FUNCTION 1 hashint8(bigint);
}

teardown
{
	DROP SCHEMA key_join_proof_race_filter_operator_prelock CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_filter_operator_prelock, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-fkey-list-copy', 'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT p.id AS parent_key, c.parent_id
	FROM (SELECT id FROM parent WHERE id =## 1::bigint) p
	LEFT JOIN child c FOR KEY (parent_id) -> p (id);
}

session ddl
setup
{
	SET search_path = key_join_proof_race_filter_operator_prelock;
}
step drop_operator
{
	DROP OPERATOR =## (integer, bigint) CASCADE;
}

session ctl
# Wake the first pause and detach in one step; the injection point fires once
# per proof relation, so detaching turns the second hit into a no-op.  This
# avoids two separate wakeups, which cannot be ordered reliably against the
# re-arm of the second wait (see key_join_proof_race_operator_prelock).
step release
{
	SELECT FROM injection_points_wakeup('key-join-after-fkey-list-copy');
	SELECT FROM injection_points_detach('key-join-after-fkey-list-copy');
}

permutation create_view drop_operator release
