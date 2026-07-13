# Key-join proof dependency race against operator cascade.
#
# The selected proof uses a custom equality operator through a custom btree
# opclass, a bare unique index, and the FK that targets it.  Dropping the
# operator cascades through those proof inputs.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_operator_cascade;
	SET search_path = key_join_proof_race_operator_cascade;

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
		id integer PRIMARY KEY,
		parent_id integer NOT NULL REFERENCES parent (id)
	);
	CREATE VIEW parent_v AS SELECT id FROM parent;
	CREATE VIEW child_v AS SELECT id, parent_id FROM child;
}

teardown
{
	DROP SCHEMA key_join_proof_race_operator_cascade CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_operator_cascade, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-proof-match', 'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT p.id AS parent_key, c.id AS child_id, c.parent_id
	FROM parent_v p
	LEFT JOIN child_v c FOR KEY (parent_id) -> p (id);
}

step noop { }

session ddl
setup
{
	SET search_path = key_join_proof_race_operator_cascade;
}
step drop_operator
{
	DROP OPERATOR =# (integer, integer) CASCADE;
}

session ctl
step wakeup
{
	SELECT FROM injection_points_wakeup('key-join-after-proof-match');
}
step detach
{
	SELECT FROM injection_points_detach('key-join-after-proof-match');
}

permutation create_view(drop_operator notices 1) drop_operator(create_view) wakeup noop detach
