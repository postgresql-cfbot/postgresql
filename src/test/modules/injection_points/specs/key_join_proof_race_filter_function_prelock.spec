# Key-join proof race while acquiring filter-function proof locks.
#
# The parser pauses after collecting proof-filter dependencies, after parse
# analysis has resolved a filter value function in the current query but before
# key-join proof locks that function.  DROP FUNCTION can remove the function
# before CREATE VIEW resumes, so proof validation must fail cleanly.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_filter_function_prelock;
	SET search_path = key_join_proof_race_filter_function_prelock;

	CREATE TABLE parent
	(
		id integer PRIMARY KEY
	);
	CREATE TABLE child
	(
		parent_id integer NOT NULL REFERENCES parent (id)
	);

	CREATE FUNCTION filter_value() RETURNS integer
		LANGUAGE sql IMMUTABLE AS $$
		SELECT 1
		$$;
}

teardown
{
	DROP SCHEMA key_join_proof_race_filter_function_prelock CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_filter_function_prelock, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-before-filter-dependency-lock',
										'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT p.id AS parent_key, c.parent_id
	FROM (SELECT id FROM parent WHERE id = filter_value()) p
	LEFT JOIN child c FOR KEY (parent_id) -> p (id);
}

step noop { }

session ddl
setup
{
	SET search_path = key_join_proof_race_filter_function_prelock;
}
step drop_function
{
	DROP FUNCTION filter_value();
}

session ctl
step wakeup
{
	SELECT FROM injection_points_wakeup('key-join-before-filter-dependency-lock');
}
step detach
{
	SELECT FROM injection_points_detach('key-join-before-filter-dependency-lock');
}

permutation create_view drop_function wakeup noop detach
