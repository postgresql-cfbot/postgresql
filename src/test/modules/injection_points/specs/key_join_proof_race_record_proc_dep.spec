# Key-join proof race while recording an ordinary function dependency.
#
# The parser pauses after accepting the key-join proof.  At that point the
# left subquery's ordinary function reference has already been resolved, but
# CREATE VIEW has not yet recorded the rewrite rule's dependencies.  Dropping
# that function before CREATE VIEW resumes must fail cleanly when dependency
# recording tries to lock the stale function OID.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_record_proc_dep;
	SET search_path = key_join_proof_race_record_proc_dep;

	CREATE TABLE parent
	(
		id integer PRIMARY KEY
	);
	CREATE TABLE child
	(
		parent_id integer NOT NULL REFERENCES parent (id)
	);

	CREATE FUNCTION ordinary_value() RETURNS integer
		LANGUAGE sql IMMUTABLE AS $$
		SELECT 1
	$$;
}

teardown
{
	DROP SCHEMA key_join_proof_race_record_proc_dep CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_record_proc_dep, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-proof-match', 'wait');
}
step create_view
{
	DO $$
	BEGIN
		EXECUTE $view$
			CREATE VIEW consumer AS
			SELECT p.id AS parent_key, p.marker, c.parent_id
			FROM (SELECT id, ordinary_value() AS marker FROM parent) p
			LEFT JOIN child c FOR KEY (parent_id) -> p (id)
		$view$;
	EXCEPTION WHEN undefined_object THEN
		RAISE NOTICE 'function dependency disappeared before recording';
	END
	$$;
}

session ddl
setup
{
	SET search_path = key_join_proof_race_record_proc_dep;
}
step ddl_begin { BEGIN; }
step drop_function
{
	DROP FUNCTION ordinary_value();
}
step ddl_commit { COMMIT; }

session ctl
step wakeup
{
	SELECT FROM injection_points_wakeup('key-join-after-proof-match');
}
step detach { SELECT FROM injection_points_detach('key-join-after-proof-match'); }

permutation create_view ddl_begin drop_function wakeup ddl_commit detach
