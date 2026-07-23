# Key-join revalidation races against a dropped SQL-body function.
#
# Constraint changes revalidate stored key-join proofs after scanning
# pg_depend.  If a dependent SQL function disappears before its pg_proc row is
# looked up, the stale dependency should be ignored instead of reaching the old
# assertion path.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_stale_function_dependency;
	SET search_path = key_join_stale_function_dependency;

	CREATE TABLE p
	(
		id integer PRIMARY KEY
	);
	CREATE TABLE c
	(
		id integer PRIMARY KEY,
		parent_id integer CONSTRAINT c_parent_id_not_null NOT NULL,
		CONSTRAINT c_parent_id_fkey
			FOREIGN KEY (parent_id)
			REFERENCES p (id)
	);

	CREATE FUNCTION dep() RETURNS TABLE(pid integer, cid integer)
		LANGUAGE sql
		BEGIN ATOMIC
			SELECT p.id, c.id
			FROM p
			JOIN c FOR KEY (parent_id) -> p (id);
		END;
}

teardown
{
	DROP SCHEMA key_join_stale_function_dependency CASCADE;
	DROP EXTENSION injection_points;
}

session alterer
setup
{
	SET search_path = key_join_stale_function_dependency, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-before-dependent-function-lookup',
										'wait');
}
step alter_fk
{
	ALTER TABLE c ALTER CONSTRAINT c_parent_id_fkey NOT ENFORCED;
}

step noop { }

session ddl
setup
{
	SET search_path = key_join_stale_function_dependency;
}
step drop_function
{
	DROP FUNCTION dep();
}

session ctl
step wakeup
{
	SELECT FROM injection_points_wakeup('key-join-before-dependent-function-lookup');
}
step detach
{
	SELECT FROM injection_points_detach('key-join-before-dependent-function-lookup');
}

permutation alter_fk drop_function(alter_fk) wakeup noop detach
