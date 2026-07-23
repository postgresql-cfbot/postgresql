# Key-join proof dependency races against constraint changes.
#
# CREATE VIEW pauses after selecting a key-join proof whose fact dependencies
# are already locked.  Concurrent DDL then tries to remove one catalog fact
# used by that proof and must wait behind dependency creation.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_constraints;
	CREATE TABLE key_join_proof_race_constraints.parent
	(
		id integer CONSTRAINT parent_pkey PRIMARY KEY
	);
	CREATE TABLE key_join_proof_race_constraints.child
	(
		id integer PRIMARY KEY,
		parent_id integer CONSTRAINT child_parent_id_not_null NOT NULL,
		CONSTRAINT child_parent_id_fkey
			FOREIGN KEY (parent_id)
			REFERENCES key_join_proof_race_constraints.parent (id)
	);
	CREATE VIEW key_join_proof_race_constraints.parent_v AS
		SELECT id FROM key_join_proof_race_constraints.parent;
	CREATE VIEW key_join_proof_race_constraints.child_v AS
		SELECT id, parent_id FROM key_join_proof_race_constraints.child;
}

teardown
{
	DROP SCHEMA key_join_proof_race_constraints CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_constraints, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-proof-match', 'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT p.id AS parent_id, c.id AS child_id,
		   c.parent_id AS child_parent_id
	FROM parent_v p
	LEFT JOIN child_v c FOR KEY (parent_id) -> p (id);
}

step noop { }

session ddl
setup
{
	SET search_path = key_join_proof_race_constraints;
}
step drop_fk
{
	ALTER TABLE child DROP CONSTRAINT child_parent_id_fkey;
}
step drop_not_null
{
	ALTER TABLE child ALTER COLUMN parent_id DROP NOT NULL;
}
step drop_pk_cascade
{
	ALTER TABLE parent DROP CONSTRAINT parent_pkey CASCADE;
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

permutation create_view drop_fk(create_view) wakeup noop detach
permutation create_view drop_not_null(create_view) wakeup noop detach
permutation create_view(drop_pk_cascade notices 1) drop_pk_cascade(create_view) wakeup noop detach
