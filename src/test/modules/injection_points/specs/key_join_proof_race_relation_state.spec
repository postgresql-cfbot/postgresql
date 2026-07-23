# Key-join proof dependency races against base relation state changes.
#
# Relation-level facts are valid only while row security and inheritance shape
# still expose the rows used by the proof.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_relation_state;
	CREATE TABLE key_join_proof_race_relation_state.parent
	(
		id integer PRIMARY KEY
	);
	CREATE TABLE key_join_proof_race_relation_state.child
	(
		id integer PRIMARY KEY,
		parent_id integer NOT NULL
			REFERENCES key_join_proof_race_relation_state.parent (id)
	);
	CREATE VIEW key_join_proof_race_relation_state.parent_v AS
		SELECT id FROM key_join_proof_race_relation_state.parent;
	CREATE VIEW key_join_proof_race_relation_state.child_v AS
		SELECT id, parent_id FROM key_join_proof_race_relation_state.child;
}

teardown
{
	DROP SCHEMA key_join_proof_race_relation_state CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_relation_state, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-proof-match', 'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT p.id AS parent_id, c.id AS child_id,
		   c.parent_id AS child_parent_id
	FROM parent_v p
	JOIN child_v c FOR KEY (parent_id) -> p (id);
}

step noop { }

session ddl
setup
{
	SET search_path = key_join_proof_race_relation_state;
}
step enable_rls
{
	ALTER TABLE parent ENABLE ROW LEVEL SECURITY;
}
step add_inherits
{
	CREATE TABLE parent_child () INHERITS (parent);
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

permutation create_view enable_rls(create_view) wakeup noop detach
permutation create_view add_inherits(create_view) wakeup noop detach
