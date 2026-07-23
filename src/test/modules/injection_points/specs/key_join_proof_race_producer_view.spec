# Key-join proof dependency race against a transitive producer view change.
#
# The consumer reads outer_parent_v, whose key-join facts are projected through
# inner_parent_v.  Replacing inner_parent_v with a filtered definition destroys
# the row-coverage proof that the consumer selected.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_producer_view;
	CREATE TABLE key_join_proof_race_producer_view.parent
	(
		id integer PRIMARY KEY,
		visible boolean NOT NULL
	);
	CREATE TABLE key_join_proof_race_producer_view.child
	(
		id integer PRIMARY KEY,
		parent_id integer NOT NULL
			REFERENCES key_join_proof_race_producer_view.parent (id)
	);
	CREATE VIEW key_join_proof_race_producer_view.inner_parent_v AS
		SELECT id, visible FROM key_join_proof_race_producer_view.parent;
	CREATE VIEW key_join_proof_race_producer_view.outer_parent_v AS
		SELECT id FROM key_join_proof_race_producer_view.inner_parent_v;
	CREATE VIEW key_join_proof_race_producer_view.child_v AS
		SELECT id, parent_id FROM key_join_proof_race_producer_view.child;
}

teardown
{
	DROP SCHEMA key_join_proof_race_producer_view CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_producer_view, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-proof-match', 'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT p.id AS parent_id, c.id AS child_id,
		   c.parent_id AS child_parent_id
	FROM outer_parent_v p
	JOIN child_v c FOR KEY (parent_id) -> p (id);
}

step noop { }

session ddl
setup
{
	SET search_path = key_join_proof_race_producer_view;
}
step replace_inner
{
	CREATE OR REPLACE VIEW inner_parent_v AS
	SELECT id, visible FROM parent WHERE visible;
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

permutation create_view replace_inner(create_view) wakeup noop detach
