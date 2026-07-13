# Key-join proof dependency race against a bare unique index drop.
#
# The producer view parent_child_v already contains one key join.  The
# consumer view has a single key join whose proof of parent_child_v.parent_id
# depends on the bare unique index on child(parent_id), while the foreign key
# proof input is backed by parent_pkey.  DROP INDEX CONCURRENTLY must not pass
# through the dependency gap before the consumer dependency is visible.

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA key_join_proof_race_indexes;
	CREATE TABLE key_join_proof_race_indexes.parent
	(
		id integer PRIMARY KEY
	);
	CREATE TABLE key_join_proof_race_indexes.child
	(
		id integer PRIMARY KEY,
		parent_id integer NOT NULL
			REFERENCES key_join_proof_race_indexes.parent (id)
	);
	CREATE UNIQUE INDEX child_parent_id_bare_idx
		ON key_join_proof_race_indexes.child (parent_id);
	CREATE TABLE key_join_proof_race_indexes.grandchild
	(
		id integer PRIMARY KEY,
		parent_id integer NOT NULL
			REFERENCES key_join_proof_race_indexes.parent (id)
	);
	CREATE VIEW key_join_proof_race_indexes.parent_v AS
		SELECT id FROM key_join_proof_race_indexes.parent;
	CREATE VIEW key_join_proof_race_indexes.child_v AS
		SELECT id, parent_id FROM key_join_proof_race_indexes.child;
	CREATE VIEW key_join_proof_race_indexes.grandchild_v AS
		SELECT id, parent_id FROM key_join_proof_race_indexes.grandchild;
	CREATE VIEW key_join_proof_race_indexes.parent_child_v AS
		SELECT p.id AS parent_id, c.id AS child_id,
			   c.parent_id AS child_parent_id
		FROM key_join_proof_race_indexes.parent_v p
		LEFT JOIN key_join_proof_race_indexes.child_v c
			FOR KEY (parent_id) -> p (id);
}

teardown
{
	DROP SCHEMA key_join_proof_race_indexes CASCADE;
	DROP EXTENSION injection_points;
}

session creator
setup
{
	SET search_path = key_join_proof_race_indexes, public;
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('key-join-after-proof-match', 'wait');
}
step create_view
{
	CREATE VIEW consumer AS
	SELECT pc.parent_id, pc.child_id, g.id AS grandchild_id
	FROM parent_child_v pc
	JOIN grandchild_v g FOR KEY (parent_id) -> pc (parent_id);
}

step noop { }

session ddl
setup
{
	SET search_path = key_join_proof_race_indexes;
}
step drop_idx
{
	DROP INDEX CONCURRENTLY child_parent_id_bare_idx;
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

permutation create_view drop_idx(create_view) wakeup noop detach
