CREATE EXTENSION injection_points;

SELECT injection_points_set_local();

CREATE SCHEMA injection_point_key_join;
CREATE TABLE injection_point_key_join.parent
(
    id int PRIMARY KEY,
    visible text
);
CREATE TABLE injection_point_key_join.child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL
        REFERENCES injection_point_key_join.parent (id),
    visible text
);

INSERT INTO injection_point_key_join.parent VALUES (1, 'parent-one');
INSERT INTO injection_point_key_join.child VALUES (10, 1, 'child-one');

-- Populate the relcache FK list before attaching the injection point.
SELECT p.visible AS parent_visible, c.visible AS child_visible
FROM injection_point_key_join.parent p
JOIN injection_point_key_join.child c FOR KEY (parent_id) -> p (id);

SELECT injection_points_attach('key-join-after-fkey-list-copy',
                               'invalidate_system_caches');

SELECT p.visible AS parent_visible, c.visible AS child_visible
FROM injection_point_key_join.parent p
JOIN injection_point_key_join.child c FOR KEY (parent_id) -> p (id);

SELECT injection_points_detach('key-join-after-fkey-list-copy');

DROP SCHEMA injection_point_key_join CASCADE;
DROP EXTENSION injection_points;
