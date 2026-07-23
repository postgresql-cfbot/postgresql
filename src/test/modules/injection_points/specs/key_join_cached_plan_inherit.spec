# Key-join proof staleness across cached-plan rebuilds.
#
# A FOR KEY proof is derived at parse analysis and assumes plain inheritance
# parents among its evidence relations have no children; the inheritance-shape
# lock guarantees that only until the proving transaction ends.  plpgsql keeps
# the analyzed tree in the SPI plan cache and rebuilds custom plans from it,
# re-expanding inheritance from the live catalogs, so a child attached after
# the proof was made can be seen by a rebuilt plan whose proof never covered
# it.  The two injection points widen the natural window: the backend blocks
# after the plan-cache validity check, the child is attached, and the cache
# refresh a sinval catchup/reset would deliver is injected before planning.
#
# The planner must flag the stale plan at the relhassubclass read that decides
# child expansion, and plancache must discard it and re-derive the analysis,
# so the blocked call fails with the ordinary proof error instead of returning
# a fan-out row (the referencing row matched twice).

setup
{
	CREATE EXTENSION injection_points;
	CREATE SCHEMA kcpi;
	CREATE TABLE kcpi.parent (id int PRIMARY KEY);
	CREATE TABLE kcpi.child (id int NOT NULL);
	CREATE TABLE kcpi.kref (id int PRIMARY KEY,
	                        pid int NOT NULL REFERENCES kcpi.parent (id));
	INSERT INTO kcpi.parent VALUES (1);
	INSERT INTO kcpi.child VALUES (1);
	INSERT INTO kcpi.kref VALUES (10, 1);
	CREATE FUNCTION kcpi.run(min_id int) RETURNS TABLE(out_rid int, out_pid int)
	LANGUAGE plpgsql AS $$
	BEGIN
		RETURN QUERY
		SELECT r.id, p.id
		FROM kcpi.parent p
		JOIN kcpi.kref r FOR KEY (pid) -> p (id)
		WHERE r.id >= min_id
		ORDER BY 1, 2;
	END $$;
}

teardown
{
	DROP SCHEMA kcpi CASCADE;
	DROP EXTENSION injection_points;
}

session runner
step exec1	{ SELECT * FROM kcpi.run(0); }
step attach
{
	SELECT FROM injection_points_set_local();
	SELECT FROM injection_points_attach('build-cached-plan-before-planning', 'wait');
	SELECT FROM injection_points_attach('build-cached-plan-planning-start', 'invalidate_system_caches');
}
step exec2	{ SELECT * FROM kcpi.run(0); }
step exec3	{ SELECT * FROM kcpi.run(0); }

session ddl
step inherit	{ ALTER TABLE kcpi.child INHERIT kcpi.parent; }

session ctl
step wakeup	{ SELECT FROM injection_points_wakeup('build-cached-plan-before-planning'); }
step detach
{
	SELECT FROM injection_points_detach('build-cached-plan-before-planning');
	SELECT FROM injection_points_detach('build-cached-plan-planning-start');
}

permutation exec1 attach exec2 inherit wakeup(exec2) detach exec3
