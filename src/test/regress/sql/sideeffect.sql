-- Queries to ensure no sideeffects from the regress suite. This test does not
-- have a corresponding expectfile stored.
SELECT rolname FROM pg_roles ORDER BY 1;
SELECT spcname FROM pg_tablespace ORDER BY 1;
SELECT subname FROM pg_subscription ORDER BY 1;
