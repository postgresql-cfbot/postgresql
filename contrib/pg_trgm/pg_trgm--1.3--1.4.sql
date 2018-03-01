/* contrib/pg_trgm/pg_trgm--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_trgm UPDATE TO '1.4'" to load this file. \quit

-- Update procedure signatures the hard way.
-- We use to_regprocedure() so that query doesn't fail if run against 9.6beta1 definitions,
-- wherein the signatures have been updated already.  In that case to_regprocedure() will
-- return NULL and no updates will happen.

UPDATE pg_catalog.pg_proc SET
  proargtypes = pg_catalog.array_to_string(newtypes::pg_catalog.oid[], ' ')::pg_catalog.oidvector,
  pronargs = pg_catalog.array_length(newtypes, 1)
FROM (VALUES
(NULL::pg_catalog.text, NULL::pg_catalog.regtype[]), -- establish column types
('gtrgm_compress(internal)', '{internal,internal}'),
('gtrgm_decompress(internal)', '{internal,internal}'),
('gtrgm_same(gtrgm,gtrgm,internal)', '{gtrgm,gtrgm,internal,internal}'),
('gtrgm_union(internal,internal)', '{internal,internal,internal}'),
('gtrgm_penalty(internal,internal,internal)', '{internal,internal,internal,internal}'),
('gtrgm_picksplit(internal,internal)', '{internal,internal,internal}'),
('gtrgm_consistent(internal,text,smallint,oid,internal)', '{internal,text,smallint,oid,internal,internal}'),
('gtrgm_distance(internal,text,smallint,oid,internal)', '{internal,text,smallint,oid,internal,internal}')
) AS update_data (oldproc, newtypes)
WHERE oid = pg_catalog.to_regprocedure(oldproc);

CREATE FUNCTION gtrgm_options(internal, boolean)
RETURNS internal
AS 'MODULE_PATHNAME', 'gtrgm_options'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

ALTER OPERATOR FAMILY gist_trgm_ops USING gist
ADD FUNCTION 10 (text) gtrgm_options (internal, boolean);
