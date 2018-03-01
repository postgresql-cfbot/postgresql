/* contrib/ltree/ltree--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION ltree UPDATE TO '1.2'" to load this file. \quit

-- Update procedure signatures the hard way.
-- We use to_regprocedure() so that query doesn't fail if run against 9.6beta1 definitions,
-- wherein the signatures have been updated already.  In that case to_regprocedure() will
-- return NULL and no updates will happen.

UPDATE pg_catalog.pg_proc SET
  proargtypes = pg_catalog.array_to_string(newtypes::pg_catalog.oid[], ' ')::pg_catalog.oidvector,
  pronargs = pg_catalog.array_length(newtypes, 1)
FROM (VALUES
(NULL::pg_catalog.text, NULL::pg_catalog.regtype[]), -- establish column types
('ltree_compress(internal)', '{internal,internal}'),
('ltree_decompress(internal)', '{internal,internal}'),
('ltree_same(ltree_gist,ltree_gist,internal)', '{ltree_gist,ltree_gist,internal,internal}'),
('ltree_union(internal,internal)', '{internal,internal,internal}'),
('ltree_penalty(internal,internal,internal)', '{internal,internal,internal,internal}'),
('ltree_picksplit(internal,internal)', '{internal,internal,internal}'),
('ltree_consistent(internal,_int4,smallint,oid,internal)', '{internal,_int4,smallint,oid,internal,internal}'),
('_ltree_compress(internal)', '{internal,internal}'),
('_ltree_same(ltree_gist,ltree_gist,internal)', '{ltree_gist,ltree_gist,internal,internal}'),
('_ltree_union(internal,internal)', '{internal,internal,internal}'),
('_ltree_penalty(internal,internal,internal)', '{internal,internal,internal,internal}'),
('_ltree_picksplit(internal,internal)', '{internal,internal,internal}'),
('_ltree_consistent(internal,_int4,smallint,oid,internal)', '{internal,_int4,smallint,oid,internal,internal}')
) AS update_data (oldproc, newtypes)
WHERE oid = pg_catalog.to_regprocedure(oldproc);

CREATE FUNCTION ltree_gist_options(internal, boolean)
RETURNS internal
AS 'MODULE_PATHNAME', 'ltree_gist_options'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION _ltree_gist_options(internal, boolean)
RETURNS internal
AS 'MODULE_PATHNAME', '_ltree_gist_options'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

ALTER OPERATOR FAMILY gist_ltree_ops USING gist
ADD FUNCTION 10 (ltree) ltree_gist_options (internal, boolean);

ALTER OPERATOR FAMILY gist__ltree_ops USING gist
ADD FUNCTION 10 (_ltree) _ltree_gist_options (internal, boolean);

