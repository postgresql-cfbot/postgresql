/* contrib/intarray/intarray--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION intarray UPDATE TO '1.3'" to load this file. \quit

-- Update procedure signatures the hard way.
-- We use to_regprocedure() so that query doesn't fail if run against 9.6beta1 definitions,
-- wherein the signatures have been updated already.  In that case to_regprocedure() will
-- return NULL and no updates will happen.

UPDATE pg_catalog.pg_proc SET
  proargtypes = pg_catalog.array_to_string(newtypes::pg_catalog.oid[], ' ')::pg_catalog.oidvector,
  pronargs = pg_catalog.array_length(newtypes, 1)
FROM (VALUES
(NULL::pg_catalog.text, NULL::pg_catalog.regtype[]), -- establish column types
('g_int_compress(internal)', '{internal,internal}'),
('g_int_decompress(internal)', '{internal,internal}'),
('g_intbig_consistent(internal,_int4,smallint,oid,internal)', '{internal,_int4,smallint,oid,internal,internal}'),
('g_intbig_compress(internal)', '{internal,internal}'),
('g_intbig_decompress(internal)', '{internal,internal}'),
('g_intbig_penalty(internal,internal,internal)', '{internal,internal,internal,internal}'),
('g_intbig_picksplit(internal,internal)', '{internal,internal,internal}'),
('g_intbig_union(internal,internal)', '{internal,internal,internal}'),
('g_intbig_same(intbig_gkey,intbig_gkey,internal)', '{intbig_gkey,intbig_gkey,internal,internal}')
) AS update_data (oldproc, newtypes)
WHERE oid = pg_catalog.to_regprocedure(oldproc);

CREATE FUNCTION g_intbig_options(internal, boolean)
RETURNS internal
AS 'MODULE_PATHNAME', 'g_intbig_options'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

ALTER OPERATOR FAMILY gist__intbig_ops USING gist
ADD FUNCTION 10 (_int4) g_intbig_options (internal, boolean);
