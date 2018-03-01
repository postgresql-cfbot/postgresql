/* contrib/hstore/hstore--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION intarray UPDATE TO '1.6'" to load this file. \quit

-- Update procedure signatures the hard way.
-- We use to_regprocedure() so that query doesn't fail if run against 9.6beta1 definitions,
-- wherein the signatures have been updated already.  In that case to_regprocedure() will
-- return NULL and no updates will happen.

UPDATE pg_catalog.pg_proc SET
  proargtypes = pg_catalog.array_to_string(newtypes::pg_catalog.oid[], ' ')::pg_catalog.oidvector,
  pronargs = pg_catalog.array_length(newtypes, 1)
FROM (VALUES
(NULL::pg_catalog.text, NULL::pg_catalog.regtype[]), -- establish column types
('ghstore_compress(internal)', '{internal,internal}'),
('ghstore_decompress(internal)', '{internal,internal}'),
('ghstore_consistent(internal,hstore,smallint,oid,internal)', '{internal,hstore,smallint,oid,internal,internal}'),
('ghstore_penalty(internal,internal,internal)', '{internal,internal,internal,internal}'),
('ghstore_picksplit(internal,internal)', '{internal,internal,internal}'),
('ghstore_union(internal,internal)', '{internal,internal,internal}'),
('ghstore_same(ghstore,ghstore,internal)', '{ghstore,ghstore,internal,internal}')
) AS update_data (oldproc, newtypes)
WHERE oid = pg_catalog.to_regprocedure(oldproc);

CREATE FUNCTION ghstore_options(internal, boolean)
RETURNS internal
AS 'MODULE_PATHNAME', 'ghstore_options'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

ALTER OPERATOR FAMILY gist_hstore_ops USING gist
ADD FUNCTION 10 (hstore) ghstore_options (internal, boolean);
