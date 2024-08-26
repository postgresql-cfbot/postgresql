-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION tablefunc UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION rand_array(numvals int, minlen int, maxlen int, minval int, maxval int)
RETURNS setof int[]
AS 'MODULE_PATHNAME','rand_array_int'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION rand_array(numvals int, minlen int, maxlen int, minval bigint, maxval bigint)
RETURNS setof bigint[]
AS 'MODULE_PATHNAME','rand_array_bigint'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION rand_array(numvals int, minlen int, maxlen int, minval numeric, maxval numeric)
RETURNS setof numeric[]
AS 'MODULE_PATHNAME','rand_array_numeric'
LANGUAGE C VOLATILE STRICT;
