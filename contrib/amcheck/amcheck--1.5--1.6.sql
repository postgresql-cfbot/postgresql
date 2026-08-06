/* contrib/amcheck/amcheck--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck UPDATE TO '1.6'" to load this file. \quit


--
-- brin_index_check()
--
CREATE FUNCTION brin_index_check(index regclass,
                                 regularpagescheck boolean default false,
                                 heapallindexed boolean default false,
                                 variadic text[] default '{}'
)
    RETURNS VOID
AS 'MODULE_PATHNAME', 'brin_index_check'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- We don't want this to be available to public
REVOKE ALL ON FUNCTION brin_index_check(regclass, boolean, boolean, text[]) FROM PUBLIC;