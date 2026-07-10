/* contrib/pageinspect/pageinspect--1.13--1.14.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.14'" to load this file. \quit

--
-- bt_page_items(relname, blkno) -- add hot_indexed column
--
-- A HOT-indexed (SIU) fresh index entry stores its heap TID with an internal
-- marker bit (ItemPointerSIUMaybeStaleFlag) set in the offset field, which
-- tells a bitmap scan the entry points mid-chain and must be resolved
-- lossily.  Earlier pageinspect versions surfaced that marker as an inflated
-- ctid offset; from 1.14 the ctid/htid columns report the real offset and the
-- new hot_indexed column exposes the marker explicitly.
--
DROP FUNCTION bt_page_items(text, int8);
CREATE FUNCTION bt_page_items(IN relname text, IN blkno int8,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT nulls bool,
    OUT vars bool,
    OUT data text,
    OUT dead boolean,
    OUT htid tid,
    OUT tids tid[],
    OUT hot_indexed bool)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_page_items_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_page_items(page) -- add hot_indexed column
--
DROP FUNCTION bt_page_items(bytea);
CREATE FUNCTION bt_page_items(IN page bytea,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT nulls bool,
    OUT vars bool,
    OUT data text,
    OUT dead boolean,
    OUT htid tid,
    OUT tids tid[],
    OUT hot_indexed bool)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_page_items_bytea'
LANGUAGE C STRICT PARALLEL SAFE;
