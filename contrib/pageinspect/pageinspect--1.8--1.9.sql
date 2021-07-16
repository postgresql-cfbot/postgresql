/* contrib/pageinspect/pageinspect--1.8--1.9.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.9'" to load this file. \quit

--
-- gist_page_opaque_info()
--
CREATE FUNCTION gist_page_opaque_info(IN page bytea,
    OUT lsn pg_lsn,
    OUT nsn pg_lsn,
    OUT rightlink bigint,
    OUT flags text[])
AS 'MODULE_PATHNAME', 'gist_page_opaque_info'
LANGUAGE C STRICT PARALLEL SAFE;


--
-- gist_page_items_bytea()
--
CREATE FUNCTION gist_page_items_bytea(IN page bytea,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT dead boolean,
    OUT key_data bytea)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gist_page_items_bytea'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- gist_page_items()
--
CREATE FUNCTION gist_page_items(IN page bytea,
    IN index_oid regclass,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT dead boolean,
    OUT keys text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gist_page_items'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- get_raw_page()
--
DROP FUNCTION get_raw_page(text, int4);
CREATE FUNCTION get_raw_page(text, int8)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

DROP FUNCTION get_raw_page(text, text, int4);
CREATE FUNCTION get_raw_page(text, text, int8)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_fork_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- page_checksum()
--
DROP FUNCTION page_checksum(IN page bytea, IN blkno int4);
CREATE FUNCTION page_checksum(IN page bytea, IN blkno int8)
RETURNS smallint
AS 'MODULE_PATHNAME', 'page_checksum_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_metap()
--
DROP FUNCTION bt_metap(text);
CREATE FUNCTION bt_metap(IN relname text,
    OUT magic int4,
    OUT version int4,
    OUT root int8,
    OUT level int8,
    OUT fastroot int8,
    OUT fastlevel int8,
    OUT last_cleanup_num_delpages int8,
    OUT last_cleanup_num_tuples float8,
    OUT allequalimage boolean)
AS 'MODULE_PATHNAME', 'bt_metap'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_page_stats()
--
DROP FUNCTION bt_page_stats(text, int4);
CREATE FUNCTION bt_page_stats(IN relname text, IN blkno int8,
    OUT blkno int8,
    OUT type "char",
    OUT live_items int4,
    OUT dead_items int4,
    OUT avg_item_size int4,
    OUT page_size int4,
    OUT free_size int4,
    OUT btpo_prev int8,
    OUT btpo_next int8,
    OUT btpo_level int8,
    OUT btpo_flags int4)
AS 'MODULE_PATHNAME', 'bt_page_stats_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_page_items()
--
DROP FUNCTION bt_page_items(text, int4);
CREATE FUNCTION bt_page_items(IN relname text, IN blkno int8,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT nulls bool,
    OUT vars bool,
    OUT data text,
    OUT dead boolean,
    OUT htid tid,
    OUT tids tid[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_page_items_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- brin_page_items()
--
DROP FUNCTION brin_page_items(IN page bytea, IN index_oid regclass);
CREATE FUNCTION brin_page_items(IN page bytea, IN index_oid regclass,
    OUT itemoffset int,
    OUT blknum int8,
    OUT attnum int,
    OUT allnulls bool,
    OUT hasnulls bool,
    OUT placeholder bool,
    OUT value text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'brin_page_items'
LANGUAGE C STRICT PARALLEL SAFE;

-- Convert SQL functions to new style

CREATE OR REPLACE FUNCTION heap_page_item_attrs(
    IN page bytea,
    IN rel_oid regclass,
    IN do_detoast bool,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_attrs bytea[]
    )
RETURNS SETOF record
LANGUAGE SQL PARALLEL SAFE
BEGIN ATOMIC
SELECT lp,
       lp_off,
       lp_flags,
       lp_len,
       t_xmin,
       t_xmax,
       t_field3,
       t_ctid,
       t_infomask2,
       t_infomask,
       t_hoff,
       t_bits,
       t_oid,
       tuple_data_split(
         rel_oid,
         t_data,
         t_infomask,
         t_infomask2,
         t_bits,
         do_detoast)
         AS t_attrs
  FROM heap_page_items(page);
END;

CREATE OR REPLACE FUNCTION heap_page_item_attrs(IN page bytea, IN rel_oid regclass,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_attrs bytea[]
    )
RETURNS SETOF record
LANGUAGE SQL PARALLEL SAFE
BEGIN ATOMIC
SELECT * FROM heap_page_item_attrs(page, rel_oid, false);
END;
