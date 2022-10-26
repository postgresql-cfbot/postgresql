-- directory paths and dlsuffix are passed to us in environment variables
\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX

\set regresslib :libdir '/regress' :dlsuffix

--
-- num_nulls()
--

SELECT num_nonnulls(NULL);
SELECT num_nonnulls('1');
SELECT num_nonnulls(NULL::text);
SELECT num_nonnulls(NULL::text, NULL::int);
SELECT num_nonnulls(1, 2, NULL::text, NULL::point, '', int8 '9', 1.0 / NULL);
SELECT num_nonnulls(VARIADIC '{1,2,NULL,3}'::int[]);
SELECT num_nonnulls(VARIADIC '{"1","2","3","4"}'::text[]);
SELECT num_nonnulls(VARIADIC ARRAY(SELECT CASE WHEN i <> 40 THEN i END FROM generate_series(1, 100) i));

SELECT num_nulls(NULL);
SELECT num_nulls('1');
SELECT num_nulls(NULL::text);
SELECT num_nulls(NULL::text, NULL::int);
SELECT num_nulls(1, 2, NULL::text, NULL::point, '', int8 '9', 1.0 / NULL);
SELECT num_nulls(VARIADIC '{1,2,NULL,3}'::int[]);
SELECT num_nulls(VARIADIC '{"1","2","3","4"}'::text[]);
SELECT num_nulls(VARIADIC ARRAY(SELECT CASE WHEN i <> 40 THEN i END FROM generate_series(1, 100) i));

-- special cases
SELECT num_nonnulls(VARIADIC NULL::text[]);
SELECT num_nonnulls(VARIADIC '{}'::int[]);
SELECT num_nulls(VARIADIC NULL::text[]);
SELECT num_nulls(VARIADIC '{}'::int[]);

-- should fail, one or more arguments is required
SELECT num_nonnulls();
SELECT num_nulls();

--
-- canonicalize_path()
--

CREATE FUNCTION test_canonicalize_path(text)
   RETURNS text
   AS :'regresslib'
   LANGUAGE C STRICT IMMUTABLE;

SELECT test_canonicalize_path('/');
SELECT test_canonicalize_path('/./abc/def/');
SELECT test_canonicalize_path('/./../abc/def');
SELECT test_canonicalize_path('/./../../abc/def/');
SELECT test_canonicalize_path('/abc/.././def/ghi');
SELECT test_canonicalize_path('/abc/./../def/ghi//');
SELECT test_canonicalize_path('/abc/def/../..');
SELECT test_canonicalize_path('/abc/def/../../..');
SELECT test_canonicalize_path('/abc/def/../../../../ghi/jkl');
SELECT test_canonicalize_path('.');
SELECT test_canonicalize_path('./');
SELECT test_canonicalize_path('./abc/..');
SELECT test_canonicalize_path('abc/../');
SELECT test_canonicalize_path('abc/../def');
SELECT test_canonicalize_path('..');
SELECT test_canonicalize_path('../abc/def');
SELECT test_canonicalize_path('../abc/..');
SELECT test_canonicalize_path('../abc/../def');
SELECT test_canonicalize_path('../abc/../../def/ghi');
SELECT test_canonicalize_path('./abc/./def/.');
SELECT test_canonicalize_path('./abc/././def/.');
SELECT test_canonicalize_path('./abc/./def/.././ghi/../../../jkl/mno');

--
-- pg_log_backend_memory_contexts()
--
-- Memory contexts are logged and they are not returned to the function.
-- Furthermore, their contents can vary depending on the timing. However,
-- we can at least verify that the code doesn't fail, and that the
-- permissions are set properly.
--

SELECT pg_log_backend_memory_contexts(pg_backend_pid());

SELECT pg_log_backend_memory_contexts(pid) FROM pg_stat_activity
  WHERE backend_type = 'checkpointer';

CREATE ROLE regress_log_memory;

SELECT has_function_privilege('regress_log_memory',
  'pg_log_backend_memory_contexts(integer)', 'EXECUTE'); -- no

GRANT EXECUTE ON FUNCTION pg_log_backend_memory_contexts(integer)
  TO regress_log_memory;

SELECT has_function_privilege('regress_log_memory',
  'pg_log_backend_memory_contexts(integer)', 'EXECUTE'); -- yes

SET ROLE regress_log_memory;
SELECT pg_log_backend_memory_contexts(pg_backend_pid());
RESET ROLE;

REVOKE EXECUTE ON FUNCTION pg_log_backend_memory_contexts(integer)
  FROM regress_log_memory;

DROP ROLE regress_log_memory;

--
-- Test some built-in SRFs
--
-- The outputs of these are variable, so we can't just print their results
-- directly, but we can at least verify that the code doesn't fail.
--
select setting as segsize
from pg_settings where name = 'wal_segment_size'
\gset

select count(*) > 0 as ok from pg_ls_waldir();
-- Test ProjectSet as well as FunctionScan
select count(*) > 0 as ok from (select pg_ls_waldir()) ss;
-- Test not-run-to-completion cases.
select * from pg_ls_waldir() limit 0;
select count(*) > 0 as ok from (select * from pg_ls_waldir() limit 1) ss;
select (w).size = :segsize as ok
from (select pg_ls_waldir() w) ss where length((w).name) = 24 limit 1;

select count(*) >= 0 as ok from pg_ls_archive_statusdir();

-- pg_read_file()
select length(pg_read_file('postmaster.pid')) > 20;
select length(pg_read_file('postmaster.pid', 1, 20));
-- Test missing_ok
select pg_read_file('does not exist'); -- error
select pg_read_file('does not exist', true) IS NULL; -- ok
-- Test invalid argument
select pg_read_file('does not exist', 0, -1); -- error
select pg_read_file('does not exist', 0, -1, true); -- error

-- pg_read_binary_file()
select length(pg_read_binary_file('postmaster.pid')) > 20;
select length(pg_read_binary_file('postmaster.pid', 1, 20));
-- Test missing_ok
select pg_read_binary_file('does not exist'); -- error
select pg_read_binary_file('does not exist', true) IS NULL; -- ok
-- Test invalid argument
select pg_read_binary_file('does not exist', 0, -1); -- error
select pg_read_binary_file('does not exist', 0, -1, true); -- error

-- pg_stat_file()
select size > 20, isdir from pg_stat_file('postmaster.pid');

-- pg_ls_dir()
select * from (select pg_ls_dir('.') a) a where a = 'base' limit 1;
-- Test missing_ok (second argument)
select pg_ls_dir('does not exist', false, false); -- error
select pg_ls_dir('does not exist', true, false); -- ok
-- Test include_dot_dirs (third argument)
select count(*) = 1 as dot_found
  from pg_ls_dir('.', false, true) as ls where ls = '.';
select count(*) = 1 as dot_found
  from pg_ls_dir('.', false, false) as ls where ls = '.';

-- pg_timezone_names()
select * from (select (pg_timezone_names()).name) ptn where name='UTC' limit 1;

-- pg_tablespace_databases()
select count(*) > 0 from
  (select pg_tablespace_databases(oid) as pts from pg_tablespace
   where spcname = 'pg_default') pts
  join pg_database db on pts.pts = db.oid;

--
-- Test replication slot directory functions
--
CREATE ROLE regress_slot_dir_funcs;
-- Not available by default.
SELECT has_function_privilege('regress_slot_dir_funcs',
  'pg_ls_logicalsnapdir()', 'EXECUTE');
SELECT has_function_privilege('regress_slot_dir_funcs',
  'pg_ls_logicalmapdir()', 'EXECUTE');
SELECT has_function_privilege('regress_slot_dir_funcs',
  'pg_ls_replslotdir(text)', 'EXECUTE');
GRANT pg_monitor TO regress_slot_dir_funcs;
-- Role is now part of pg_monitor, so these are available.
SELECT has_function_privilege('regress_slot_dir_funcs',
  'pg_ls_logicalsnapdir()', 'EXECUTE');
SELECT has_function_privilege('regress_slot_dir_funcs',
  'pg_ls_logicalmapdir()', 'EXECUTE');
SELECT has_function_privilege('regress_slot_dir_funcs',
  'pg_ls_replslotdir(text)', 'EXECUTE');
DROP ROLE regress_slot_dir_funcs;

--
-- Test adding a support function to a subject function
--

CREATE FUNCTION my_int_eq(int, int) RETURNS bool
  LANGUAGE internal STRICT IMMUTABLE PARALLEL SAFE
  AS $$int4eq$$;

-- By default, planner does not think that's selective
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 a JOIN tenk1 b ON a.unique1 = b.unique1
WHERE my_int_eq(a.unique2, 42);

-- With support function that knows it's int4eq, we get a different plan
CREATE FUNCTION test_support_func(internal)
    RETURNS internal
    AS :'regresslib', 'test_support_func'
    LANGUAGE C STRICT;

ALTER FUNCTION my_int_eq(int, int) SUPPORT test_support_func;

EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 a JOIN tenk1 b ON a.unique1 = b.unique1
WHERE my_int_eq(a.unique2, 42);

-- Also test non-default rowcount estimate
CREATE FUNCTION my_gen_series(int, int) RETURNS SETOF integer
  LANGUAGE internal STRICT IMMUTABLE PARALLEL SAFE
  AS $$generate_series_int4$$
  SUPPORT test_support_func;

EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 a JOIN my_gen_series(1,1000) g ON a.unique1 = g;

EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 a JOIN my_gen_series(1,10) g ON a.unique1 = g;

--
-- Test functions for control data
--
\x
SELECT checkpoint_lsn > '0/0'::pg_lsn AS checkpoint_lsn,
    redo_lsn > '0/0'::pg_lsn AS redo_lsn,
    redo_wal_file IS NOT NULL AS redo_wal_file,
    timeline_id > 0 AS timeline_id,
    prev_timeline_id > 0 AS prev_timeline_id,
    next_xid IS NOT NULL AS next_xid,
    next_oid > 0 AS next_oid,
    next_multixact_id != '0'::xid AS next_multixact_id,
    next_multi_offset IS NOT NULL AS next_multi_offset,
    oldest_xid != '0'::xid AS oldest_xid,
    oldest_xid_dbid > 0 AS oldest_xid_dbid,
    oldest_active_xid != '0'::xid AS oldest_active_xid,
    oldest_multi_xid != '0'::xid AS oldest_multi_xid,
    oldest_multi_dbid > 0 AS oldest_multi_dbid,
    oldest_commit_ts_xid IS NOT NULL AS oldest_commit_ts_xid,
    newest_commit_ts_xid IS NOT NULL AS newest_commit_ts_xid
  FROM pg_control_checkpoint();
SELECT max_data_alignment > 0 AS max_data_alignment,
    database_block_size > 0 AS database_block_size,
    blocks_per_segment > 0 AS blocks_per_segment,
    wal_block_size > 0 AS wal_block_size,
    max_identifier_length > 0 AS max_identifier_length,
    max_index_columns > 0 AS max_index_columns,
    max_toast_chunk_size > 0 AS max_toast_chunk_size,
    large_object_chunk_size > 0 AS large_object_chunk_size,
    float8_pass_by_value IS NOT NULL AS float8_pass_by_value,
    data_page_checksum_version >= 0 AS data_page_checksum_version
  FROM pg_control_init();
SELECT min_recovery_end_lsn >= '0/0'::pg_lsn AS min_recovery_end_lsn,
    min_recovery_end_timeline >= 0 AS min_recovery_end_timeline,
    backup_start_lsn >= '0/0'::pg_lsn AS backup_start_lsn,
    backup_end_lsn >= '0/0'::pg_lsn AS backup_end_lsn,
    end_of_backup_record_required IS NOT NULL AS end_of_backup_record_required
  FROM pg_control_recovery();
SELECT pg_control_version > 0 AS pg_control_version,
    catalog_version_no > 0 AS catalog_version_no,
    system_identifier >= 0 AS system_identifier,
    pg_control_last_modified <= now() AS pg_control_last_modified
  FROM pg_control_system();
