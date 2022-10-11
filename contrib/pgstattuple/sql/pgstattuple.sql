CREATE EXTENSION pgstattuple;
--
-- It's difficult to come up with platform-independent test cases for
-- the pgstattuple functions, but the results for empty tables and
-- indexes should be that.
--

create table test (a int primary key, b int[], p point) with (autovacuum_enabled = off);

insert into test(a, b, p)
select a, array(select generate_series as num from generate_series(1, 10)), point(a*10, a*10) from generate_series(1, 10000) a;
-- make dead tuples
delete from test where a between 1 and 5000;

select * from pgstattuple('test');
select * from pgstattuple('test'::text);
select * from pgstattuple('test'::name);
select * from pgstattuple('test'::regclass);
select pgstattuple(oid) from pg_class where relname = 'test';
select pgstattuple(relname) from pg_class where relname = 'test';
select * from pgstattuple_approx('test');
select * from pgstattuple_approx('test'::text);
select * from pgstattuple_approx('test'::regclass);

select version, tree_level,
    index_size / current_setting('block_size')::int as index_size,
    root_block_no, internal_pages, leaf_pages, empty_pages, deleted_pages,
    avg_leaf_density, leaf_fragmentation
    from pgstatindex('test_pkey');
select version, tree_level,
    index_size / current_setting('block_size')::int as index_size,
    root_block_no, internal_pages, leaf_pages, empty_pages, deleted_pages,
    avg_leaf_density, leaf_fragmentation
    from pgstatindex('test_pkey'::text);
select version, tree_level,
    index_size / current_setting('block_size')::int as index_size,
    root_block_no, internal_pages, leaf_pages, empty_pages, deleted_pages,
    avg_leaf_density, leaf_fragmentation
    from pgstatindex('test_pkey'::name);
select version, tree_level,
    index_size / current_setting('block_size')::int as index_size,
    root_block_no, internal_pages, leaf_pages, empty_pages, deleted_pages,
    avg_leaf_density, leaf_fragmentation
    from pgstatindex('test_pkey'::regclass);

select pg_relpages('test');
select pg_relpages('test_pkey');
select pg_relpages('test_pkey'::text);
select pg_relpages('test_pkey'::name);
select pg_relpages('test_pkey'::regclass);
select pg_relpages(oid) from pg_class where relname = 'test_pkey';
select pg_relpages(relname) from pg_class where relname = 'test_pkey';

create index test_ginidx on test using gin (b);

select * from pgstatginindex('test_ginidx');

create index test_hashidx on test using hash (b);

select * from pgstathashindex('test_hashidx');

-- check that (btree, hash, gist) index with pgstattuple should work
create index test_btreeidx on test using btree (a);
create index test_gistidx on test using gist (p);

select * from pgstattuple('test_btreeidx');
select * from pgstattuple('test_hashidx');
select * from pgstattuple('test_gistidx');

-- check that these index type error with (gin, spgist, brin, etc)
create index test_spgistidx on test using spgist (p);
create index test_brinidx on test using brin (a);

select * from pgstattuple('test_ginidx');
select * from pgstattuple('test_spgistidx');
select * from pgstattuple('test_brinidx');

-- these should error with the wrong type
select pgstatginindex('test_pkey');
select pgstathashindex('test_pkey');

select pgstatindex('test_ginidx');
select pgstathashindex('test_ginidx');

select pgstatindex('test_hashidx');
select pgstatginindex('test_hashidx');

-- check that using any of these functions with unsupported relations will fail
create table test_partitioned (a int) partition by range (a);
create index test_partitioned_index on test_partitioned(a);
-- these should all fail
select pgstattuple('test_partitioned');
select pgstattuple('test_partitioned_index');
select pgstattuple_approx('test_partitioned');
select pg_relpages('test_partitioned');
select pgstatindex('test_partitioned');
select pgstatginindex('test_partitioned');
select pgstathashindex('test_partitioned');

create view test_view as select 1;
-- these should all fail
select pgstattuple('test_view');
select pgstattuple_approx('test_view');
select pg_relpages('test_view');
select pgstatindex('test_view');
select pgstatginindex('test_view');
select pgstathashindex('test_view');

create foreign data wrapper dummy;
create server dummy_server foreign data wrapper dummy;
create foreign table test_foreign_table () server dummy_server;
-- these should all fail
select pgstattuple('test_foreign_table');
select pgstattuple_approx('test_foreign_table');
select pg_relpages('test_foreign_table');
select pgstatindex('test_foreign_table');
select pgstatginindex('test_foreign_table');
select pgstathashindex('test_foreign_table');

-- a partition of a partitioned table should work though
create table test_partition partition of test_partitioned for values from (1) to (100);
select pgstattuple('test_partition');
select pgstattuple_approx('test_partition');
select pg_relpages('test_partition');

-- toast tables should work
select pgstattuple((select reltoastrelid from pg_class where relname = 'test'));
select pgstattuple_approx((select reltoastrelid from pg_class where relname = 'test'));
select pg_relpages((select reltoastrelid from pg_class where relname = 'test'));

-- not for the index calls though, of course
select pgstatindex('test_partition');
select pgstatginindex('test_partition');
select pgstathashindex('test_partition');

-- an actual index of a partitioned table should work though
create index test_partition_idx on test_partition(a);
create index test_partition_hash_idx on test_partition using hash (a);
-- these should work
select pgstatindex('test_partition_idx');
select pgstathashindex('test_partition_hash_idx');

drop table test_partitioned;
drop view test_view;
drop foreign table test_foreign_table;
drop server dummy_server;
drop foreign data wrapper dummy;
