--
-- GLobal emparary table test case 
--

CREATE SCHEMA IF NOT EXISTS global_temporary_table;
set search_path=global_temporary_table,sys;

--
--  test create global temp table basic syntax
--
create global temp table gtt_on_commit_default(a int primary key, b text);
create global temp table gtt_on_commit_delete(a int primary key, b text) on commit delete rows;
create global temp table gtt_on_commit_delete2(n int) with (on_commit_delete_rows='true');
create global temp table gtt_on_commit_preserve(a int primary key, b text) on commit PRESERVE rows;
create global temp table gtt_test_rename(a int primary key, b text);
create global temp table gtt_test_createindex(c0 tsvector,c1 varchar(100), c2 int);
create global temp table gtt_test_alter(b text) with(on_commit_delete_rows=true);
--
-- test DML on global temp table
--

-- update empty temp table
update gtt_on_commit_delete set b ='test';
begin;
insert into gtt_on_commit_delete values (1);
update gtt_on_commit_delete set b ='test';
-- should 1 row
select * from gtt_on_commit_delete;
commit;
-- data delete after transaction commit
-- should 0 row
select * from gtt_on_commit_delete;

-- update empty temp table
update gtt_on_commit_preserve set b ='test';
insert into gtt_on_commit_preserve values (2);
begin;
insert into gtt_on_commit_preserve values (3);
update gtt_on_commit_preserve set b ='test';
-- should 2 row
select * from gtt_on_commit_preserve order by a;
delete from gtt_on_commit_preserve where a=2;
commit;
-- should 1 row
select * from gtt_on_commit_preserve order by a;

begin;
insert into gtt_on_commit_preserve values (4);
-- temp table support truncate;
truncate gtt_on_commit_preserve;
select * from gtt_on_commit_preserve order by a;
rollback;
-- should 1 row
select * from gtt_on_commit_preserve order by a;

--
-- test unsupported global temp partition table
--

-- should fail
CREATE global temp TABLE global_temp_partition_01 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
) PARTITION BY RANGE (cre_time)
on commit delete rows;

CREATE TABLE regular_partition_01_2019 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
);

CREATE TABLE regular_partition01 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
) PARTITION BY RANGE (cre_time);

-- should fail
CREATE global temp TABLE temp_partition01_2018
PARTITION OF regular_partition01
FOR VALUES FROM ('2018-01-01 00:00:00') TO ('2019-01-01 00:00:00');

CREATE global temp TABLE global_temp_partition_01_2021 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
)on commit delete rows;

-- should fail
ALTER TABLE regular_partition01 ATTACH PARTITION global_temp_partition_01_2021 FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2022-01-01 00:00:00');

--
-- test unsupported inherit table
--
create table inherits_parent(
a int not null,
b varchar(32) not null default 'Got u',
c int check (c > 0),
d date not null
);

create global temp table inherits_parent_global_temp(
a int not null,
b varchar(32) not null default 'Got u',
c int check (c > 0),
d date not null
)on commit delete rows;

-- should fail
create global temp table temp_inherit() inherits (inherits_parent);
-- should fail
create global temp table temp_inherit() inherits (inherits_parent_global_temp) on commit delete rows;

--
-- test DDL on global temp table
--
create index idx_gtt_test_alter_b on gtt_test_alter (b);
insert into gtt_test_alter values('test');
alter table gtt_test_alter alter b type varchar;
create index gtt_idx_1 on gtt_test_createindex using gin (c0);
create index gtt_idx_2 on gtt_test_createindex using gist (c0);
create index gtt_idx_3 on gtt_test_createindex using hash (c2);
alter table gtt_test_rename rename to gtt_test_new;
ALTER TABLE gtt_test_new ADD COLUMN address varchar(30);
create index CONCURRENTLY idx_b on gtt_on_commit_default (b);
cluster gtt_on_commit_default using gtt_on_commit_default_pkey;
insert into gtt_on_commit_default values(1,'test');
create global temp table gtt_test_alter1 (a int primary key,b text);
alter table gtt_test_alter1 alter a type varchar;
-- should fail
alter table gtt_on_commit_default alter a type varchar;
-- should fail
cluster gtt_on_commit_default using gtt_on_commit_default_pkey;
-- should fail
create table gtt_on_commit_default(a int primary key, b text) on commit delete rows;
-- should fail
alter table gtt_on_commit_default SET TABLESPACE pg_default;
-- should fail
alter table gtt_on_commit_default set ( on_commit_delete_rows='true');
-- should fail
create table gtt_on_commit_default(a int primary key, b text) with(on_commit_delete_rows=true);
-- should fail
create or replace global temp view gtt_v as select 5;
create table foo();
-- should fail
alter table foo set (on_commit_delete_rows='true');
-- should fail
create global temp table gtt_on_commit_preserve(a int primary key, b text) on commit drop;
-- should fail
create global temp table gtt4(a int primary key, b text) with(on_commit_delete_rows=true) on commit delete rows;
-- should fail
CREATE MATERIALIZED VIEW mv_gtt_on_commit_default as select * from gtt_on_commit_default;
-- test create table as select
CREATE GLOBAL TEMPORARY TABLE test_create_table_as AS SELECT 1 AS a;
-- test copy stmt
create global temp table gtt_copytest (
        c1 int,
        "col with , comma" text,
        "col with "" quote"  int);

copy gtt_copytest from stdin csv header;
this is just a line full of junk that would error out if parsed
1,a,1
2,b,2
\.
select count(*) from gtt_copytest;

--
-- test foreign key dependencies for global temp table
--
CREATE global temp TABLE temp_products (
    product_no integer PRIMARY KEY,
    name text,
    price numeric
);

CREATE global temp TABLE products (
    product_no integer PRIMARY KEY,
    name text,
    price numeric
);

CREATE global temp TABLE temp_orders (
    order_id integer PRIMARY KEY,
    product_no integer REFERENCES temp_products(product_no),
    quantity integer
)on commit delete rows;

-- should fail
CREATE TABLE orders (
    order_id integer PRIMARY KEY,
    product_no integer REFERENCES temp_products(product_no),
    quantity integer
);

CREATE global temp TABLE temp_orders_2 (
    order_id integer PRIMARY KEY,
    product_no integer REFERENCES products(product_no),
    quantity integer
);

--should fail
insert into temp_orders values(1,1,1);

insert into temp_products values(1,'test',1.0);
begin;
insert into temp_orders values(1,1,1);
commit;
-- should 1 row
select count(*) from temp_products;
-- should 0 row
select count(*) from temp_orders;

--
-- test sequence on global temp table
--
create global temp table global_temp_with_serial (a SERIAL,b int);
begin;
set transaction_read_only = true;
-- support insert data to temp table in read only transaction
insert into global_temp_with_serial (b) values(1);
select * from global_temp_with_serial;
commit;

create sequence seq_1;
CREATE GLOBAL TEMPORARY TABLE temp_table_with_sequence_oncommit_delete(c1 int PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE temp_table_with_sequence_oncommit_preserve(c1 int PRIMARY KEY) ON COMMIT PRESERVE ROWS;
alter table temp_table_with_sequence_oncommit_delete add c2 int default nextval('seq_1');
alter table temp_table_with_sequence_oncommit_preserve add c2 int default nextval('seq_1');
begin;
insert into temp_table_with_sequence_oncommit_delete (c1)values(1);
insert into temp_table_with_sequence_oncommit_preserve (c1)values(2);
insert into temp_table_with_sequence_oncommit_delete (c1)values(3);
insert into temp_table_with_sequence_oncommit_preserve (c1)values(4);
-- should 2 row
select * from temp_table_with_sequence_oncommit_delete order by c1;
commit;
-- should 0 row
select * from temp_table_with_sequence_oncommit_delete order by c1;
-- should 2 row
select * from temp_table_with_sequence_oncommit_preserve order by c1;

--
-- test statistics on temp table
--
create global temp table temp_table_test_statistics(a int);
insert into temp_table_test_statistics values(generate_series(1,100000));
create index idx_test_1 on temp_table_test_statistics (a);
create index idx_test_2 on temp_table_test_statistics((a*10));
explain (costs off) select * from temp_table_test_statistics where a=200000;
explain (costs off) select * from temp_table_test_statistics where a*10=3;
-- test statistic for whole row
explain (costs off) select count(*) from temp_table_test_statistics group by temp_table_test_statistics;
-- test statistic for system column
explain (costs off) select count(*) from temp_table_test_statistics group by tableoid;
analyze temp_table_test_statistics;
-- indexscan by idx_test_1
explain (costs off) select * from temp_table_test_statistics where a=200000;
-- indexscan by idx_test_2
explain (costs off) select * from temp_table_test_statistics where a*10=3;
explain (costs off) select count(*) from temp_table_test_statistics group by temp_table_test_statistics;
explain (costs off) select count(*) from temp_table_test_statistics group by tableoid;

--
-- test temp table with toast table
--
create global temp table gtt_t_kenyon(id int,vname varchar(48),remark text) on commit PRESERVE rows;
create index idx_gtt_t_kenyon_1 on gtt_t_kenyon(id);
create index idx_gtt_t_kenyon_2 on gtt_t_kenyon(remark);
insert into gtt_t_kenyon select generate_series(1,2000),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God',500);
select relname, pg_relation_size(oid),pg_relation_size(reltoastrelid),pg_table_size(oid),pg_indexes_size(oid),pg_total_relation_size(oid) from pg_class where relname like '%t_kenyon%' order by relname;
insert into gtt_t_kenyon select generate_series(5,6),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God,Remark here!!',5500);
select relname, pg_relation_size(oid),pg_relation_size(reltoastrelid),pg_table_size(oid),pg_indexes_size(oid),pg_total_relation_size(oid) from pg_class where relname like '%t_kenyon%' order by relname;
begin;
truncate gtt_t_kenyon;
insert into gtt_t_kenyon select generate_series(1,10),repeat('kenyon here'||'^_^',2),repeat('^_^ Kenyon is not God',10);
select relname, pg_relation_size(oid),pg_relation_size(reltoastrelid),pg_table_size(oid),pg_indexes_size(oid),pg_total_relation_size(oid) from pg_class where relname like '%t_kenyon%' order by relname;
rollback;
select relname, pg_relation_size(oid),pg_relation_size(reltoastrelid),pg_table_size(oid),pg_indexes_size(oid),pg_total_relation_size(oid) from pg_class where relname like '%t_kenyon%' order by relname;

-- test analyze/vacuum on global temp table
ANALYZE gtt_t_kenyon;
VACUUM gtt_t_kenyon;

--
-- test global temp table system view
--
create global temp table temp_table_test_systemview(a int primary key, b text) on commit PRESERVE rows;
-- should empty, storage not initialized
select tablename from pg_gtt_attached_pids where tablename = 'temp_table_test_systemview';
-- should empty, storage not initialized
select count(*) from pg_list_gtt_relfrozenxids();
insert into temp_table_test_systemview values(generate_series(1,10000),'test');
select tablename from pg_gtt_attached_pids where tablename = 'temp_table_test_systemview';
select count(*) from pg_list_gtt_relfrozenxids();
select schemaname, tablename, relpages, reltuples, relallvisible from pg_gtt_relstats where tablename in('temp_table_test_systemview','temp_table_test_systemview_pkey') order by tablename;
-- should empty
select * from pg_gtt_stats where tablename = 'temp_table_test_systemview' order by tablename;
analyze temp_table_test_systemview;
select schemaname, tablename, relpages, reltuples, relallvisible from pg_gtt_relstats where tablename in('temp_table_test_systemview','temp_table_test_systemview_pkey') order by tablename;
-- get data after analyze;
select * from pg_gtt_stats where tablename = 'temp_table_test_systemview' order by tablename;

-- get all object info in current schema
select relname ,relkind, relpersistence, reloptions from pg_class c, pg_namespace n where c.relnamespace = n.oid and n.nspname = 'global_temporary_table' order by relkind,relpersistence,relname;

reset search_path;
drop schema global_temporary_table cascade;
-- should empty
select * from pg_list_gtt_relfrozenxids();

