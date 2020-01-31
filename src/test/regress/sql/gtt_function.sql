
CREATE SCHEMA IF NOT EXISTS gtt_function;

set search_path=gtt_function,sys;

create global temp table gtt1(a int primary key, b text);

create global temp table gtt_test_rename(a int primary key, b text);

create global temp table gtt2(a int primary key, b text) on commit delete rows;

create global temp table gtt3(a int primary key, b text) on commit PRESERVE rows;

create global temp table tmp_t0(c0 tsvector,c1 varchar(100));

create table tbl_inherits_parent(
a int not null,
b varchar(32) not null default 'Got u',
c int check (c > 0),
d date not null
);

create global temp table tbl_inherits_parent_global_temp(
a int not null,
b varchar(32) not null default 'Got u',
c int check (c > 0),
d date not null
)on commit delete rows;

CREATE global temp TABLE products (
    product_no integer PRIMARY KEY,
    name text,
    price numeric
);

-- ERROR
create index CONCURRENTLY idx_gtt1 on gtt1 (b);

-- ERROR
cluster gtt1 using gtt1_pkey;

-- ERROR
create table gtt1(a int primary key, b text) on commit delete rows;

-- ok
create table gtt1(a int primary key, b text) with(on_commit_delete_rows=true);

-- ok
CREATE global temp TABLE measurement (
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) PARTITION BY RANGE (logdate);

--ok
CREATE global temp TABLE p_table01 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
) PARTITION BY RANGE (cre_time)
WITH (
OIDS = FALSE
)on commit delete rows;
 
CREATE global temp TABLE p_table01_2018
PARTITION OF p_table01
FOR VALUES FROM ('2018-01-01 00:00:00') TO ('2019-01-01 00:00:00');
 
CREATE global temp TABLE p_table01_2017
PARTITION OF p_table01
FOR VALUES FROM ('2017-01-01 00:00:00') TO ('2018-01-01 00:00:00');

begin;
insert into p_table01 values(1,'2018-01-02 00:00:00','test1');
insert into p_table01 values(1,'2018-01-02 00:00:00','test2');
select count(*) from p_table01;
commit;

select count(*) from p_table01;

--ok
CREATE global temp TABLE p_table02 (
id        bigserial NOT NULL,
cre_time  timestamp without time zone,
note      varchar(30)
) PARTITION BY RANGE (cre_time)
WITH (
OIDS = FALSE
)
on commit PRESERVE rows;

CREATE global temp TABLE p_table02_2018
PARTITION OF p_table02
FOR VALUES FROM ('2018-01-01 00:00:00') TO ('2019-01-01 00:00:00');

CREATE global temp TABLE p_table02_2017
PARTITION OF p_table02
FOR VALUES FROM ('2017-01-01 00:00:00') TO ('2018-01-01 00:00:00');

-- ok
create global temp table tbl_inherits_partition() inherits (tbl_inherits_parent);
create global temp table tbl_inherits_partition() inherits (tbl_inherits_parent_global_temp) on commit delete rows;

select relname ,relkind, relpersistence, reloptions from pg_class where relname like 'p_table0%' or  relname like 'tbl_inherits%' order by relname;

-- ERROR
create global temp table gtt3(a int primary key, b text) on commit drop;

-- ERROR
create global temp table gtt4(a int primary key, b text) with(on_commit_delete_rows=true) on commit delete rows;

-- ok
create global temp table gtt5(a int primary key, b text) with(on_commit_delete_rows=true);

--ok
alter table gtt_test_rename rename to gtt_test;

-- ok
ALTER TABLE gtt_test ADD COLUMN address varchar(30);

-- ERROR
CREATE TABLE orders (
    order_id integer PRIMARY KEY,
    product_no integer REFERENCES products (product_no),
    quantity integer
);

-- ERROR
CREATE global temp TABLE orders (
    order_id integer PRIMARY KEY,
    product_no integer REFERENCES products (product_no),
    quantity integer
);

-- ERROR
CREATE GLOBAL TEMPORARY TABLE mytable (
  id SERIAL PRIMARY KEY,
  data text
) on commit preserve rows;

-- ok
create global temp table gtt_seq(id int GENERATED ALWAYS AS IDENTITY (START WITH 2) primary key, a int)  on commit PRESERVE rows;
insert into gtt_seq (a) values(1);
insert into gtt_seq (a) values(2);
select * from gtt_seq order by id;
truncate gtt_seq;
select * from gtt_seq order by id;
insert into gtt_seq (a) values(3);
select * from gtt_seq order by id;

--ERROR
CREATE MATERIALIZED VIEW mv_gtt1 as select * from gtt1;

-- ALL ERROR
create index idx_err on gtt1 using hash (a);
create index idx_err on gtt1 using gist (a);
create index idx_tmp_t0_1 on tmp_t0 using gin (c0);
create index idx_tmp_t0_1 on tmp_t0 using gist (c0);

--ok
create global temp table gt (a SERIAL,b int);
begin;
set transaction_read_only = true;
insert into gt (b) values(1);
select * from gt;
commit;

--ok
create global temp table gt1(a int);
insert into gt1 values(generate_series(1,100000));
create index idx_gt1_1 on gt1 (a);
create index idx_gt1_2 on gt1((a + 1));
create index idx_gt1_3 on gt1((a*10),(a+a),(a-1));
explain select * from gt1 where a=1;
explain select * from gt1 where a=200000;
explain select * from gt1 where a*10=300;
explain select * from gt1 where a*10=3;
analyze gt1;
explain select * from gt1 where a=1;
explain select * from gt1 where a=200000;
explain select * from gt1 where a*10=300;
explain select * from gt1 where a*10=3;

reset search_path;

drop schema gtt_function cascade;

