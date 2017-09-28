--
-- UPDATE syntax tests
--

CREATE TABLE update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);

CREATE TABLE upsert_test (
    a   INT PRIMARY KEY,
    b   TEXT
);

INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);

SELECT * FROM update_test;

UPDATE update_test SET a = DEFAULT, b = DEFAULT;

SELECT * FROM update_test;

-- aliases for the UPDATE target table
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

SELECT * FROM update_test;

UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

SELECT * FROM update_test;

--
-- Test VALUES in FROM
--

UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

SELECT * FROM update_test;

-- fail, wrong data type:
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--
-- Test multiple-set-clause syntax
--

INSERT INTO update_test SELECT a,b+1,c FROM update_test;
SELECT * FROM update_test;

UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
SELECT * FROM update_test;
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
SELECT * FROM update_test;
-- fail, multi assignment to same column:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- uncorrelated sub-select:
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;
SELECT * FROM update_test;
-- correlated sub-select:
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);
SELECT * FROM update_test;
-- fail, multiple rows supplied:
UPDATE update_test SET (b,a) = (select a+1,b from update_test);
-- set to null if no rows supplied:
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;
SELECT * FROM update_test;
-- *-expansion should work in this context:
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;
-- you might expect this to work, but syntactically it's not a RowExpr:
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
SELECT a, b, char_length(c) FROM update_test;

-- Test ON CONFLICT DO UPDATE
INSERT INTO upsert_test VALUES(1, 'Boo');
-- uncorrelated  sub-select:
WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;
-- correlated sub-select:
INSERT INTO upsert_test VALUES (1, 'Baz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Correlated', a from upsert_test i WHERE i.a = upsert_test.a)
  RETURNING *;
-- correlated sub-select (EXCLUDED.* alias):
INSERT INTO upsert_test VALUES (1, 'Bat') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING *;

DROP TABLE update_test;
DROP TABLE upsert_test;


---------------------------
-- UPDATE with row movement
---------------------------

-- update to a partition should check partition bound constraint for the new tuple.
-- If partition key is updated, the row should be moved to the appropriate
-- partition. updatable views using partitions should enforce the check options
-- for the rows that have been moved.
create table mintab(c1 int);
insert into mintab values (120);
CREATE TABLE range_parted (
	a text,
	b int,
	c numeric
) partition by range (a, b);
CREATE VIEW upview AS SELECT * FROM range_parted WHERE (select c > c1 from mintab) WITH CHECK OPTION;

-- Create partitions intentionally in descending bound order, so as to test
-- that the sub plans are getting ordered in ascending bound order rather than ordered by the oid values.
create table part_b_10_b_20 partition of range_parted for values from ('b', 10) to ('b', 20) partition by range (c);
create table part_b_1_b_10 partition of range_parted for values from ('b', 1) to ('b', 10);
create table part_a_10_a_20 partition of range_parted for values from ('a', 10) to ('a', 20);
create table part_a_1_a_10 partition of range_parted for values from ('a', 1) to ('a', 10);

-- This tests partition-key UPDATE on a partitioned table that does not have any child partitions
update part_b_10_b_20 set b = b - 6;

-- As mentioned above, the partition creation is intentionally kept in descending bound order.
create table part_c_100_200 (c numeric, a text, b int);
alter table part_b_10_b_20 attach partition part_c_100_200 for values from (100) to (200);
create table part_c_1_100 (b int, c numeric, a text);
alter table part_b_10_b_20 attach partition part_c_1_100 for values from (1) to (100);

\set init_range_parted 'truncate range_parted; insert into range_parted values (''a'', 1, NULL), (''a'', 10, 200), (''b'', 12, 96), (''b'', 13, 97), (''b'', 15, 105), (''b'', 17, 105)'
:init_range_parted;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

-- The order of subplans should be in bound order
explain (costs off) update range_parted set c = c - 50 where c > 97;

-- fail (row movement happens only within the partition subtree) :
update part_c_1_100 set c = c + 20 where c = 96;
-- No row found :
update part_c_1_100 set c = c + 20 where c = 98;
-- ok (row movement)
update part_b_10_b_20 set c = c + 20 returning c, b, a;
select a, b, c from part_c_1_100 order by 1, 2, 3;
select a, b, c from part_c_100_200 order by 1, 2, 3;

-- fail (row movement happens only within the partition subtree) :
update part_b_10_b_20 set b = b - 6 where c > 116 returning *;
-- ok (row movement, with subset of rows moved into different partition)
update range_parted set b = b - 6 where c > 116 returning a, b + c;

select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

-- update partition key using updatable view.

-- succeeds
update upview set c = 199 where b = 4;
-- fail, check option violation
update upview set c = 120 where b = 4;
-- fail, row movement with check option violation
update upview set a = 'b', b = 15, c = 120 where b = 4;
-- succeeds, row movement , check option passes
update upview set a = 'b', b = 15 where b = 4;

select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

-- cleanup
drop view upview;

-- RETURNING having whole-row vars.
----------------------------------
:init_range_parted;
update range_parted set c = 95 where a = 'b' and b > 10 and c > 100 returning (range_parted)  , *;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;


-- Transition tables with update row movement
---------------------------------------------
:init_range_parted;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

create function trans_updatetrigfunc() returns trigger language plpgsql as
$$
  begin
    raise notice 'trigger = %, old table = %, new table = %',
                 TG_NAME,
                 (select string_agg(old_table::text, ', ' order by a) from old_table),
                 (select string_agg(new_table::text, ', ' order by a) from new_table);
    return null;
  end;
$$;

create trigger trans_updatetrig
  after update on range_parted referencing old table as old_table new table as new_table
  for each statement execute procedure trans_updatetrigfunc();

update range_parted set c = (case when c = 96 then 110 else c + 1 end ) where a = 'b' and b > 10 and c >= 96;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;
:init_range_parted;

-- Enabling OLD TABLE capture for both DELETE as well as UPDATE stmt triggers
-- should not cause DELETEd rows to be captured twice. Similar thing for
-- INSERT triggers and inserted rows.
create trigger trans_deletetrig
  after delete on range_parted referencing old table as old_table
  for each statement execute procedure trans_updatetrigfunc();
create trigger trans_inserttrig
  after insert on range_parted referencing new table as new_table
  for each statement execute procedure trans_updatetrigfunc();
update range_parted set c = c + 50 where a = 'b' and b > 10 and c >= 96;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;
drop trigger trans_updatetrig ON range_parted;
drop trigger trans_deletetrig ON range_parted;
drop trigger trans_inserttrig ON range_parted;

-- Install BR triggers on child partition, so that transition tuple conversion takes place.
create function func_parted_mod_b() returns trigger as $$
begin
   NEW.b = NEW.b + 1;
   return NEW;
end $$ language plpgsql;
create trigger trig_c1_100 before update or insert on part_c_1_100
   for each row execute procedure func_parted_mod_b();
create trigger trig_c100_200 before update or insert on part_c_100_200
   for each row execute procedure func_parted_mod_b();
:init_range_parted;
update range_parted set c = (case when c = 96 then 110 else c + 1 end ) where a = 'b' and b > 10 and c >= 96;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;
:init_range_parted;
update range_parted set c = c + 50 where a = 'b' and b > 10 and c >= 96;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;
drop trigger trig_c1_100 ON part_c_1_100;
drop trigger trig_c100_200 ON part_c_100_200;
drop function func_parted_mod_b();


-- statement triggers with update row movement
---------------------------------------------------

:init_range_parted;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

create function trigfunc() returns trigger language plpgsql as
$$
  begin
    raise notice 'trigger = % fired on table % during %',
                 TG_NAME, TG_TABLE_NAME, TG_OP;
    return null;
  end;
$$;
-- Triggers on root partition
create trigger parent_delete_trig
  after delete on range_parted for each statement execute procedure trigfunc();
create trigger parent_update_trig
  after update on range_parted for each statement execute procedure trigfunc();
create trigger parent_insert_trig
  after insert on range_parted for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_c_1_100
create trigger c1_delete_trig
  after delete on part_c_1_100 for each statement execute procedure trigfunc();
create trigger c1_update_trig
  after update on part_c_1_100 for each statement execute procedure trigfunc();
create trigger c1_insert_trig
  after insert on part_c_1_100 for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_c_100_200
create trigger c100_delete_trig
  after delete on part_c_100_200 for each statement execute procedure trigfunc();
create trigger c100_update_trig
  after update on part_c_100_200 for each statement execute procedure trigfunc();
create trigger c100_insert_trig
  after insert on part_c_100_200 for each statement execute procedure trigfunc();

-- Move all rows from part_c_100_200 to part_c_1_100. None of the delete or insert statement triggers should be fired.
update range_parted set c = c - 50 where c > 97;
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

drop trigger parent_delete_trig ON range_parted;
drop trigger parent_update_trig ON range_parted;
drop trigger parent_insert_trig ON range_parted;
drop trigger c1_delete_trig ON part_c_1_100;
drop trigger c1_update_trig ON part_c_1_100;
drop trigger c1_insert_trig ON part_c_1_100;
drop trigger c100_delete_trig ON part_c_100_200;
drop trigger c100_update_trig ON part_c_100_200;
drop trigger c100_insert_trig ON part_c_100_200;

drop table mintab;


-- Creating default partition for range
:init_range_parted;
create table part_def partition of range_parted default;
\d+ part_def
insert into range_parted values ('c', 9);
-- ok
update part_def set a = 'd' where a = 'c';
-- fail
update part_def set a = 'a' where a = 'd';

select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

-- Update row movement from non-default to default partition.
-- Fail, default partition is not under part_a_10_a_20;
update part_a_10_a_20 set a = 'ad' where a = 'a';
-- Success
update range_parted set a = 'ad' where a = 'a';
update range_parted set a = 'bd' where a = 'b';
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;
-- Update row movement from default to non-default partitions.
-- Success
update range_parted set a = 'a' where a = 'ad';
update range_parted set a = 'b' where a = 'bd';
select tableoid::regclass::text partname, * from range_parted order by 1, 2, 3, 4;

create table list_parted (
	a text,
	b int
) partition by list (a);
create table list_part1  partition of list_parted for values in ('a', 'b');
create table list_default partition of list_parted default;
insert into list_part1 values ('a', 1);
insert into list_default values ('d', 10);

-- fail
update list_default set a = 'a' where a = 'd';
-- ok
update list_default set a = 'x' where a = 'd';

drop table list_parted;

--------------
-- UPDATE with
-- partition key or non-partition columns, with different column ordering,
-- triggers.
--------------

-- Setup
--------
create table list_parted (a numeric, b int, c int8) partition by list (a);
create table sub_parted partition of list_parted for values in (1) partition by list (b);

create table sub_part1(b int, c int8, a numeric);
alter table sub_parted attach partition sub_part1 for values in (1);
create table sub_part2(b int, c int8, a numeric);
alter table sub_parted attach partition sub_part2 for values in (2);

create table list_part1(a numeric, b int, c int8);
alter table list_parted attach partition list_part1 for values in (2,3);

insert into list_parted values (2,5,50);
insert into list_parted values (3,6,60);
insert into sub_parted values (1,1,60);
insert into sub_parted values (1,2,10);

-- Test partition constraint violation when intermediate ancestor is used and
-- constraint is inherited from upper root.
update sub_parted set a = 2 where c = 10;

-- UPDATE which does not modify partition key of partitions that are chosen for update.
select tableoid::regclass::text , * from list_parted where a = 2 order by 1;
update list_parted set b = c + a where a = 2;
select tableoid::regclass::text , * from list_parted where a = 2 order by 1;


-----------
-- Triggers can cause UPDATE row movement if it modified partition key.
-----------
create function func_parted_mod_b() returns trigger as $$
begin
   NEW.b = 2; -- This is changing partition key column.
   return NEW;
end $$ language plpgsql;
create trigger parted_mod_b before update on sub_part1
   for each row execute procedure func_parted_mod_b();

select tableoid::regclass::text , * from list_parted order by 1, 2, 3, 4;

-- This should do the tuple routing even though there is no explicit
-- partition-key update, because there is a trigger on sub_part1
update list_parted set c = 70 where b  = 1 ;
select tableoid::regclass::text , * from list_parted order by 1, 2, 3, 4;

drop trigger parted_mod_b ON sub_part1 ;

-- If BR DELETE trigger prevented DELETE from happening, we should also skip
-- the INSERT if that delete is part of UPDATE=>DELETE+INSERT.
create or replace function func_parted_mod_b() returns trigger as $$
begin return NULL; end $$ language plpgsql;
create trigger trig_skip_delete before delete on sub_part1
   for each row execute procedure func_parted_mod_b();
update list_parted set b = 1 where c = 70;
select tableoid::regclass::text , * from list_parted order by 1, 2, 3, 4;

drop trigger trig_skip_delete ON sub_part1 ;

-- UPDATE partition-key with FROM clause. If join produces multiple output
-- rows for the same row to be modified, we should tuple-route the row only once.
-- There should not be any rows inserted.
create table non_parted (id int);
insert into non_parted values (1), (1), (1), (2), (2), (2), (3), (3), (3);
update list_parted t1 set a = 2 from non_parted t2 where t1.a = t2.id and a = 1;
select tableoid::regclass::text , * from list_parted order by 1, 2, 3, 4;
drop table non_parted;

drop function func_parted_mod_b ( ) ;
drop table range_parted;
drop table list_parted;
