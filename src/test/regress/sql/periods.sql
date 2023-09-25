/* System periods are not implemented */
create table pt (id integer, ds date, de date, period for system_time (ds, de));

/* Periods must specify actual columns */
create table pt (id integer, ds date, de date, period for p (bogus, de));
create table pt (id integer, ds date, de date, period for p (ds, bogus));

/* Data types must match exactly */
create table pt (id integer, ds date, de timestamp, period for p (ds, de));
create table pt (id integer, ds text collate "C", de text collate "POSIX", period for p (ds, de));

/* Periods must have a default BTree operator class */
create table pt (id integer, ds xml, de xml, period for p (ds, de));

/* Period and column names are in the same namespace */
create table pt (id integer, ds date, de date, period for ctid (ds, de));
create table pt (id integer, ds date, de date, period for id (ds, de));

/* Period name can't be given more than once */
create table pt (id integer, ds date, de date, period for p (ds, de), period for p (ds, de));

/* Now make one that works */
create table pt (id integer, ds date, de date, period for p (ds, de));

/* SELECT * excludes the PERIOD */
insert into pt values (1, '2000-01-01', '2001-01-01');
select * from pt;

/* You can get it if you want */
select *, p from pt;

/* Two are okay */
create table pt2 (id integer, ds date, de date, period for p1 (ds, de), period for p2 (ds, de));
drop table pt2;

/*
 * ALTER TABLE tests
 */
alter table pt drop period for p;
alter table pt add period for system_time (ds, de);
alter table pt add period for p (ds, de);

/* Adding a second one */
create table pt2 (id integer, ds date, de date, period for p1 (ds, de));
alter table pt2 add period for p2 (ds, de);
drop table pt2;

/* Can't drop its columns */
alter table pt drop column ds;
alter table pt drop column de;

/* Can't change the data types */
alter table pt alter column ds type timestamp;
alter table pt alter column ds type timestamp;

/* column/period namespace conflicts */
alter table pt add column p integer;
alter table pt rename column id to p;

/* adding columns and the period at the same time */
create table pt2 (id integer);
alter table pt2 add column ds date, add column de date, add period for p (ds, de);
drop table pt2;

/* Ambiguous range types raise an error */
create type mydaterange as range(subtype=date);
create table pt2 (id int, ds date, de date, period for p (ds, de));

/* You can give an explicit range type */
create table pt2 (id int, ds date, de date, period for p (ds, de) with (rangetype = 'mydaterange'));
drop type mydaterange;
drop type mydaterange cascade;
drop table pt2;
create table pt2 (id int, ds date, de date, period for p (ds, de) with (rangetype = 'daterange'));

/* Range type is not found */
create table pt3 (id int, ds date, de date, period for p (ds, de) with (rangetype = 'notarange'));

/* Range type is the wrong type */
create table pt3 (id int, ds date, de date, period for p (ds, de) with (rangetype = 'tstzrange'));
drop table pt2;

/* CREATE TABLE (LIKE ...) */

/* Periods are not copied by LIKE, so their columns aren't either */
create table pt2 (like pt);
\d pt2
drop table pt2;

/* Can add a period referring to LIKE'd columns */
create table not_p (id integer, ds date, de date);
create table pt2 (like not_p, period for p (ds, de));
\d pt2
drop table pt2;

/* Can add a period with the same name */
create table pt2 (like pt, period for p (ds, de));
\d pt2
drop table pt2;

/* Can add a period with a different name */
create table pt2 (like pt, period for p2 (ds, de));
\d pt2
drop table pt2;

/* Can't add a period whose name conflicts with a LIKE'd column */
create table pt2 (like pt, period for id (ds, de));

/* CREATE TALBE INHERITS */

/* Can't inherit from a table with a period */
create table pt2 (name text) inherits (pt);

/* Can't inherit with a period */
create table pt2 (d2s date, d2e date, period for p (d2s, d2e)) inherits (not_p);

drop table not_p;
