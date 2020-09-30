--
-- SI_ST_SM_SR
-- Test cases for recreating CREATE commands
--

CREATE TABLE smtest (id int NOT NULL PRIMARY KEY, type text NOT NULL, amt numeric NOT NULL);
CREATE VIEW smtestv AS SELECT type, sum(amt) AS totamt FROM smtest GROUP BY type;
CREATE MATERIALIZED VIEW smtestm AS SELECT type, sum(amt) AS totamt FROM smtest GROUP BY type WITH NO DATA;
CREATE MATERIALIZED VIEW smtestvm AS SELECT * FROM smtestv ORDER BY type;
\sm smtestm
\sm smtestvm
DROP TABLE smtest CASCADE;

create table pkeys (pkey1 int4 not null, pkey2 text not null);
create table fkeys (fkey1 int4, fkey2 text, fkey3 int);
create table fkeys2 (fkey21 int4, fkey22 text, pkey23 int not null);

create trigger check_fkeys_pkey_exist
	before insert or update on fkeys
	for each row
	execute function
	check_primary_key ('fkey1', 'fkey2', 'pkeys', 'pkey1', 'pkey2');
create trigger check_fkeys2_pkey_exist
	before insert or update on fkeys2
	for each row
	execute procedure
	check_primary_key ('fkey21', 'fkey22', 'pkeys', 'pkey1', 'pkey2');
create trigger check_fkeys2_pkey_exist
	before insert or update on fkeys
	for each row
	execute procedure
	check_primary_key ('fkey21', 'fkey22');
\st fkeys TRIGGER check_fkeys_pkey_exist
\st fkeys2 TRIGGER check_fkeys2_pkey_exist
\st fkeys TRIGGER check_fkeys2_pkey_exist
DROP TABLE pkeys;
DROP TABLE fkeys;
DROP TABLE fkeys2;
create table idxtable (a int, b int, c text);
create index idx on idxtable using hash (a);
create index idx2 on idxtable (c COLLATE "POSIX");
\si idx
\si idx2
drop index idx;
drop index idx2;
drop table idxtable;

CREATE TABLE collate_test (
    a int,
    b text COLLATE "C" NOT NULL
);
\sr collate_test
DROP TABLE collate_test;
CREATE TABLE ptif_test (a int, b int) PARTITION BY range (a);
CREATE TABLE ptif_test0 PARTITION OF ptif_test
  FOR VALUES FROM (minvalue) TO (0) PARTITION BY list (b);
\sr ptif_test
\sr ptif_test0
DROP TABLE ptif_test0;
DROP TABLE ptif_test;

CREATE TABLE srtest0 (aa TEXT);
CREATE TABLE srtest1 (bb TEXT) INHERITS (srtest0);
CREATE TABLE srtest2 (cc TEXT) INHERITS (srtest0);
CREATE TABLE srtest3 (dd TEXT) INHERITS (srtest1, srtest2, srtest0);
\sr srtest0
\sr srtest1
\sr srtest2
\sr srtest3
DROP TABLE srtest0 CASCADE;
CREATE TABLE srtest4 (id int, name text) WITH (fillfactor=10);
\sr srtest4
DROP TABLE srtest4;

CREATE TABLE constraint_test(
   ID INT PRIMARY KEY     NOT NULL,
   NAME           TEXT    NOT NULL,
   AGE            INT     NOT NULL UNIQUE,
   ADDRESS        CHAR(50),
   SALARY         REAL    DEFAULT 50000.00
);
\sr constraint_test
DROP TABLE constraint_test;
