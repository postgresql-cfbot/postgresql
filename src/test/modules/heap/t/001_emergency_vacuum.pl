# Copyright (c) 2022, PostgreSQL Global Development Group

# Test for wraparound emergency situation

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('main');

$node->init;
$node->append_conf('postgresql.conf', qq[
autovacuum = off # run autovacuum only when to anti wraparound
max_prepared_transactions = 10
autovacuum_naptime = 1s
# so it's easier to verify the order of operations
autovacuum_max_workers = 1
log_autovacuum_min_duration = 0
]);
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION test_heap');

# Create tables for a few different test scenarios
$node->safe_psql('postgres', qq[
CREATE TABLE large(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO large(data) SELECT generate_series(1,30000);

CREATE TABLE large_trunc(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO large_trunc(data) SELECT generate_series(1,30000);

CREATE TABLE small(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO small(data) SELECT generate_series(1,15000);

CREATE TABLE small_trunc(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO small_trunc(data) SELECT generate_series(1,15000);

CREATE TABLE autovacuum_disabled(id serial primary key, data text) WITH (autovacuum_enabled=false);
INSERT INTO autovacuum_disabled(data) SELECT generate_series(1,1000);
]);

# To prevent autovacuum from handling the tables immediately after
# restart, acquire locks in a 2PC transaction. That allows us to test
# interactions with running commands.
$node->safe_psql('postgres', qq[
BEGIN;
LOCK TABLE large IN SHARE UPDATE EXCLUSIVE MODE;
LOCK TABLE large_trunc IN SHARE UPDATE EXCLUSIVE MODE;
LOCK TABLE small IN SHARE UPDATE EXCLUSIVE MODE;
LOCK TABLE small_trunc IN SHARE UPDATE EXCLUSIVE MODE;
LOCK TABLE autovacuum_disabled IN SHARE UPDATE EXCLUSIVE MODE;
PREPARE TRANSACTION 'prevent-vacuum';
]);

# Delete a few rows to ensure that vacuum has work to do.
$node->safe_psql('postgres', qq[
DELETE FROM large WHERE id % 2 = 0;
DELETE FROM large_trunc WHERE id > 10000;
DELETE FROM small WHERE id % 2 = 0;
DELETE FROM small_trunc WHERE id > 1000;
DELETE FROM autovacuum_disabled WHERE id % 2 = 0;
]);

# New XID needs to be a clog page boundary, otherwise we'll get errors about
# the file not exisitng error. With default compilation settings
# CLOG_XACTS_PER_PAGE is 32768. The value below is 32768 *
# (2000000000/32768 + 1), with 2000000000 being the max value for
# autovacuum_freeze_max_age.  Since the prepared transaction keeps holding the
# lock on tables above, autovacuum won't run
$node->safe_psql('postgres', qq[SELECT set_next_xid('2000027648'::xid)]);

# Make sure updating the latest completed with the advanced XID.
$node->safe_psql('postgres', qq[INSERT INTO small(data) SELECT 1]);

# Check if all databases became old now.
my $ret = $node->safe_psql('postgres',
			   qq[
SELECT datname,
       age(datfrozenxid) > current_setting('autovacuum_freeze_max_age')::int as old
FROM pg_database ORDER BY 1
]);
is($ret, "postgres|t
template0|t
template1|t", "all tables became old");

# Allow autovacuum to start working on these tables.
$node->safe_psql('postgres', qq[COMMIT PREPARED 'prevent-vacuum']);

$node->poll_query_until('postgres',
			qq[
SELECT NOT EXISTS (
	SELECT *
	FROM pg_database
	WHERE age(datfrozenxid) > current_setting('autovacuum_freeze_max_age')::int)
]) or die "timeout waiting all database are vacuumed";

# Check if these tables are vacuumed.
$ret = $node->safe_psql('postgres', qq[
SELECT relname, age(relfrozenxid) > current_setting('autovacuum_freeze_max_age')::int
FROM pg_class
WHERE oid = ANY(ARRAY['large'::regclass, 'large_trunc', 'small', 'small_trunc', 'autovacuum_disabled'])
ORDER BY 1
]);

is($ret, "autovacuum_disabled|f
large|f
large_trunc|f
small|f
small_trunc|f", "all tables are vacuumed");

$node->stop;

done_testing();
