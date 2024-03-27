# Copyright (c) 2024, PostgreSQL Global Development Group

# Test PostgreSQL backtrace related code.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;

# Turn off query statement logging so that we get to see the functions in only
# backtraces.
$node->append_conf(
	'postgresql.conf', qq{
backtrace_functions = 'pg_terminate_backend, pg_create_restore_point'
wal_level = replica
log_statement = none
log_min_error_statement = fatal
});
$node->start;

# Check if backtrace generation is supported on this installation.
my ($result, $stdout, $stderr);
my $log_offset = -s $node->logfile;

# Generate an error with negative timeout.
($result, $stdout, $stderr) = $node->psql(
	'postgres', q(
	SELECT pg_terminate_backend(123, -1);
));

if ($stderr =~ m/"timeout" must not be negative/
	&& $node->log_contains(
		qr/backtrace generation is not supported by this installation/,
		$log_offset))
{
	plan skip_all =>
	  'backtrace generation is not supported by this installation';
}

# Start verifying for the results only after skip_all check.
ok( $stderr =~ m/"timeout" must not be negative/,
	'error from terminating backend is logged');

my $backtrace_text = qr/BACKTRACE:  /;

ok( $node->log_contains($backtrace_text, $log_offset),
	"backtrace pg_terminate_backend start is found");

ok($node->log_contains("pg_terminate_backend", $log_offset),
	"backtrace pg_terminate_backend is found");

$log_offset = -s $node->logfile;

# Generate an error with a long restore point name.
($result, $stdout, $stderr) = $node->psql(
	'postgres', q(
	SELECT pg_create_restore_point(repeat('A', 1024));
));
ok( $stderr =~ m/value too long for restore point \(maximum .* characters\)/,
	'error from restore point function is logged');

ok( $node->log_contains($backtrace_text, $log_offset),
	"backtrace pg_create_restore_point start is found");

ok($node->log_contains("pg_create_restore_point", $log_offset),
	"backtrace pg_create_restore_point is found");

# Test if backtrace gets generated on internal errors when
# backtrace_on_internal_error is set. Note that we use a function (i.e.
# pg_replication_slot_advance) that generates an error with ereport without
# setting errcode explicitly, in which case elog.c assumes it as an internal
# error (see edata->sqlerrcode = ERRCODE_INTERNAL_ERROR; in errstart() in
# elog.c).
$node->append_conf('postgresql.conf', "backtrace_on_internal_error = on");
$node->reload;

$node->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('myslot', true);");

$log_offset = -s $node->logfile;

($result, $stdout, $stderr) = $node->psql(
	'postgres', q(
	SELECT pg_replication_slot_advance('myslot', '0/0'::pg_lsn);
));
ok($stderr =~ m/invalid target WAL LSN/,
	'error from replication slot advance is logged');

ok( $node->log_contains($backtrace_text, $log_offset),
	"backtrace pg_replication_slot_advance start is found");

ok($node->log_contains("pg_replication_slot_advance", $log_offset),
	"backtrace pg_replication_slot_advance is found");

done_testing();
