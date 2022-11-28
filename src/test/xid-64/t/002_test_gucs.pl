# Tests for guc boundary values
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use bigint;

sub command_output
{
	my ($cmd) = @_;
	my ($stdout, $stderr);
	print("# Running: " . join(" ", @{$cmd}) . "\n");
	my $result = IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;
	ok($result, "@$cmd exit code 0");
	is($stderr, '', "@$cmd no stderr");
	return $stdout;
}

sub set_guc
{
	my ($node, $guc, $val) = @_;
	print("SET $guc = $val\n");
	$node->safe_psql('postgres', "ALTER SYSTEM SET $guc = $val");
	$node->restart();
}

sub test_pgbench
{
	my ($node) = @_;
	$node->command_ok(
		[ qw(pgbench --progress=5 --transactions=1000 --jobs=5 --client=5) ],
		  'pgbench finished without errors');
}

my @guc_vals = (
	[ "autovacuum_freeze_max_age", 100000, 2**63 - 1 ],
	[ "autovacuum_multixact_freeze_max_age", 10000, 2**63 - 1 ],
	[ "vacuum_freeze_min_age", 0, 2**63 - 1 ],
	[ "vacuum_freeze_table_age", 0, 2**63 - 1 ],
	[ "vacuum_multixact_freeze_min_age", 0, 2**63 - 1 ],
	[ "vacuum_multixact_freeze_table_age", 0, 2**63 -1 ]
);

my $START_VAL = 2**32;
my $MAX_VAL = 2**62;

my $ixid = $START_VAL + int(rand($MAX_VAL - $START_VAL));
my $imxid = $START_VAL + int(rand($MAX_VAL - $START_VAL));
my $imoff = $START_VAL + int(rand($MAX_VAL - $START_VAL));

# Initialize master node
my $node = PostgreSQL::Test::Cluster->new('master');
$node->init(extra => [ "--xid=$ixid", "--multixact-id=$imxid", "--multixact-offset=$imoff" ]);
# Disable logging of all statements to avoid log bloat during pgbench
$node->append_conf('postgresql.conf', "log_statement = none");
$node->start;

# Fill the test database with the pgbench data
$node->command_ok(
	[ qw(pgbench --initialize --scale=10) ],
	  'pgbench finished without errors');

# Test all GUCs with minimum, maximum and random value inbetween
#  (run pgbench for every configuration setting)
foreach my $gi (0 .. $#guc_vals) {
	print($guc_vals[$gi][0]); print("\n");
	my $guc = $guc_vals[$gi][0];
	my $minval = $guc_vals[$gi][1];
	my $maxval = $guc_vals[$gi][2];
	set_guc($node, $guc, $minval);
	test_pgbench($node);
	set_guc($node, $guc, $maxval);
	test_pgbench($node);
	set_guc($node, $guc, $minval + int(rand($maxval - $minval)));
	test_pgbench($node);
}

done_testing();