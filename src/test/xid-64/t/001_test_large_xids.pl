# Tests for large xid values
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

my $START_VAL = 2**32;
my $MAX_VAL = 2**62;

my $ixid = $START_VAL + int(rand($MAX_VAL - $START_VAL));
my $imxid = $START_VAL + int(rand($MAX_VAL - $START_VAL));
my $imoff = $START_VAL + int(rand($MAX_VAL - $START_VAL));

# Initialize master node with the random xid-related parameters
my $node = PostgreSQL::Test::Cluster->new('master');
$node->init(extra => [ "--xid=$ixid", "--multixact-id=$imxid", "--multixact-offset=$imoff" ]);
$node->start;

# Initialize master node and check the xid-related parameters
my $pgcd_output = command_output(
	[ 'pg_controldata', '-D', $node->data_dir ] );
print($pgcd_output); print('\n');
ok($pgcd_output =~ qr/Latest checkpoint's NextXID:\s*(\d+)/, "XID found");
my ($nextxid) = ($1);
ok($nextxid >= $ixid && $nextxid < $ixid + 1000,
	"Latest checkpoint's NextXID ($nextxid) is close to the initial xid ($ixid).");
ok($pgcd_output =~ qr/Latest checkpoint's NextMultiXactId:\s*(\d+)/, "MultiXactId found");
my ($nextmxid) = ($1);
ok($nextmxid >= $imxid && $nextmxid < $imxid + 1000,
	"Latest checkpoint's NextMultiXactId ($nextmxid) is close to the initial multiXactId ($imxid).");
ok($pgcd_output =~ qr/Latest checkpoint's NextMultiOffset:\s*(\d+)/, "MultiOffset found");
my ($nextmoff) = ($1);
ok($nextmoff >= $imoff && $nextmoff < $imoff + 1000,
	"Latest checkpoint's NextMultiOffset ($nextmoff) is close to the initial multiOffset ($imoff).");

# Run pgbench to check whether the database is working properly
$node->command_ok(
	[ qw(pgbench --initialize --no-vacuum --scale=10) ],
	  'pgbench finished without errors');

done_testing();