use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Check  WAL for ZEROPAGE record.

sub command_output
{
	my ($cmd) = @_;
	my ($stdout, $stderr);
	print("# Running: " . join(" ", @{$cmd}) . "\n");
	my $result = IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;
	return $stdout;
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(extra => [ "--xid=3", "--multixact-id=3", "--multixact-offset=0" ]);;
$node->start;
my $pgdata = $node->data_dir;
my $xlogfilename0 = $node->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_current_wal_lsn())");
#$node->command_like(
#	[ 'pg_waldump', '-S', "$pgdata/pg_wal/$xlogfilename0" ],
#	qr/ZEROPAGE/,
#	'pg_waldump prints start timestamp');
my $wd_output = command_output(
    [ 'pg_waldump', "$pgdata/pg_wal/$xlogfilename0" ]);
ok($wd_output =~ qr/ZEROPAGE page 0/, "ZEROPAGE found");

done_testing();
