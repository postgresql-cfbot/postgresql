# Test cluster file encryption.
#

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

if ($ENV{with_openssl} eq 'yes')
{
	plan tests => 5;
}
else
{
	plan skip_all => "tests cannot run without OpenSSL";
}

# generate two cluster file encryption keys of random hex digits
my ($rand_hex, $rand_hex2);
$rand_hex  .= sprintf("%x", rand 16) for 1 .. 64;
$rand_hex2 .= sprintf("%x", rand 16) for 1 .. 64;

# initialize cluster using the first cluster key
my $node = get_new_node('node');
$node->init(
	extra => [
		'--file-encryption-method', 'AES256',
		'--cluster-key-command',    "echo $rand_hex"
	]);

# grab initdb output
my $logfile = slurp_file($TestLib::test_logfile);

# get debug lines
my @initdb_out = $logfile =~ m/^(CFE DEBUG:.*)$/mg;
ok(@initdb_out > 0, "initdb returned CFE DEBUG output");

# change "generated" to "decrypted" to match later entries
s/generated/decrypted/ for @initdb_out;

$node->start;

# We can run pg_alterckey and change the cluster_key_command here
# without affecting the running server.
system_or_bail(
	'pg_alterckey',
	"echo $rand_hex",
	"echo $rand_hex2",
	$node->data_dir);

$node->safe_psql('postgres',
	"ALTER SYSTEM SET cluster_key_command TO 'echo $rand_hex2'");

$node->stop;

# start/stop with new cluster key
$node->start;

$node->stop;

# get server log
$logfile = slurp_file($node->logfile());

# get server debug lines
my @server_out = $logfile =~ m/^(CFE DEBUG:.*)$/mg;
ok(@server_out > 0, "server start returned CFE DEBUG output");

# check line count
ok(@initdb_out == @server_out, "initdb has the same number of output lines");

# generate unique log values
my %initdb_hash = map { $_ => 1 } @initdb_out;
my %server_hash = map { $_ => 1 } @server_out;

# check that there are the same number of unique values
ok(keys %initdb_hash == keys %server_hash,
	"initdb and server logs have the same number of unique elements");

# check the unique values are the same
ok( join("\0", sort keys %initdb_hash) eq join("\0", sort keys %server_hash),
	"initdb and server logs have the same unique elements");
