# Copyright (c) 2022, PostgreSQL Global Development Group
use strict;
use warnings;
use Config;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use File::Spec::Functions 'catfile';
use Test::More;

# To test load balancing when there's a single host that resolves to different
# IPs we use addresess 127.0.0.2 and 127.0.0.3. Linux and Windows allow binding
# to these addresess by default, but other OSes don't. We skip these tests on
# platforms that don't support this.
my $can_bind_to_127_0_0_2 = $Config{osname} eq 'linux' || $PostgreSQL::Test::Utils::windows_os;

if ($can_bind_to_127_0_0_2)
{
	$PostgreSQL::Test::Cluster::use_tcp = 1;
	$PostgreSQL::Test::Cluster::test_pghost = '127.0.0.1';
}
my $port = PostgreSQL::Test::Cluster::get_free_port();
my $node1 = PostgreSQL::Test::Cluster->new('node1', port => $port);
my $node2 = PostgreSQL::Test::Cluster->new('node2', port => $port, own_host => 1);
my $node3 = PostgreSQL::Test::Cluster->new('node3', port => $port, own_host => 1);

# Create a data directory with initdb
$node1->init();
$node2->init();
$node3->init();

# Start the PostgreSQL server
$node1->start();
$node2->start();
$node3->start();
my $host = $node1->host . ',' . $node2->host . ',' . $node3->host;
my $portlist = "$port,$port,$port";

$node1->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=123",
	"seed 123 selects node 1 first",
	sql => "SELECT 'connect1'",
	log_like => [qr/statement: SELECT 'connect1'/]);

$node2->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=123",
	"seed 123 does not select node 2 first",
	sql => "SELECT 'connect1'",
	log_unlike => [qr/statement: SELECT 'connect1'/]);

$node3->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=123",
	"seed 123 does not select node 3 first",
	sql => "SELECT 'connect1'",
	log_unlike => [qr/statement: SELECT 'connect1'/]);

$node3->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 selects node 3 first",
	sql => "SELECT 'connect2'",
	log_like => [qr/statement: SELECT 'connect2'/]);

$node1->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does not select node 1 first",
	sql => "SELECT 'connect2'",
	log_unlike => [qr/statement: SELECT 'connect2'/]);

$node2->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does not select node 2 first",
	sql => "SELECT 'connect2'",
	log_unlike => [qr/statement: SELECT 'connect2'/]);

$node3->stop();

$node1->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does select node 1 second",
	sql => "SELECT 'connect3'",
	log_like => [qr/statement: SELECT 'connect3'/]);

$node2->connect_ok("host=$host port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does not select node 2 second",
	sql => "SELECT 'connect3'",
	log_unlike => [qr/statement: SELECT 'connect3'/]);

$node3->start();

if ($can_bind_to_127_0_0_2) {
	# To make the following tests run and pass add the following content to your
	# hosts file:
	# 127.0.0.1 pg-loadbalancetest
	# 127.0.0.2 pg-loadbalancetest
	# 127.0.0.3 pg-loadbalancetest
	#
	# If the hosts file doesn't contain the hostname we expect, then we skip
	# these tests. In CI we set up these specific rules in the hosts files to
	# test this load balancing behaviour. But users running our test suite
	# locally should not be forced to do so to make all the tests pass.
	my $hosts_path;
	if ($windows_os) {
		$hosts_path = 'c:\Windows\System32\Drivers\etc\hosts';
	}
	else
	{
		$hosts_path = '/etc/hosts';
	}

	my $hosts_content = PostgreSQL::Test::Utils::slurp_file($hosts_path);

	if ($hosts_content =~ m/pg-loadbalancetest/) {
		$node2->connect_ok("host=pg-loadbalancetest port=$port load_balance_hosts=1 random_seed=44",
			"seed 44 selects node 2 first",
			sql => "SELECT 'connect4'",
			log_like => [qr/statement: SELECT 'connect4'/]);

		$node1->connect_ok("host=pg-loadbalancetest port=$port load_balance_hosts=1 random_seed=44",
			"seed 44 does not select node 1 first",
			sql => "SELECT 'connect4'",
			log_unlike => [qr/statement: SELECT 'connect4'/]);

		$node3->connect_ok("host=pg-loadbalancetest port=$port load_balance_hosts=1 random_seed=44",
			"seed 44 does not select node 3 first",
			sql => "SELECT 'connect4'",
			log_unlike => [qr/statement: SELECT 'connect4'/]);

		$node2->stop();

		$node1->connect_ok("host=pg-loadbalancetest port=$port load_balance_hosts=1 random_seed=44",
			"seed 44 does select node 1 second",
			sql => "SELECT 'connect5'",
			log_like => [qr/statement: SELECT 'connect5'/]);

		$node3->connect_ok("host=pg-loadbalancetest port=$port load_balance_hosts=1 random_seed=44",
			"seed 44 does not select node 3 second",
			sql => "SELECT 'connect5'",
			log_unlike => [qr/statement: SELECT 'connect5'/]);
	}
}

done_testing();
