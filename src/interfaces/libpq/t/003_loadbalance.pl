# Copyright (c) 2022, PostgreSQL Global Development Group
use strict;
use warnings;
use Config;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use File::Spec::Functions 'catfile';
use Test::More;

# This tests two different methods of load balancing from libpq
# 1. Load balancing by providing multiple host and port combinations in the
#    libpq connection string.
# 2. By using a hosts file where hostname maps to multiple different IP
#    addresses. Regular Postgres users wouldn't usually use such a host file,
#    but this is the easiest way to immitate behaviour of a DNS server that
#    returns multiple IP addresses for the same DNS record.
#
# Testing method 1 is supported on all platforms and works out of the box. But
# testing method 2 has some more requirements, both on the platform and on the
# initial setup. If any of these requirements are not met, then method 2 is
# simply not tested.
#
# The requirements to test method 2 are as follows:
# 1. Windows or Linux should be used.
# 2. The OS hosts file at /etc/hosts or c:\Windows\System32\Drivers\etc\hosts
#    should contain the following contents:
#
# 127.0.0.1 pg-loadbalancetest
# 127.0.0.2 pg-loadbalancetest
# 127.0.0.3 pg-loadbalancetest
#
#
# Windows or Linux are required to test method 2 because these OSes allow
# binding to 127.0.0.2 and 127.0.0.3 addresess by default, but other OSes
# don't. We need to bind to different IP addresses, so that we can use these
# different IP addresses in the hosts file.
#
# The hosts file needs to be prepared before running this test. We don't do it
# on the fly, because it requires root permissions to change the hosts file. In
# CI we set up the previously mentioned rules in the hosts file, so that this
# load balancing method is tested.


# Cluster setup which is shared for testing both load balancing methods
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

# Start the tests for load balancing method 1
my $hostlist = $node1->host . ',' . $node2->host . ',' . $node3->host;
my $portlist = "$port,$port,$port";

$node1->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=123",
	"seed 123 selects node 1 first",
	sql => "SELECT 'connect1'",
	log_like => [qr/statement: SELECT 'connect1'/]);

$node2->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=123",
	"seed 123 does not select node 2 first",
	sql => "SELECT 'connect1'",
	log_unlike => [qr/statement: SELECT 'connect1'/]);

$node3->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=123",
	"seed 123 does not select node 3 first",
	sql => "SELECT 'connect1'",
	log_unlike => [qr/statement: SELECT 'connect1'/]);

$node3->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 selects node 3 first",
	sql => "SELECT 'connect2'",
	log_like => [qr/statement: SELECT 'connect2'/]);

$node1->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does not select node 1 first",
	sql => "SELECT 'connect2'",
	log_unlike => [qr/statement: SELECT 'connect2'/]);

$node2->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does not select node 2 first",
	sql => "SELECT 'connect2'",
	log_unlike => [qr/statement: SELECT 'connect2'/]);

$node3->stop();

$node1->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does select node 1 second",
	sql => "SELECT 'connect3'",
	log_like => [qr/statement: SELECT 'connect3'/]);

$node2->connect_ok("host=$hostlist port=$portlist load_balance_hosts=1 random_seed=42",
	"seed 42 does not select node 2 second",
	sql => "SELECT 'connect3'",
	log_unlike => [qr/statement: SELECT 'connect3'/]);

$node3->start();

# Checks for the requirements for testing load balancing method 2
if (!$can_bind_to_127_0_0_2) {
	# The OS requirement is not met
	done_testing();
	exit;
}

my $hosts_path;
if ($windows_os) {
	$hosts_path = 'c:\Windows\System32\Drivers\etc\hosts';
}
else
{
	$hosts_path = '/etc/hosts';
}

my $hosts_content = PostgreSQL::Test::Utils::slurp_file($hosts_path);

if ($hosts_content !~ m/pg-loadbalancetest/) {
	# Host file is not prepared for this test
	done_testing();
	exit;
}

# Start the tests for load balancing method 2
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

done_testing();
