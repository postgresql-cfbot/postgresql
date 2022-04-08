use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Socket qw(AF_INET AF_INET6 inet_pton);
use IO::Socket;

plan tests => 25;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
log_connections = on
});
$node->append_conf(
	'pg_hba.conf', qq{
host all all 11.22.33.44/32 trust
host all all 1:2:3:4:5:6:0:9/128 trust
});
$node->append_conf('postgresql.conf', "proxy_port = " . ($node->port() + 1));

$node->start;

$node->safe_psql('postgres', 'CREATE USER proxytest;');

sub make_message
{
	my ($msg) = @_;
	return pack("Na*", length($msg) + 4, $msg);
}

sub read_packet
{
	my ($socket) = @_;
	my $buf = "";
	$socket->recv($buf, 1024);
	return $buf;
}


# Test normal connection through localhost
sub test_connection
{
	my ($socket, $proxy, $what, $shouldbe, $shouldfail, $extra) = @_;
	ok($socket, $what);

	my $startup = make_message(
		pack("N(Z*Z*)*x", 196608, (user => "proxytest", database => "postgres")));

	$extra = "" if !defined($extra);

	if (defined($proxy))
	{
		my $p = "\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A\x21";
		if ($proxy =~ ":")
		{
			# ipv6
			$p .= "\x21";                        # TCP v6
			$p .= pack "n", 36 + length($extra); # size
			$p .= inet_pton(AF_INET6, $proxy);
			$p .= "\0" x 16;                     # destination address
		}
		else
		{
			# ipv4
			$p .= "\x11";                        # TCP v4
			$p .= pack "n", 12 + length($extra); # size
			$p .= inet_pton(AF_INET, $proxy);
			$p .= "\0\0\0\0";                    # destination address
		}
		$p .= pack "n", 1919;                    # source port
		$p .= pack "n", 0;
		$p .= $extra;
		print $socket $p;
	}
	print $socket $startup;

	my $in = read_packet($socket);
	if (defined($shouldfail))
	{
		isnt(substr($in, 0, 1), 'R', $what);
	}
	else
	{
		is(substr($in, 0, 1), 'R', $what);
	}

  SKIP:
	{
		skip "The rest of this test should fail", 3 if (defined($shouldfail));

		is(substr($in, 8, 1), "\0", $what);

		my ($resip, $resport) = split /\|/,
		  $node->safe_psql('postgres',
			"SELECT client_addr, client_port FROM pg_stat_activity WHERE pid != pg_backend_pid() AND backend_type='client backend'"
		  );
		is($resip, $shouldbe, $what);
		if ($proxy)
		{
			is($resport, "1919", $what);
		}
		else
		{
			ok($resport, $what);
		}
	}

	$socket->close();

	return;
}

sub make_socket
{
	my ($port) = @_;
	if ($PostgreSQL::Test::Cluster::use_tcp) {
		return IO::Socket::INET->new(
			PeerAddr => "127.0.0.1",
			PeerPort => $port,
			Proto    => "tcp",
			Type     => SOCK_STREAM);
	}
	else {
		return IO::Socket::UNIX->new(
			Peer => $node->host() . "/.s.PGSQL." . $port,
			Type => SOCK_STREAM);
	}
}

# Test a regular connection first to make sure connecting etc works fine.
test_connection(make_socket($node->port()),
	undef, "normal connection", $PostgreSQL::Test::Cluster::use_tcp ? "127.0.0.1": "");

# Make sure we can't make a proxy connection until it's allowed
test_connection(make_socket($node->port() + 1),
	"11.22.33.44", "proxy ipv4", "11.22.33.44", 1);

# Allow proxy connections and test them
$node->append_conf('postgresql.conf', "proxy_servers = 'unix, 127.0.0.1/32'");
$node->restart();

test_connection(make_socket($node->port() + 1),
	"11.22.33.44", "proxy ipv4", "11.22.33.44");
test_connection(make_socket($node->port() + 1),
	"1:2:3:4:5:6::9", "proxy ipv6", "1:2:3:4:5:6:0:9");

test_connection(make_socket($node->port() + 1),
    "11.22.33.44", "proxy with extra", "11.22.33.44", undef, "abcdef"x100);
