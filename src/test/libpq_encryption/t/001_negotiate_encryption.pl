
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Test negotiation of SSL and GSSAPI encryption

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Kerberos;
use File::Basename;
use File::Copy;
use Test::More;

if (!$ENV{PG_TEST_EXTRA} || $ENV{PG_TEST_EXTRA} !~ /\blibpq_encryption\b/)
{
	plan skip_all =>
	  'Potentially unsafe test libpq_encryption not enabled in PG_TEST_EXTRA';
}

my $host = 'enc-test-localhost.postgresql.example.com';
my $hostaddr = '127.0.0.1';
my $servercidr = '127.0.0.1/32';

note "setting up PostgreSQL instance";

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
listen_addresses = '$hostaddr'
log_connections = on
lc_messages = 'C'
});
my $pgdata = $node->data_dir;

my $dbname = 'postgres';
my $username = 'enctest';
my $application = '001_negotiate_encryption.pl';

my $gssuser_password = 'secret1';

my $krb;

my $ssl_supported = $ENV{with_ssl} eq 'openssl';
my $gss_supported = $ENV{with_gssapi} eq 'yes';

if ($ENV{with_gssapi} eq 'yes')
{
	note "setting up Kerberos";

	my $realm = 'EXAMPLE.COM';
	$krb = PostgreSQL::Test::Kerberos->new($host, $hostaddr, $realm);
	$node->append_conf('postgresql.conf', "krb_server_keyfile = '$krb->{keytab}'\n");
}

if ($ENV{with_ssl} eq 'openssl')
{
	my $certdir = dirname(__FILE__) . "/../../ssl/ssl";

	copy "$certdir/server-cn-only.crt", "$pgdata/server.crt"
	  || die "copying server.crt: $!";
	copy "$certdir/server-cn-only.key", "$pgdata/server.key"
	  || die "copying server.key: $!";
	chmod(0600, "$pgdata/server.key");

	# Start with SSL disabled.
	$node->append_conf('postgresql.conf', "ssl = off\n");
}

$node->start;

$node->safe_psql('postgres', 'CREATE USER localuser;');
$node->safe_psql('postgres', 'CREATE USER testuser;');
$node->safe_psql('postgres', 'CREATE USER ssluser;');
$node->safe_psql('postgres', 'CREATE USER nossluser;');
$node->safe_psql('postgres', 'CREATE USER gssuser;');
$node->safe_psql('postgres', 'CREATE USER nogssuser;');

my $unixdir = $node->safe_psql('postgres', 'SHOW unix_socket_directories;');
chomp($unixdir);

$node->safe_psql('postgres', q{
CREATE FUNCTION current_enc() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  ssl_in_use bool;
  gss_in_use bool;
BEGIN
  ssl_in_use = (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid());
  gss_in_use = (SELECT encrypted FROM pg_stat_gssapi WHERE pid = pg_backend_pid());

  raise log 'ssl %  gss %', ssl_in_use, gss_in_use;

  IF ssl_in_use AND gss_in_use THEN
    RETURN 'ssl+gss';   -- shouldn't happen
  ELSIF ssl_in_use THEN
    RETURN 'ssl';
  ELSIF gss_in_use THEN
    RETURN 'gss';
  ELSE
    RETURN 'plain';
  END IF;
END;
$$;
});

# Only accept SSL connections from $servercidr. Our tests don't depend on this
# but seems best to keep it as narrow as possible for security reasons.
#
# When connecting to certdb, also check the client certificate.
open my $hba, '>', "$pgdata/pg_hba.conf";
print $hba qq{
# TYPE        DATABASE        USER            ADDRESS                 METHOD             OPTIONS
local         postgres        localuser                               trust
host          postgres        testuser        $servercidr             trust
hostnossl     postgres        nossluser       $servercidr             trust
hostnogssenc  postgres        nogssuser       $servercidr             trust
};

print $hba qq{
hostssl       postgres        ssluser         $servercidr             trust
} if ($ENV{with_ssl} eq 'openssl');

print $hba qq{
hostgssenc    postgres        gssuser         $servercidr             trust
} if ($ENV{with_gssapi} eq 'yes');
close $hba;
$node->reload;

note "running tests";

sub connect_test
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $connstr, $expected_enc, @expect_log_msgs)
	  = @_;

	my %params = ();

	if (@expect_log_msgs)
	{
		# Match every message literally.
		my @regexes = map { qr/\Q$_\E/ } @expect_log_msgs;

		$params{log_like} = \@regexes;
	}

	my $test_name = "'$connstr' -> $expected_enc";

	my $connstr_full = "";
	$connstr_full .= "dbname=postgres " unless $connstr =~ m/dbname=/;
	$connstr_full .= "host=$host hostaddr=$hostaddr " unless $connstr =~ m/host=/;
	$connstr_full .= $connstr;

	if ($expected_enc eq "fail")
	{
		$node->connect_fails($connstr_full, $test_name, %params);
	}
	else
	{
		$params{sql} = "SELECT current_enc()";
		$params{expected_stdout} = qr/^$expected_enc$/;;

		$node->connect_ok($connstr_full, $test_name, %params);
	}
}


# First test with SSL disabled in the server
connect_test($node, 'user=testuser sslmode=disable', 'plain');
connect_test($node, 'user=testuser sslmode=allow', 'plain');
connect_test($node, 'user=testuser sslmode=prefer', 'plain');
connect_test($node, 'user=testuser sslmode=require', 'fail');

connect_test($node, 'user=testuser sslmode=disable sslnegotiation=direct', 'plain');
connect_test($node, 'user=testuser sslmode=allow sslnegotiation=direct', 'plain');
connect_test($node, 'user=testuser sslmode=prefer sslnegotiation=direct', 'plain');
connect_test($node, 'user=testuser sslmode=require sslnegotiation=direct', 'fail');

connect_test($node, 'user=testuser sslmode=disable sslnegotiation=requiredirect', 'plain');
connect_test($node, 'user=testuser sslmode=allow sslnegotiation=requiredirect', 'plain');
connect_test($node, 'user=testuser sslmode=prefer sslnegotiation=requiredirect', 'plain');
connect_test($node, 'user=testuser sslmode=require sslnegotiation=requiredirect', 'fail');

# Enable SSL in the server
SKIP:
{
	skip "SSL not supported by this build" unless $ssl_supported;

	$node->adjust_conf('postgresql.conf', 'ssl', 'on');
	$node->restart;

	connect_test($node, 'user=testuser sslmode=disable', 'plain');
	connect_test($node, 'user=testuser sslmode=allow', 'plain');
	connect_test($node, 'user=testuser sslmode=prefer', 'ssl');
	connect_test($node, 'user=testuser sslmode=require', 'ssl');

	connect_test($node, 'user=testuser sslmode=disable sslnegotiation=direct', 'plain');
	connect_test($node, 'user=testuser sslmode=allow sslnegotiation=direct', 'plain');
	connect_test($node, 'user=testuser sslmode=prefer sslnegotiation=direct', 'ssl');
	connect_test($node, 'user=testuser sslmode=require sslnegotiation=direct', 'ssl');

	connect_test($node, 'user=testuser sslmode=disable sslnegotiation=requiredirect', 'plain');
	connect_test($node, 'user=testuser sslmode=allow sslnegotiation=requiredirect', 'plain');
	connect_test($node, 'user=testuser sslmode=prefer sslnegotiation=requiredirect', 'ssl');
	connect_test($node, 'user=testuser sslmode=require sslnegotiation=requiredirect', 'ssl');

	connect_test($node, 'user=ssluser sslmode=disable', 'fail');
	connect_test($node, 'user=ssluser sslmode=allow', 'ssl');
	connect_test($node, 'user=ssluser sslmode=prefer', 'ssl');
	connect_test($node, 'user=ssluser sslmode=require', 'ssl');

	connect_test($node, 'user=ssluser sslmode=disable sslnegotiation=direct', 'fail');
	connect_test($node, 'user=ssluser sslmode=allow sslnegotiation=direct', 'ssl');
	connect_test($node, 'user=ssluser sslmode=prefer sslnegotiation=direct', 'ssl');
	connect_test($node, 'user=ssluser sslmode=require sslnegotiation=direct', 'ssl');

	connect_test($node, 'user=ssluser sslmode=disable sslnegotiation=requiredirect', 'fail');
	connect_test($node, 'user=ssluser sslmode=allow sslnegotiation=requiredirect', 'ssl');
	connect_test($node, 'user=ssluser sslmode=prefer sslnegotiation=requiredirect', 'ssl');
	connect_test($node, 'user=ssluser sslmode=require sslnegotiation=requiredirect', 'ssl');

	connect_test($node, 'user=nossluser sslmode=disable', 'plain');
	connect_test($node, 'user=nossluser sslmode=allow', 'plain');
	connect_test($node, 'user=nossluser sslmode=prefer', 'plain');
	connect_test($node, 'user=nossluser sslmode=require', 'fail');

	$node->adjust_conf('postgresql.conf', 'ssl', 'off');
	$node->reload;
}

# Test GSSAPI
SKIP:
{
	skip "GSSAPI/Kerberos not supported by this build" unless $ENV{with_gssapi} eq 'yes';

	# No ticket
	connect_test($node, 'user=testuser sslmode=disable gssencmode=require', 'fail');

	$krb->create_principle('gssuser', $gssuser_password);
	$krb->create_ticket('gssuser', $gssuser_password);

	connect_test($node, 'user=testuser sslmode=disable gssencmode=disable', 'plain');
	connect_test($node, 'user=testuser sslmode=disable gssencmode=prefer', 'gss');
	connect_test($node, 'user=testuser sslmode=disable gssencmode=require', 'gss');

	connect_test($node, 'user=testuser sslmode=prefer gssencmode=disable', 'plain');
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=prefer', 'gss');
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=require', 'gss');

	connect_test($node, 'user=testuser sslmode=prefer sslnegotiation=direct gssencmode=disable', 'plain');
	connect_test($node, 'user=testuser sslmode=prefer sslnegotiation=direct gssencmode=prefer', 'gss');
	connect_test($node, 'user=testuser sslmode=prefer sslnegotiation=direct gssencmode=require', 'gss');

	connect_test($node, 'user=testuser sslmode=require sslnegotiation=direct gssencmode=disable', 'fail');
	connect_test($node, 'user=testuser sslmode=require sslnegotiation=direct gssencmode=prefer', 'gss');
	connect_test($node, 'user=testuser sslmode=require sslnegotiation=requiredirect gssencmode=prefer', 'gss');

	# If you set both sslmode and gssencmode to 'require', 'gssencmode=require' takes
	# precedence.
	connect_test($node, 'user=testuser sslmode=require gssencmode=require', 'gss');

	# Test case that server supports GSSAPI, but it's not allowed for
	# this user.
	connect_test($node, 'user=nogssuser sslmode=prefer gssencmode=require', 'fail',
	  'no pg_hba.conf entry for host "127.0.0.1", user "nogssuser", database "postgres", GSS encryption');

	# With 'gssencmode=prefer', libpq will first negotiate GSSAPI
	# encryption, but the connection will fail because pg_hba.conf
	# forbids GSSAPI encryption for this user. It will then reconnect
	# with SSL, but the server doesn't support it, so it will continue
	# with no encryption.
	connect_test($node, 'user=nogssuser sslmode=prefer gssencmode=prefer', 'plain',
				 'no pg_hba.conf entry for host "127.0.0.1", user "nogssuser", database "postgres", GSS encryption');
	connect_test($node, 'user=nogssuser sslmode=prefer gssencmode=prefer sslnegotiation=direct', 'plain',
				 'no pg_hba.conf entry for host "127.0.0.1", user "nogssuser", database "postgres", GSS encryption');
	connect_test($node, 'user=nogssuser sslmode=prefer gssencmode=prefer sslnegotiation=requiredirect', 'plain',
				 'no pg_hba.conf entry for host "127.0.0.1", user "nogssuser", database "postgres", GSS encryption');
}

# Server supports both SSL and GSSAPI
SKIP:
{
	skip "GSSAPI/Kerberos or SSL not supported by this build" unless ($ssl_supported && $gss_supported);

	# SSL is still disabled
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=prefer', 'gss');
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=prefer sslnegotiation=direct', 'gss');
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=prefer sslnegotiation=requiredirect', 'gss');

	# Enable SSL
	$node->adjust_conf('postgresql.conf', 'ssl', 'on');
	$node->reload;

	connect_test($node, 'user=testuser sslmode=disable gssencmode=disable', 'plain');
	connect_test($node, 'user=testuser sslmode=disable gssencmode=prefer', 'gss');
	connect_test($node, 'user=testuser sslmode=disable gssencmode=require', 'gss');

	connect_test($node, 'user=testuser sslmode=prefer gssencmode=disable', 'ssl');
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=prefer', 'gss');
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=require', 'gss');

	connect_test($node, 'user=testuser sslmode=require gssencmode=disable', 'ssl');
	connect_test($node, 'user=testuser sslmode=require gssencmode=prefer', 'gss');

	connect_test($node, 'user=testuser sslmode=prefer gssencmode=prefer sslnegotiation=direct', 'gss');
	connect_test($node, 'user=testuser sslmode=prefer gssencmode=prefer sslnegotiation=requiredirect', 'gss');

	# If you set both sslmode and gssencmode to 'require', 'gssencmode=require' takes
	# precedence.
	connect_test($node, 'user=testuser sslmode=require gssencmode=require', 'gss');

	# Test case that server supports GSSAPI, but it's not allowed for
	# this user.
	connect_test($node, 'user=nogssuser sslmode=prefer gssencmode=require', 'fail',
	  'no pg_hba.conf entry for host "127.0.0.1", user "nogssuser", database "postgres", GSS encryption');

	# with 'gssencmode=prefer', libpq will first negotiate GSSAPI
	# encryption, but the connection will fail because pg_hba.conf
	# forbids GSSAPI encryption for this user. It will then reconnect
	# with SSL.
	connect_test($node, 'user=nogssuser sslmode=prefer gssencmode=prefer', 'ssl',
	  'no pg_hba.conf entry for host "127.0.0.1", user "nogssuser", database "postgres", GSS encryption');

	# Setting both sslmode=require and gssencmode=require fails if GSSAPI is not
	# available.
	connect_test($node, 'user=nogssuser sslmode=require gssencmode=require', 'fail');
}

# Test negotiation over unix domain sockets.
SKIP:
{
	skip "Unix domain sockets not supported" unless ($unixdir ne "");

	connect_test($node, "user=localuser sslmode=require gssencmode=prefer host=$unixdir", 'plain');
	connect_test($node, "user=localuser sslmode=prefer gssencmode=require host=$unixdir", 'fail');
}

done_testing();
