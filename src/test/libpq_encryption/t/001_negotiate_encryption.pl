
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

if ($gss_supported != 0)
{
	note "setting up Kerberos";

	my $realm = 'EXAMPLE.COM';
	$krb = PostgreSQL::Test::Kerberos->new($host, $hostaddr, $realm);
	$node->append_conf('postgresql.conf', "krb_server_keyfile = '$krb->{keytab}'\n");
}

if ($ssl_supported != 0)
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
} if ($ssl_supported != 0);

print $hba qq{
hostgssenc    postgres        gssuser         $servercidr             trust
} if ($gss_supported != 0);
close $hba;
$node->reload;

sub connect_test
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $connstr, $expected_enc, @expect_log_msgs)
	  = @_;

	my $test_name = " '$connstr' -> $expected_enc";

	my $connstr_full = "";
	$connstr_full .= "dbname=postgres " unless $connstr =~ m/dbname=/;
	$connstr_full .= "host=$host hostaddr=$hostaddr " unless $connstr =~ m/host=/;
	$connstr_full .= $connstr;

	my $log_location = -s $node->logfile;

	my ($ret, $stdout, $stderr) = $node->psql(
		'postgres',
		'SELECT current_enc()',
		extra_params => ['-w'],
		connstr => "$connstr_full",
		on_error_stop => 0);

	my $result = $ret == 0 ? $stdout : 'fail';

	is($result, $expected_enc, $test_name);

	if (@expect_log_msgs)
	{
		# Match every message literally.
		my @regexes = map { qr/\Q$_\E/ } @expect_log_msgs;
		my %params = ();
		$params{log_like} = \@regexes;
		$node->log_check($test_name, $log_location, %params);
	}
}

# Return the encryption mode that we expect to be chosen by libpq,
# when connecting with given the user, gssmode, sslmode settings.
sub resolve_connection_type
{
	my ($config) = @_;
	my $user = $config->{user};
	my $gssmode = $config->{gssmode};
	my $sslmode = $config->{sslmode};

	my @conntypes = qw(plain);

	# Add connection types supported by the server to the pool
	push(@conntypes, "ssl") if $config->{server_ssl} == 1;
	push(@conntypes, "gss") if $config->{server_gss} == 1;

	# User configurations:
	# gssuser/ssluser require the relevant connection type,
	@conntypes = grep {/gss/} @conntypes if $user eq 'gssuser';
	@conntypes = grep {/ssl/} @conntypes if $user eq 'ssluser';

	# nogssuser/nossluser require anything but the relevant connection type.
	@conntypes = grep {!/gss/} @conntypes if $user eq 'nogssuser';
	@conntypes = grep {!/ssl/} @conntypes if $user eq 'nossluser';

	print STDOUT "After user filter: @conntypes\n";

	# remove disabled connection modes
	@conntypes = grep {!/gss/} @conntypes if $gssmode eq 'disable';
	@conntypes = grep {!/ssl/} @conntypes if $sslmode eq 'disable';

	# If gssmode=require, drop all non-GSS modes.
	if ($gssmode eq 'require')
	{
		@conntypes = grep {/gss/} @conntypes;
	}

	# If sslmode=require, drop plain mode.
	#
	# NOTE: GSS is also allowed with sslmode=require.
	if ($sslmode eq 'require')
	{
		@conntypes = grep {!/plain/} @conntypes;
	}

	print STDOUT "After mode require filter: @conntypes\n";

	# Handle priorities of the various types.
	# Note that this doesn't need to care about require/disable/etc, those
	# filters were applied before we get here.
	# Also note that preference is 1 > 2 > 3 > 4 > 5, so first preference
	# without ssl or gss 'prefer/require' is plain connections.
	my %order = (plain=>3, gss=>4, ssl=>5);

	$order{ssl} = 2 if $sslmode eq "prefer";
	$order{gss} = 1 if $gssmode eq "prefer";
	@conntypes = sort { $order{$a} cmp $order{$b} } @conntypes;

	# If there are no connection types available after filtering requirements,
	# the connection fails.
	return "fail" if @conntypes == 0;
	# Else, we get to connect using the connection type with the highest
	# priority.
	return $conntypes[0];
}

# First test with SSL disabled in the server

# Test the cube of parameters: user, sslmode, sslnegotiation and gssencmode
sub test_modes
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($pg_node, $node_conf,
		$test_users, $ssl_modes, $ssl_negotiations, $gss_modes) = @_;

	foreach my $test_user (@{$test_users})
	{
		foreach my $client_mode (@{$ssl_modes})
		{
			foreach my $gssencmode (@{$gss_modes})
			{
				my %params = (
					server_ssl=>$node_conf->{server_ssl},
					server_gss=>$node_conf->{server_gss},
					user=>$test_user,
					sslmode=>$client_mode,
					gssmode=>$gssencmode,
				);
				my $res = resolve_connection_type(\%params);
				# Negotiation type doesn't matter for supported connection types
				foreach my $negotiation (@{$ssl_negotiations})
				{
					connect_test($pg_node, "user=$test_user sslmode=$client_mode sslnegotiation=$negotiation gssencmode=$gssencmode", $res);
				}
			}
		}
	}
}

my $sslmodes = ['disable', 'allow', 'prefer', 'require'];
my $sslnegotiations = ['postgres', 'direct', 'requiredirect'];
my $gssencmodes = ['disable', 'prefer', 'require'];

my $server_config = {
	server_ssl => 0,
	server_gss => 0,
};

note("Running tests with SSL and GSS disabled in server");
test_modes($node, $server_config,
		   ['testuser'],
		   $sslmodes, $sslnegotiations, $gssencmodes);

# Enable SSL in the server
SKIP:
{
	skip "SSL not supported by this build" if $ssl_supported == 0;

	$node->adjust_conf('postgresql.conf', 'ssl', 'on');
	$node->restart;
	$server_config->{server_ssl} = 1;

	note("Running tests with SSL enabled in server");
	test_modes($node, $server_config,
			   ['testuser', 'ssluser', 'nossluser'],
			   $sslmodes, $sslnegotiations, ['disable']);

	$node->adjust_conf('postgresql.conf', 'ssl', 'off');
	$node->reload;
	$server_config->{server_ssl} = 0;
}

# Test GSSAPI
SKIP:
{
	skip "GSSAPI/Kerberos not supported by this build" if $gss_supported == 0;

	# No ticket
	connect_test($node, 'user=testuser sslmode=disable gssencmode=require', 'fail');

	$krb->create_principal('gssuser', $gssuser_password);
	$krb->create_ticket('gssuser', $gssuser_password);
	$server_config->{server_gss} = 1;

	note("Running tests with GSS enabled in server");
	test_modes($node, $server_config,
			   ['testuser', 'gssuser', 'nogssuser'],
			   $sslmodes, $sslnegotiations, $gssencmodes);

	# Check that logs match the expected 'no pg_hba.conf entry' line, too, as
	# that is not tested by test_modes.
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
	$server_config->{server_ssl} = 1;

	note("Running tests with both GSS and SSL enabled in server");
	test_modes($node, $server_config,
			   ['testuser', 'gssuser', 'ssluser', 'nogssuser', 'nossluser'],
			   $sslmodes, $sslnegotiations, $gssencmodes);

	# Test case that server supports GSSAPI, but it's not allowed for
	# this user. Special cased because we check output
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
