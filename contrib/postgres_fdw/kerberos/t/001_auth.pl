
# Copyright (c) 2021, PostgreSQL Global Development Group

# Sets up a KDC and then runs a variety of tests to make sure that the
# GSSAPI/Kerberos authentication and encryption are working properly,
# that the options in pg_hba.conf and pg_ident.conf are handled correctly,
# and that the server-side pg_stat_gssapi view reports what we expect to
# see for each test.
#
# Since this requires setting up a full KDC, it doesn't make much sense
# to have multiple test scripts (since they'd have to also create their
# own KDC and that could cause race conditions or other problems)- so
# just add whatever other tests are needed to here.
#
# See the README for additional information.

use strict;
use warnings;
use TestLib;
use PostgresNode;
use Test::More;
use Time::HiRes qw(usleep);
use Cwd 'abs_path';

note "start";
if ($ENV{with_gssapi} eq 'yes')
{
	plan tests => 23;
}
else
{
	plan skip_all => 'GSSAPI/Kerberos not supported by this build';
}

my ($krb5_bin_dir, $krb5_sbin_dir);

if ($^O eq 'darwin')
{
	$krb5_bin_dir  = '/usr/local/opt/krb5/bin';
	$krb5_sbin_dir = '/usr/local/opt/krb5/sbin';
}
elsif ($^O eq 'freebsd')
{
	$krb5_bin_dir  = '/usr/local/bin';
	$krb5_sbin_dir = '/usr/local/sbin';
}
elsif ($^O eq 'linux')
{
	$krb5_sbin_dir = '/usr/sbin';
}

my $krb5_config  = 'krb5-config';
my $kinit        = 'kinit';
my $kdb5_util    = 'kdb5_util';
my $kadmin_local = 'kadmin.local';
my $krb5kdc      = 'krb5kdc';

if ($krb5_bin_dir && -d $krb5_bin_dir)
{
	$krb5_config = $krb5_bin_dir . '/' . $krb5_config;
	$kinit       = $krb5_bin_dir . '/' . $kinit;
}
if ($krb5_sbin_dir && -d $krb5_sbin_dir)
{
	$kdb5_util    = $krb5_sbin_dir . '/' . $kdb5_util;
	$kadmin_local = $krb5_sbin_dir . '/' . $kadmin_local;
	$krb5kdc      = $krb5_sbin_dir . '/' . $krb5kdc;
}

my $host     = 'auth-test-localhost.postgresql.example.com';
my $hostaddr = '127.0.0.1';
my $realm    = 'EXAMPLE.COM';


my $tmp_check = abs_path(${TestLib::tmp_check});
my $krb5_conf   = "$tmp_check/krb5.conf";
my $kdc_conf    = "$tmp_check/kdc.conf";
#my $krb5_cache  = "$tmp_check/krb5cc";
my $krb5_cache  = "MEMORY:";
my $krb5_log    = "${TestLib::log_path}/krb5libs.log";
my $kdc_log     = "${TestLib::log_path}/krb5kdc.log";
my $kdc_port    = get_free_port();
my $kdc_datadir = "$tmp_check/krb5kdc";
my $kdc_pidfile = "$tmp_check/krb5kdc.pid";
my $keytab_client      = "$tmp_check/client.keytab";
my $keytab_server      = "$tmp_check/server.keytab";

my $dbname      = 'testdb';
my $testuser    = 'testuser';
my $test1   = 'test1';
my $application = '001_auth.pl';

note "setting up Kerberos";

my ($stdout, $krb5_version);
run_log [ $krb5_config, '--version' ], '>', \$stdout
  or BAIL_OUT("could not execute krb5-config");
BAIL_OUT("Heimdal is not supported") if $stdout =~ m/heimdal/;
$stdout =~ m/Kerberos 5 release ([0-9]+\.[0-9]+)/
  or BAIL_OUT("could not get Kerberos version");
$krb5_version = $1;

append_to_file(
	$krb5_conf,
	qq![logging]
default = FILE:$krb5_log
kdc = FILE:$kdc_log

[libdefaults]
default_realm = $realm
forwardable = false

[realms]
$realm = {
    kdc = $hostaddr:$kdc_port
}!);

append_to_file(
	$kdc_conf,
	qq![kdcdefaults]
!);

# For new-enough versions of krb5, use the _listen settings rather
# than the _ports settings so that we can bind to localhost only.
if ($krb5_version >= 1.15)
{
	append_to_file(
		$kdc_conf,
		qq!kdc_listen = $hostaddr:$kdc_port
kdc_tcp_listen = $hostaddr:$kdc_port
!);
}
else
{
	append_to_file(
		$kdc_conf,
		qq!kdc_ports = $kdc_port
kdc_tcp_ports = $kdc_port
!);
}

append_to_file(
	$kdc_conf,
	qq!
[realms]
$realm = {
    database_name = $kdc_datadir/principal
    admin_keytab = FILE:$kdc_datadir/kadm5.keytab
    acl_file = $kdc_datadir/kadm5.acl
    key_stash_file = $kdc_datadir/_k5.$realm
}!);

mkdir $kdc_datadir or die;

# Ensure that we use test's config and cache files, not global ones.
$ENV{'KRB5_CONFIG'}      = $krb5_conf;
$ENV{'KRB5_KDC_PROFILE'} = $kdc_conf;
$ENV{'KRB5CCNAME'}       = $krb5_cache;

my $service_principal = "$ENV{with_krb_srvnam}/$host";

system_or_bail $kdb5_util, 'create', '-s', '-P', 'secret0';

my $test1_password = 'secret1';
system_or_bail $kadmin_local, '-q', "addprinc -randkey $testuser";
system_or_bail $kadmin_local, '-q', "addprinc -randkey $service_principal";
system_or_bail $kadmin_local, '-q', "ktadd -k $keytab_client $testuser";
system_or_bail $kadmin_local, '-q', "ktadd -k $keytab_server $service_principal";

system_or_bail $krb5kdc, '-P', $kdc_pidfile;

END
{
	kill 'INT', `cat $kdc_pidfile` if -f $kdc_pidfile;
}

note "setting up PostgreSQL instance";

# server node for fdw to connect
my $server_node = get_new_node('node');
$server_node->init;
$server_node->append_conf(
	'postgresql.conf', qq{
listen_addresses = '$hostaddr'
krb_server_keyfile = '$keytab_server'
log_connections = on
lc_messages = 'C'
});
$server_node->start;

note "setting up user on server";
$server_node->safe_psql('postgres', "CREATE USER $testuser;");
# there's no direct way to get the backend pid from postgres_fdw, create a VIEW at server side to provide this.
$server_node->safe_psql('postgres', 'CREATE VIEW my_pg_stat_gssapi AS
    SELECT  S.pid,
            S.gss_auth AS gss_authenticated,
            S.gss_princ AS principal,
            S.gss_enc AS encrypted
    FROM pg_stat_get_activity(NULL) AS S
    WHERE S.client_port IS NOT NULL AND S.pid = pg_backend_pid();');
$server_node->safe_psql('postgres', "GRANT ALL ON my_pg_stat_gssapi TO $testuser;");

my $server_node_port = $server_node->port;
note "setting up PostgreSQL fdw instance";
# client node that runs postgres fdw
my $fdw_node = get_new_node('node_fdw');
$fdw_node->init;
$fdw_node->append_conf(
	'postgresql.conf', qq{
listen_addresses = '$hostaddr'
krb_server_keyfile = '$keytab_server'
log_connections = on
lc_messages = 'C'
});
$fdw_node->start;

$fdw_node->safe_psql('postgres', "CREATE USER $testuser;");
$fdw_node->safe_psql('postgres', "CREATE DATABASE $dbname;");
$fdw_node->safe_psql($dbname, "CREATE EXTENSION postgres_fdw");
$fdw_node->safe_psql($dbname, "GRANT ALL ON FOREIGN DATA WRAPPER postgres_fdw TO $testuser");

note "running tests";

sub set_user_mapping_option
{
	my ($option, $value, $action) = @_;
	if (!defined $action)
	{
		$action = "SET";
	}

	my $option_str = "$option";
	if (defined $value)
	{
		$option_str = "$option '$value'";
	}

	my $q = "ALTER USER MAPPING for $testuser SERVER postgres_server OPTIONS ($action $option_str);";
	$fdw_node->safe_psql($dbname, $q);
}

# Test connection success or failure, and if success, that query returns true.
sub test_access
{
	my ($query, $expected_res, $test_name,
		$expect_output, @expect_log_msgs)
	  = @_;
	test_access_gssmode($query, "prefer", $expected_res, $test_name,
		$expect_output, @expect_log_msgs)
}

sub test_access_gssmode_disable
{
	my ($query, $expected_res, $test_name,
		$expect_output, @expect_log_msgs)
	  = @_;
	test_access_gssmode($query, "disable", $expected_res, $test_name,
		$expect_output, @expect_log_msgs)
}

sub test_access_gssmode
{
	my ($query, $mode, $expected_res, $test_name,
		$expect_output, @expect_log_msgs)
	  = @_;

	my %params = (sql => $query,);

	# Check log in server node. Obtain log file size before we run any query.
	# This way we can obtain logs for this psql connection only.
	my $log_location = -s $server_node->logfile;

	# Run psql in fdw node.
	my ($ret, $stdout, $stderr) = $fdw_node->psql(
		$dbname,
		$query,
		extra_params => ['-w'],
		connstr      => "user=$testuser dbname=$dbname host=$host hostaddr=$hostaddr gssencmode=$mode");


	is($ret, $expected_res, $test_name);
	if ($ret != $expected_res)
	{
		note $ret;
		note $stdout;
		note $stderr;
	}

	if (defined $expect_output)
	{
		if ($expected_res eq 0)
		{
			like($stdout, $expect_output, "$test_name: result matches");
		}
		else
		{
			like($stderr, $expect_output, "$test_name: result matches");
		}
	}

	if (@expect_log_msgs)
	{
		# Match every message literally.
		my @regexes = map { qr/\Q$_\E/ } @expect_log_msgs;
		my $log_contents = TestLib::slurp_file($server_node->logfile, $log_location);

		while (my $regex = shift @regexes)
		{
			like($log_contents, $regex, "$test_name: matches");
		}
	}
}

# Setup pg_hba to allow superuser trust login, and testuser gss login.
# We need to remove the KRB5_CLIENT_KTNAME environemnt first.
delete $ENV{'KRB5_CLIENT_KTNAME'};
unlink($fdw_node->data_dir . '/pg_hba.conf');
$fdw_node->append_conf('pg_hba.conf',
	qq{local all $ENV{'USER'} trust});
$fdw_node->append_conf('pg_hba.conf',
	qq{host all $testuser $hostaddr/32 gss map=mymap});
$fdw_node->append_conf('pg_ident.conf', qq{mymap  /^(.*)\@$realm\$  \\1});
$fdw_node->restart;

test_access(
	'SELECT true FROM pg_stat_gssapi',
	2,
	'no access without credential',
	'/No Kerberos credentials available/'
);

$ENV{'KRB5_CLIENT_KTNAME'}       = $keytab_client;

test_access(
	'SELECT true FROM pg_stat_gssapi',
	0,
	'success',
	"/^t\$/"
);

# create foreign table using unprivileged user

test_access(
	"CREATE SERVER postgres_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '$host', hostaddr '$hostaddr', port '$server_node_port', dbname 'postgres')",
	0,
	'create server',
);
test_access(
	"CREATE FOREIGN TABLE my_pg_stat_gssapi\(pid int, gss_authenticated boolean, principal text, encrypted boolean\) SERVER postgres_server OPTIONS(table_name 'my_pg_stat_gssapi'); ",
	0,
	'create foreign table',
);
test_access(
	"CREATE USER MAPPING for $testuser SERVER postgres_server OPTIONS (user '$testuser');",
	0,
	'create user mapping',
);

test_access(
	"SELECT gss_authenticated AND encrypted from my_pg_stat_gssapi;",
	3,
	'select failed due to password required',
	"/password is required/"
);

set_user_mapping_option('password_required', 'false', 'ADD');

test_access(
	"SELECT gss_authenticated AND encrypted from my_pg_stat_gssapi;",
	0,
	'select fdw success, but with trust connection',
	"/f/"
);


unlink($server_node->data_dir . '/pg_hba.conf');
$server_node->append_conf('pg_hba.conf',
	qq{host all all $hostaddr/32 gss map=mymap});
$server_node->append_conf('pg_ident.conf', qq{mymap  /^(.*)\@$realm\$  \\1});
$server_node->restart;

test_access(
	'SELECT gss_authenticated FROM my_pg_stat_gssapi',
	3,
	'fails without ticket, due to not forwardable',
	'/No Kerberos credentials available/',
);

# Allow ticket forward in krb5.conf
string_replace_file($krb5_conf, "forwardable = false", "forwardable = true");
test_access(
	'SELECT gss_authenticated FROM my_pg_stat_gssapi',
	0,
	'success with ticket forward',
	"/^t\$/"
);

test_access_gssmode_disable(
	'SELECT gss_authenticated,encrypted FROM pg_stat_gssapi',
	0,
	'login fdw success, encryption disabled',
	"/^t|f\$/"
);

test_access_gssmode_disable(
	'SELECT gss_authenticated,encrypted FROM my_pg_stat_gssapi',
	0,
	'success, encryption enabled for fdw even disabled by login',
	"/^t|t\$/"
);

set_user_mapping_option('gssencmode', 'disable', 'ADD');

test_access(
	'SELECT gss_authenticated,encrypted FROM my_pg_stat_gssapi',
	0,
	'success, encryption disabled for fdw even enabled by login',
	"/^t|f\$/"
);

test_access_gssmode_disable(
	'SELECT gss_authenticated,encrypted FROM my_pg_stat_gssapi',
	0,
	'success, encryption disabled both by login and fdw',
	"/^t|f\$/"
);