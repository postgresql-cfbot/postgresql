
use strict;
use warnings;

use File::Copy;

use TestLib;
use Test::More;
use PostgresNode;

unless ( ($ENV{with_openssl} || 'no') eq 'yes')
{
	plan skip_all => 'SSL not supported by this build';
}

my $clearpass = "FooBaR1";
my $rot13pass = "SbbOnE1";

# self-signed cert was generated like this:
# system('openssl req -new -x509 -days 3650 -nodes -text -out server.crt -keyout server.ckey -subj "/CN=localhost"');
# add the cleartext passphrase to the key, remove the unprotected key
# system("openssl rsa -aes256 -in server.ckey -out server.key -passout pass:$clearpass");
# unlink "server.ckey";


my $node = get_new_node('main');
$node->init;
$node->append_conf('postgresql.conf',
				   "ssl_passphrase.passphrase = '$rot13pass'");
$node->append_conf('postgresql.conf',
				   "shared_preload_libraries = 'ssl_passphrase_func'");
$node->append_conf('postgresql.conf',
				   "listen_addresses = 'localhost'");
$node->append_conf('postgresql.conf',
				   "ssl = 'on'");

my $ddir = $node->data_dir;

# install certificate and protected key
move("server.crt", $ddir);
move("server.key", $ddir);
chmod 0600, "$ddir/server.key";

$node->start;

# if the server is running we must had successfully transformed the passphrase
ok(-e "$ddir/postmaster.pid","postgres started");

$node->stop('fast');

# set the wrong passphrase
$node->append_conf('postgresql.conf',
				   "ssl_passphrase.passphrase = 'blurfl'");

# try to start the server again
my $ret = TestLib::system_log('pg_ctl', '-D', $node->data_dir, '-l',
							  $node->logfile, 'start');


# with a bad passphrase the server should not start
ok($ret, "pg_ctl fails with bad passphrase");
ok(! -e "$ddir/postmaster.pid","postgres not started with bad passphrase");

# just in case
$node->stop('fast');

done_testing();
