package SSL::Backend::OpenSSL;

use strict;
use warnings;
use Exporter;
use File::Copy;

our @ISA       = qw(Exporter);
our @EXPORT_OK = qw(get_new_openssl_backend);

our (@keys);

INIT
{
	@keys = (
		"client",     "client-revoked",
		"client-der", "client-encrypted-pem",
		"client-encrypted-der");
}

sub new
{
	my ($class) = @_;

	my $self = { _library => 'OpenSSL' };

	bless $self, $class;

	return $self;
}

sub get_new_openssl_backend
{
	my $class = 'SSL::Backend::OpenSSL';

	my $backend = $class->new();

	return $backend;
}

sub init
{
	# The client's private key must not be world-readable, so take a copy
	# of the key stored in the code tree and update its permissions.
	#
	# This changes ssl/client.key to ssl/client_tmp.key etc for the rest
	# of the tests.
	foreach my $key (@keys)
	{
		copy("ssl/${key}.key", "ssl/${key}_tmp.key")
		  or die
		  "couldn't copy ssl/${key}.key to ssl/${key}_tmp.key for permissions change: $!";
		chmod 0600, "ssl/${key}_tmp.key"
		  or die "failed to change permissions on ssl/${key}_tmp.key: $!";
	}

	# Also make a copy of that explicitly world-readable.  We can't
	# necessarily rely on the file in the source tree having those
	# permissions. Add it to @keys to include it in the final clean
	# up phase.
	copy("ssl/client.key", "ssl/client_wrongperms_tmp.key")
	  or die
	  "couldn't copy ssl/client.key to ssl/client_wrongperms_tmp.key: $!";
	chmod 0644, "ssl/client_wrongperms_tmp.key"
	  or die
	  "failed to change permissions on ssl/client_wrongperms_tmp.key: $!";
	push @keys, 'client_wrongperms';
}

# Change the configuration to use given server cert file, and reload
# the server so that the configuration takes effect.
sub set_server_cert
{
	my $self     = $_[0];
	my $certfile = $_[1];
	my $cafile   = $_[2] || "root+client_ca";
	my $keyfile  = $_[3] || $certfile;

	my $sslconf =
	    "ssl_ca_file='$cafile.crt'\n"
	  . "ssl_cert_file='$certfile.crt'\n"
	  . "ssl_key_file='$keyfile.key'\n"
	  . "ssl_crl_file='root+client.crl'\n";

	return $sslconf;
}

sub get_library
{
	my ($self) = @_;

	return $self->{_library};
}

sub cleanup
{
	foreach my $key (@keys)
	{
		unlink("ssl/${key}_tmp.key");
	}
}

1;
