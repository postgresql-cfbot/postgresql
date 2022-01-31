package SSL::Backend::OpenSSL;

use strict;
use warnings;
use Exporter;
use File::Copy;

our @ISA       = qw(Exporter);
our @EXPORT_OK = qw(get_new_openssl_backend get_openssl_key);

our (%key);

INIT
{
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
	# This changes to using keys stored in a temporary path for the rest of
	# the tests. To get the full path for inclusion in connection strings, the
	# %key hash can be interrogated.
	my $cert_tempdir = PostgreSQL::Test::Utils::tempdir();
	my @keys = (
		"client.key",               "client-revoked.key",
		"client-der.key",           "client-encrypted-pem.key",
		"client-encrypted-der.key", "client-dn.key");
	foreach my $keyfile (@keys)
	{
		copy("ssl/$keyfile", "$cert_tempdir/$keyfile")
		  or die
		  "couldn't copy ssl/$keyfile to $cert_tempdir/$keyfile for permissions change: $!";
		chmod 0600, "$cert_tempdir/$keyfile"
		  or die "failed to change permissions on $cert_tempdir/$keyfile: $!";
		$key{$keyfile} = PostgreSQL::Test::Utils::perl2host("$cert_tempdir/$keyfile");
		$key{$keyfile} =~ s!\\!/!g if $PostgreSQL::Test::Utils::windows_os;
	}

	# Also make a copy of that explicitly world-readable.  We can't
	# necessarily rely on the file in the source tree having those
	# permissions.
	copy("ssl/client.key", "$cert_tempdir/client_wrongperms.key")
	  or die
	  "couldn't copy ssl/client_key to $cert_tempdir/client_wrongperms.key for permission change: $!";
	chmod 0644, "$cert_tempdir/client_wrongperms.key"
	  or die "failed to change permissions on $cert_tempdir/client_wrongperms.key: $!";
	$key{'client_wrongperms.key'} = PostgreSQL::Test::Utils::perl2host("$cert_tempdir/client_wrongperms.key");
	$key{'client_wrongperms.key'} =~ s!\\!/!g if $PostgreSQL::Test::Utils::windows_os;
}

sub get_openssl_key
{
	my $keyfile = shift;

	return " sslkey=$key{$keyfile}";
}

# Change the configuration to use given server cert file, and reload
# the server so that the configuration takes effect.
sub set_server_cert
{
	my $self   = $_[0];
	my $params = $_[1];

	$params->{cafile} = 'root+client_ca' unless defined $params->{cafile};
	$params->{crlfile} = 'root+client.crl' unless defined $params->{crlfile};
	$params->{keyfile} = $params->{certfile} unless defined $params->{keyfile};

	my $sslconf =
	    "ssl_ca_file='$params->{cafile}.crt'\n"
	  . "ssl_cert_file='$params->{certfile}.crt'\n"
	  . "ssl_key_file='$params->{keyfile}.key'\n"
	  . "ssl_crl_file='$params->{crlfile}'\n";
	$sslconf .= "ssl_crl_dir='$params->{crldir}'\n" if defined $params->{crldir};

	return $sslconf;
}

sub get_library
{
	my ($self) = @_;

	return $self->{_library};
}

sub cleanup
{
}

1;
