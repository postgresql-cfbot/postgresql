package SSL::Backend::NSS;

use strict;
use warnings;
use Exporter;

our @ISA       = qw(Exporter);
our @EXPORT_OK = qw(get_new_nss_backend);

sub new
{
	my ($class) = @_;

	my $self = { _library => 'NSS' };

	bless $self, $class;

	return $self;
}

sub get_new_nss_backend
{
	my $class = 'SSL::Backend::NSS';

	return $class->new();
}

sub init
{
	# Make sure the certificate databases are in place?
}

sub get_library
{
	my ($self) = @_;

	return $self->{_library};
}

sub set_server_cert
{
	my $self     = $_[0];
	my $certfile = $_[1];
	my $cafile   = $_[2];
	my $keyfile  = $_[3];

	my $cert_nickname;

	if ($certfile =~ /native/)
	{
		$cert_nickname = $certfile;
	}
	else
	{
		$cert_nickname = $certfile . '.crt__' . $keyfile . '.key';
	}
	my $cert_database = $cert_nickname . '.db';

	my $sslconf =
	    "ssl_ca_file='$cafile.crt'\n"
	  . "ssl_cert_file='ssl/$certfile.crt'\n"
	  . "ssl_crl_file=''\n"
	  . "ssl_database='nss/$cert_database'\n";

	return $sslconf;
}

sub cleanup
{
	# Something?
}

1;
