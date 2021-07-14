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
	my $self   = $_[0];
	my $params = $_[1];

	$params->{cafile} = 'root+client_ca' unless defined $params->{cafile};

	my $sslconf =
	    "ssl_ca_file='$params->{cafile}.crt'\n"
	  . "ssl_cert_file='ssl/$params->{certfile}.crt'\n"
	  . "ssl_crl_file=''\n"
	  . "ssl_database='nss/$params->{nssdatabase}'\n";

	return $sslconf;
}

sub cleanup
{
	# Something?
}

1;
