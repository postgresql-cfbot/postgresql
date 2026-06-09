# Copyright (c) 2025, PostgreSQL Global Development Group

=pod

=head1 NAME

PgHttpService::Server - runs a mock pg_service HTTP server for testing

=head1 SYNOPSIS

  use PgHttpService::Server;

  my $server = PgHttpService::Server->new();
  $server->run;

  my $port = $server->port;
  my $issuer = "http://127.0.0.1:$port";

  # test against $issuer...

  $server->stop;

=head1 DESCRIPTION

This is glue API between the Perl tests and the Python pg_service server
daemon implemented in t/http_service_server.py. (Python has a fairly usable HTTP server
in its standard library, so the implementation was ported from Perl.)

This pg_service server does not use TLS (it implements a nonstandard, unsafe
issuer at "http://127.0.0.1:<port>"), so libpq in particular will need to set
PGHTTPSERVICEBUG=UNSAFE to be able to talk to it.

=cut

package PgHttpService::Server;

use warnings;
use strict;
use Scalar::Util;
use Test::More;

=pod

=head1 METHODS

=over

=item PgHttpService::Server::Server->new()

Create a new HTTP PG Service Server object.

=cut

sub new
{
	my $class = shift;

	my $self = {};
	bless($self, $class);

	return $self;
}

=pod

=item $server->port()

Returns the port in use by the server.

=cut

sub port
{
	my $self = shift;

	return $self->{'port'};
}

=pod

=item $server->run()

Runs the http pg service server daemon in t/http_service_server.py.

=cut

sub run
{
	my $self = shift;
	my $service_file_path = shift;
	my $port;

	print $ENV{PYTHON};
	my $pid = open(my $read_fh, "-|", "python", "t/http_service_server.py", $service_file_path)
	  or die "failed to start http pg_service server: $!";

	# Get the port number from the daemon. It closes stdout afterwards; that way
	# we can slurp in the entire contents here rather than worrying about the
	# number of bytes to read.
	$port = do { local $/ = undef; <$read_fh> }
	  // die "failed to read port number: $!";
	chomp $port;
	die "server did not advertise a valid port"
	  unless Scalar::Util::looks_like_number($port);

	$self->{'pid'} = $pid;
	$self->{'port'} = $port;
	$self->{'child'} = $read_fh;

	note("HTTP pg_service (PID $pid) is listening on port $port\n");
}

=pod

=item $server->stop()

Sends SIGTERM to the http pg_service server and waits for it to exit.

=cut

sub stop
{
	my $self = shift;

	note("Sending SIGTERM to http pg_service PID: $self->{'pid'}\n");

	kill(15, $self->{'pid'});
	$self->{'pid'} = undef;

	# Closing the popen() handle waits for the process to exit.
	close($self->{'child'});
	$self->{'child'} = undef;
}

=pod

=back

=cut

1;
