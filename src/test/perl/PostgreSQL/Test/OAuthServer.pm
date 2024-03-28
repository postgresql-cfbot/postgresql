#!/usr/bin/perl

package PostgreSQL::Test::OAuthServer;

use warnings;
use strict;
use threads;
use Scalar::Util;
use Socket;
use IO::Select;

local *server_socket;

sub new
{
	my $class = shift;

	my $self = {};
	bless($self, $class);

	return $self;
}

sub port
{
	my $self = shift;

	return $self->{'port'};
}

sub run
{
	my $self = shift;
	my $port;

	my $pid = open(my $read_fh, "-|", $ENV{PYTHON}, "t/oauth_server.py")
		// die "failed to start OAuth server: $!";

	read($read_fh, $port, 7) // die "failed to read port number: $!";
	chomp $port;
	die "server did not advertise a valid port"
		unless Scalar::Util::looks_like_number($port);

	$self->{'pid'} = $pid;
	$self->{'port'} = $port;
	$self->{'child'} = $read_fh;

	print("# OAuth provider (PID $pid) is listening on port $port\n");
}

sub stop
{
	my $self = shift;

	print("# Sending SIGTERM to OAuth provider PID: $self->{'pid'}\n");

	kill(15, $self->{'pid'});
	$self->{'pid'} = undef;

	# Closing the popen() handle waits for the process to exit.
	close($self->{'child'});
	$self->{'child'} = undef;
}

1;
