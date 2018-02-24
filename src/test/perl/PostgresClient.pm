
=pod

=head1 NAME

PostgresClient - class representing PostgreSQL client interface

=head1 SYNOPSIS

  use PostgresClient;

  my $conn = PostgresClient::connectdb(<name>, <dbname>, <PostgresNode>);

  Or

  my $conn = PostgresClient::connectdb(<name>, <dbname>, {param1 => val1, ..});

  OR

  my $conn = PostgresClient::connectdb(<name>, <connection strting>);

  PostgresNode also provides get_new_session() to create a new session.

  # execute a query
  $result = $conn->exec('query');

  # executes a multiple query at once
  $success = $conn->exec_multi('query 1', 'query 2', ...);

  # close the connection
  $conn->finish();

  # get information.
  # see the corresponding functions of libpq. Several functions that
  # corresponding libpq function returns a enum value returns a string
  # representation

  $conn->name();
  $conn->db();
  $conn->user();
  $conn->pass();
  $conn->host();
  $conn->port();
  $conn->notice();
  $conn->clear_notice();
  $conn->status();
  $conn->transactionStatus();
  $conn->errorMessage();

=head1 DESCRIPTION

PostgresClient contains a set of routines able to work as a PostgreSQL
client, allowing to connect, disconnect and send a query and receive
the result.

=cut

package PostgresClient;

use 5.016003;
use strict;
use warnings;
use Carp;
use PgResult;

require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use PostgresClient ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	CONNECTION_AUTH_OK
	CONNECTION_AWAITING_RESPONSE
	CONNECTION_BAD
	CONNECTION_CHECK_WRITABLE
	CONNECTION_CONSUME
	CONNECTION_MADE
	CONNECTION_NEEDED
	CONNECTION_OK
	CONNECTION_SETENV
	CONNECTION_SSL_STARTUP
	CONNECTION_STARTED
	PGRES_BAD_RESPONSE
	PGRES_COMMAND_OK
	PGRES_COPY_BOTH
	PGRES_COPY_IN
	PGRES_COPY_OUT
	PGRES_EMPTY_QUERY
	PGRES_FATAL_ERROR
	PGRES_NONFATAL_ERROR
	PGRES_POLLING_ACTIVE
	PGRES_POLLING_FAILED
	PGRES_POLLING_OK
	PGRES_POLLING_READING
	PGRES_POLLING_WRITING
	PGRES_SINGLE_TUPLE
	PGRES_TUPLES_OK
	PG_COPYRES_ATTRS
	PG_COPYRES_EVENTS
	PG_COPYRES_NOTICEHOOKS
	PG_COPYRES_TUPLES
	PQERRORS_DEFAULT
	PQERRORS_TERSE
	PQERRORS_VERBOSE
	PQPING_NO_ATTEMPT
	PQPING_NO_RESPONSE
	PQPING_OK
	PQPING_REJECT
	PQSHOW_CONTEXT_ALWAYS
	PQSHOW_CONTEXT_ERRORS
	PQSHOW_CONTEXT_NEVER
	PQTRANS_ACTIVE
	PQTRANS_IDLE
	PQTRANS_INERROR
	PQTRANS_INTRANS
	PQTRANS_UNKNOWN
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	connectdb connectdbParams
	CONNECTION_AUTH_OK
	CONNECTION_AWAITING_RESPONSE
	CONNECTION_BAD
	CONNECTION_CHECK_WRITABLE
	CONNECTION_CONSUME
	CONNECTION_MADE
	CONNECTION_NEEDED
	CONNECTION_OK
	CONNECTION_SETENV
	CONNECTION_SSL_STARTUP
	CONNECTION_STARTED
	PGRES_BAD_RESPONSE
	PGRES_COMMAND_OK
	PGRES_COPY_BOTH
	PGRES_COPY_IN
	PGRES_COPY_OUT
	PGRES_EMPTY_QUERY
	PGRES_FATAL_ERROR
	PGRES_NONFATAL_ERROR
	PGRES_POLLING_ACTIVE
	PGRES_POLLING_FAILED
	PGRES_POLLING_OK
	PGRES_POLLING_READING
	PGRES_POLLING_WRITING
	PGRES_SINGLE_TUPLE
	PGRES_TUPLES_OK
	PG_COPYRES_ATTRS
	PG_COPYRES_EVENTS
	PG_COPYRES_NOTICEHOOKS
	PG_COPYRES_TUPLES
	PQERRORS_DEFAULT
	PQERRORS_TERSE
	PQERRORS_VERBOSE
	PQPING_NO_ATTEMPT
	PQPING_NO_RESPONSE
	PQPING_OK
	PQPING_REJECT
	PQSHOW_CONTEXT_ALWAYS
	PQSHOW_CONTEXT_ERRORS
	PQSHOW_CONTEXT_NEVER
	PQTRANS_ACTIVE
	PQTRANS_IDLE
	PQTRANS_INERROR
	PQTRANS_INTRANS
	PQTRANS_UNKNOWN
);

our $VERSION = '0.01';

sub AUTOLOAD {
    # This AUTOLOAD is used to 'autoload' constants from the constant()
    # XS function.

    my $constname;
    our $AUTOLOAD;
    ($constname = $AUTOLOAD) =~ s/.*:://;
    croak("&PostgresClient::constant not defined") if $constname eq 'constant';
    my ($error, $val) = constant($constname);
    if ($error) { croak $error; }
    {
	no strict 'refs';
	# Fixed between 5.005_53 and 5.005_61
#XXX	if ($] >= 5.00561) {
#XXX	    *$AUTOLOAD = sub () { $val };
#XXX	}
#XXX	else {
	    *$AUTOLOAD = sub { $val };
#XXX	}
    }
    goto &$AUTOLOAD;
}

require XSLoader;
XSLoader::load('PostgresClient', $VERSION);

sub exec_multi
{
	my ($self, @commands) = @_;

	foreach my $command (@commands)
	{
		my $result = $self->exec($command);

		return 1 if (!defined $result ||
					($result->resultStatus() != &PGRES_COMMAND_OK &&
					 $result->resultStatus() != &PGRES_TUPLES_OK));
	}

	return 0;
}


1;
