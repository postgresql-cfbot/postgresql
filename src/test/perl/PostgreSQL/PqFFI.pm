
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

=pod

=head1 NAME

PostgreSQL::PqFFI - FFI wrapper for libpq

=head1 SYNOPSIS

  use PostgreSQL::PqFFI;

  # Initialize the FFI bindings (required before use)
  PostgreSQL::PqFFI::setup($libdir);

  # Connect to database
  my $conn = PQconnectdb("dbname=postgres");
  die PQerrorMessage($conn) unless PQstatus($conn) == CONNECTION_OK;

  # Execute query
  my $result = PQexec($conn, "SELECT 1");
  if (PQresultStatus($result) == PGRES_TUPLES_OK) {
      print PQgetvalue($result, 0, 0), "\n";
  }
  PQclear($result);

  PQfinish($conn);

=head1 DESCRIPTION

This module provides Perl bindings to libpq using L<FFI::Platypus>.
It is the backend used by L<PostgreSQL::Test::Session>.

The module must be initialized by calling C<setup()> before any libpq
functions can be used.

=head1 FUNCTIONS

=head2 setup($libdir [, $use_system_path])

Initialize the FFI bindings. C<$libdir> specifies where to find libpq.
If C<$use_system_path> is false, only C<$libdir> is searched.

=head2 libpq Functions

All standard libpq functions are exported. See the PostgreSQL libpq
documentation for details. Commonly used functions include:

B<Connection:> C<PQconnectdb>, C<PQconnectStart>, C<PQconnectPoll>,
C<PQfinish>, C<PQstatus>, C<PQerrorMessage>

B<Execution:> C<PQexec>, C<PQexecParams>, C<PQprepare>, C<PQexecPrepared>,
C<PQsendQuery>, C<PQgetResult>, C<PQclear>

B<Results:> C<PQresultStatus>, C<PQntuples>, C<PQnfields>, C<PQfname>,
C<PQftype>, C<PQgetvalue>, C<PQgetisnull>

B<Pipeline:> C<PQenterPipelineMode>, C<PQexitPipelineMode>,
C<PQpipelineSync>, C<PQsendQueryParams>

B<Notifications:> C<PQnotifies>, C<PQnotify_channel>, C<PQnotify_payload>,
C<PQnotify_be_pid>, C<PQnotify_free>

B<Notice Processing:> C<PQsetNoticeProcessor>, C<create_notice_processor>

=head2 create_notice_processor($callback)

Creates a notice processor closure that can be passed to
C<PQsetNoticeProcessor>. The callback receives C<($arg, $message)>.
The caller must keep a reference to the returned closure to prevent
garbage collection.

=head1 EXPORTED CONSTANTS

This module re-exports all constants from L<PostgreSQL::PqConstants>
and L<PostgreSQL::PGTypes>.

=cut

package PostgreSQL::PqFFI;

use strict;
use warnings FATAL => qw(all);

use FFI::Platypus;
use FFI::Platypus::Record;
use PostgreSQL::FindLib;

# PGnotify struct for notification support
# typedef struct pgNotify {
#     char *relname;    /* notification channel name */
#     int   be_pid;     /* process ID of notifying server process */
#     char *extra;      /* notification payload string */
# } PGnotify;
package PGnotify {
	use FFI::Platypus::Record;
	record_layout_1(
		'opaque' => 'relname',
		'sint32' => 'be_pid',
		'opaque' => 'extra',
	);
}
package PostgreSQL::PqFFI;

# FFI::Platypus::Record keeps the record's Record::Meta object alive in a
# package-glob closure, so it is not destroyed until global destruction.
# Record::Meta's DESTROY is an FFI-attached function taking a custom type
# whose marshalling is a Perl closure; during global destruction that
# closure may already have been freed, making DESTROY die with "Can't use
# an undefined value as a subroutine reference" (seen with FFI::Platypus
# 2.08).  The process is exiting anyway, so skip the destructor then and
# let the OS reclaim the memory.
{
	my $orig_destroy = \&FFI::Platypus::Record::Meta::DESTROY;
	no warnings 'redefine';
	*FFI::Platypus::Record::Meta::DESTROY = sub {
		return if ${^GLOBAL_PHASE} eq 'DESTRUCT';
		goto &$orig_destroy;
	};
}
use PostgreSQL::PqConstants;
use PostgreSQL::PGTypes;

use Exporter qw(import);

our @EXPORT = (
  @PostgreSQL::PqConstants::EXPORT,
  @PostgreSQL::PGTypes::EXPORT,
);



my @procs = qw(

  PQnotifies
  PQfreemem
  PQnotify_channel
  PQnotify_payload
  PQnotify_be_pid
  PQnotify_free

  PQconnectdb
  PQconnectdbParams
  PQsetdbLogin
  PQfinish
  PQreset
  PQdb
  PQuser
  PQpass
  PQhost
  PQhostaddr
  PQport
  PQtty
  PQoptions
  PQstatus
  PQtransactionStatus
  PQparameterStatus
  PQping
  PQpingParams

  PQexec
  PQexecParams
  PQprepare
  PQexecPrepared

  PQdescribePrepared
  PQdescribePortal

  PQclosePrepared
  PQclosePortal
  PQclear

  PQsendQuery
  PQgetResult
  PQisBusy
  PQconsumeInput

  PQprotocolVersion
  PQserverVersion
  PQerrorMessage
  PQsocket
  PQsocketPoll
  PQgetCurrentTimeUSec
  PQbackendPID
  PQconnectionNeedsPassword
  PQconnectionUsedPassword
  PQconnectionUsedGSSAPI
  PQclientEncoding
  PQsetClientEncoding

  PQresultStatus
  PQresStatus
  PQresultErrorMessage
  PQresultErrorField
  PQntuples
  PQnfields
  PQbinaryTuples
  PQfname
  PQfnumber
  PQftable
  PQftablecol
  PQfformat
  PQftype
  PQfsize
  PQfmod
  PQcmdStatus
  PQoidValue
  PQcmdTuples
  PQgetvalue
  PQgetlength
  PQgetisnull
  PQnparams
  PQparamtype
  PQchangePassword

  PQpipelineStatus
  PQenterPipelineMode
  PQexitPipelineMode
  PQpipelineSync
  PQsendFlushRequest
  PQsendPipelineSync
  PQsendQueryParams

  PQsetnonblocking
  PQisnonblocking
  PQflush

  PQconnectStart
  PQconnectStartParams
  PQconnectPoll

  PQsetNoticeProcessor
  create_notice_processor

);

push(@EXPORT, @procs);

sub setup
{
	my $libdir = shift;
	my $use_system_path = shift;

	my $ffi = FFI::Platypus->new(api => 1);

	my @system_path;
	@system_path = (systempath => []) unless $use_system_path;

	$ffi->type('opaque' => 'PGconn');
	$ffi->type('opaque' => 'PGresult');
	$ffi->type('uint32' => 'Oid');
	$ffi->type('int' => 'ExecStatusType');

	# Register the PGnotify record type for struct access
	$ffi->type('record(PGnotify)' => 'PGnotify_record');

	my $lib = find_lib_or_die(
		lib => 'pq',
		libpath => [$libdir],
	    @system_path,
	   );
	$ffi->lib($lib);

	# This wrapper depends on libpq functions introduced in PostgreSQL 17
	# (e.g. PQsocketPoll, used by every query path).  libpq is loaded once per
	# process from the first session's installation, so in cross-version tests
	# (pg_upgrade) the first session must use the newer installation.  Fail
	# clearly here rather than with a cryptic FFI "symbol not found" at the
	# first attach below, or an "Undefined subroutine" later.
	defined $ffi->find_symbol('PQsocketPoll')
	  or die "libpq in \"$lib\" is too old for PostgreSQL::Test::Session "
	  . "(needs PQsocketPoll, PostgreSQL 17+); in cross-version tests, open "
	  . "the first session against the newer installation";

	$ffi->attach('PQconnectdb' => ['string'] => 'PGconn');
	$ffi->attach(
		'PQconnectdbParams' => [ 'string[]', 'string[]', 'int' ] => 'PGconn');
	$ffi->attach(
		'PQsetdbLogin' => [
			'string', 'string', 'string', 'string',
			'string', 'string', 'string',
		] => 'PGconn');
	$ffi->attach('PQfinish' => ['PGconn'] => 'void');
	$ffi->attach('PQreset' => ['PGconn'] => 'void');
	$ffi->attach('PQdb' => ['PGconn'] => 'string');
	$ffi->attach('PQuser' => ['PGconn'] => 'string');
	$ffi->attach('PQpass' => ['PGconn'] => 'string');
	$ffi->attach('PQhost' => ['PGconn'] => 'string');
	$ffi->attach('PQhostaddr' => ['PGconn'] => 'string');
	$ffi->attach('PQport' => ['PGconn'] => 'string');
	$ffi->attach('PQtty' => ['PGconn'] => 'string');
	$ffi->attach('PQoptions' => ['PGconn'] => 'string');
	$ffi->attach('PQstatus' => ['PGconn'] => 'int');
	$ffi->attach('PQtransactionStatus' => ['PGconn'] => 'int');
	$ffi->attach('PQparameterStatus' => [ 'PGconn', 'string' ] => 'string');
	$ffi->attach('PQping' => ['string'] => 'int');
	$ffi->attach(
		'PQpingParams' => [ 'string[]', 'string[]', 'int' ] => 'int');

	$ffi->attach('PQprotocolVersion' => ['PGconn'] => 'int');
	$ffi->attach('PQserverVersion' => ['PGconn'] => 'int');
	$ffi->attach('PQerrorMessage' => ['PGconn'] => 'string');
	$ffi->attach('PQsocket' => ['PGconn'] => 'int');
	$ffi->attach('PQsocketPoll' => ['int', 'int', 'int', 'sint64'] => 'int');
	$ffi->attach('PQgetCurrentTimeUSec' => [] => 'sint64');
	$ffi->attach('PQbackendPID' => ['PGconn'] => 'int');
	$ffi->attach('PQconnectionNeedsPassword' => ['PGconn'] => 'int');
	$ffi->attach('PQconnectionUsedPassword' => ['PGconn'] => 'int');
	$ffi->attach('PQconnectionUsedGSSAPI' => ['PGconn'] => 'int');
	$ffi->attach('PQclientEncoding' => ['PGconn'] => 'int');
	$ffi->attach('PQsetClientEncoding' => [ 'PGconn', 'string' ] => 'int');

	$ffi->attach('PQexec' => [ 'PGconn', 'string' ] => 'PGresult');
	$ffi->attach(
		'PQexecParams' => [
			'PGconn', 'string', 'int', 'int[]',
			'string[]', 'int[]', 'int[]', 'int'
		] => 'PGresult');
	$ffi->attach(
		'PQprepare' => [ 'PGconn', 'string', 'string', 'int', 'int[]' ] =>
		  'PGresult');
	$ffi->attach(
		'PQexecPrepared' => [ 'PGconn', 'string', 'int',
			'string[]', 'int[]', 'int[]', 'int' ] => 'PGresult');

	$ffi->attach('PQresultStatus' => ['PGresult'] => 'ExecStatusType');
	$ffi->attach('PQresStatus' => ['ExecStatusType'] => 'string');
	$ffi->attach('PQresultErrorMessage' => ['PGresult'] => 'string');
	$ffi->attach('PQresultErrorField' => [ 'PGresult', 'int' ] => 'string');
	$ffi->attach('PQntuples' => ['PGresult'] => 'int');
	$ffi->attach('PQnfields' => ['PGresult'] => 'int');
	$ffi->attach('PQbinaryTuples' => ['PGresult'] => 'int');
	$ffi->attach('PQfname' => [ 'PGresult', 'int' ] => 'string');
	$ffi->attach('PQfnumber' => [ 'PGresult', 'string' ] => 'int');
	$ffi->attach('PQftable' => [ 'PGresult', 'int' ] => 'Oid');
	$ffi->attach('PQftablecol' => [ 'PGresult', 'int' ] => 'int');
	$ffi->attach('PQfformat' => [ 'PGresult', 'int' ] => 'int');
	$ffi->attach('PQftype' => [ 'PGresult', 'int' ] => 'Oid');
	$ffi->attach('PQfsize' => [ 'PGresult', 'int' ] => 'int');
	$ffi->attach('PQfmod' => [ 'PGresult', 'int' ] => 'int');
	$ffi->attach('PQcmdStatus' => ['PGresult'] => 'string');
	$ffi->attach('PQoidValue' => ['PGresult'] => 'Oid');
	$ffi->attach('PQcmdTuples' => ['PGresult'] => 'string');
	$ffi->attach('PQgetvalue' => [ 'PGresult', 'int', 'int' ] => 'string');
	$ffi->attach('PQgetlength' => [ 'PGresult', 'int', 'int' ] => 'int');
	$ffi->attach('PQgetisnull' => [ 'PGresult', 'int', 'int' ] => 'int');
	$ffi->attach('PQnparams' => ['PGresult'] => 'int');
	$ffi->attach('PQparamtype' => [ 'PGresult', 'int' ] => 'Oid');


	$ffi->attach(
		'PQdescribePrepared' => [ 'PGconn', 'string' ] => 'PGresult');
	$ffi->attach('PQdescribePortal' => [ 'PGconn', 'string' ] => 'PGresult');

	$ffi->attach('PQclosePrepared' => [ 'PGconn', 'string' ] => 'PGresult');
	$ffi->attach('PQclosePortal' => [ 'PGconn', 'string' ] => 'PGresult');
	$ffi->attach('PQclear' => ['PGresult'] => 'void');

	$ffi->attach('PQconnectStart' => [ 'string' ] => 'PGconn');
	$ffi->attach(
		'PQconnectStartParams' => [ 'string[]', 'string[]', 'int' ] => 'PGconn');
	$ffi->attach('PQconnectPoll' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQresetStart' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQresetPoll' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQsendQuery' => [ 'PGconn',  'string' ] => 'int');
	$ffi->attach('PQsendQueryParams' => [
		'PGconn', 'string', 'int', 'Oid*', 'string*',
		'int*', 'int*', 'int' ] => 'int');
	$ffi->attach('PQsendPrepare' => [ 'PGconn', 'string', 'string', 'int', 'Oid[]' ] => 'int');
	$ffi->attach('PQgetResult' => [ 'PGconn' ] => 'PGresult');

	$ffi->attach('PQisBusy' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQconsumeInput' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQchangePassword' => [ 'PGconn', 'string', 'string' ] => 'PGresult');

	$ffi->attach('PQpipelineStatus' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQenterPipelineMode' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQexitPipelineMode' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQpipelineSync' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQsendFlushRequest' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQsendPipelineSync' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQsetnonblocking' => [ 'PGconn', 'int' ] => 'int');
	$ffi->attach('PQisnonblocking' => [ 'PGconn' ] => 'int');
	$ffi->attach('PQflush' => [ 'PGconn' ] => 'int');

	# Notification support - PQnotifies returns a pointer to PGnotify struct
	# We return opaque to preserve the original pointer for freeing with PQfreemem
	$ffi->attach('PQnotifies' => [ 'PGconn' ] => 'opaque');
	$ffi->attach('PQfreemem' => [ 'opaque' ] => 'void');

	# Notice processor callback support
	# typedef void (*PQnoticeProcessor)(void *arg, const char *message);
	$ffi->type('(opaque,string)->void' => 'PQnoticeProcessor');
	$ffi->attach('PQsetNoticeProcessor' => [ 'PGconn', 'PQnoticeProcessor', 'opaque' ] => 'opaque');

	# Store the $ffi instance for use by helper functions
	$PostgreSQL::PqFFI::_ffi = $ffi;
}

# Helper functions to extract values from PGnotify struct
# The opaque pointer is cast to the record type to access fields

sub _cast_to_pgnotify
{
	my $ptr = shift;
	return undef unless $ptr;
	return $PostgreSQL::PqFFI::_ffi->cast('opaque', 'record(PGnotify)*', $ptr);
}

sub PQnotify_channel
{
	my $ptr = shift;
	return undef unless $ptr;
	my $notify = _cast_to_pgnotify($ptr);
	my $str_ptr = $notify->relname;
	return undef unless $str_ptr;
	return $PostgreSQL::PqFFI::_ffi->cast('opaque', 'string', $str_ptr);
}

sub PQnotify_payload
{
	my $ptr = shift;
	return undef unless $ptr;
	my $notify = _cast_to_pgnotify($ptr);
	my $str_ptr = $notify->extra;
	return undef unless $str_ptr;
	return $PostgreSQL::PqFFI::_ffi->cast('opaque', 'string', $str_ptr);
}

sub PQnotify_be_pid
{
	my $ptr = shift;
	return undef unless $ptr;
	my $notify = _cast_to_pgnotify($ptr);
	return $notify->be_pid;
}

# Free a PGnotify struct using the original pointer
sub PQnotify_free
{
	my $ptr = shift;
	return unless $ptr;
	PQfreemem($ptr);
}

# Create a notice processor closure that can be passed to PQsetNoticeProcessor.
# The callback will be invoked with (arg, message) where arg is opaque and message is string.
# Returns the closure - caller must keep a reference to prevent garbage collection.
sub create_notice_processor
{
	my $callback = shift;  # sub { my ($arg, $message) = @_; ... }
	return $PostgreSQL::PqFFI::_ffi->closure($callback);
}

=pod

=head1 SEE ALSO

L<PostgreSQL::Test::Session>,
L<PostgreSQL::PqConstants>, L<PostgreSQL::PGTypes>, L<FFI::Platypus>

=cut

1;
