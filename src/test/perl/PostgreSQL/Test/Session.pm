
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

=pod

=head1 NAME

PostgreSQL::Test::Session - class for a PostgreSQL libpq session

=head1 SYNOPSIS

  use PostgreSQL::Test::Session;

  use PostgreSQL::Test::Cluster;

  my $node = PostgreSQL::Test::Cluster->new('mynode');

  # create a new session. defult dbname is 'postgres'
  my $session = PostgreSQL::Test::Session->new(node => $node
                                               [, dbname => $dbname] );

  # close the session
  $session->close;

  # reopen the session, after closing it if not closed
  $session->reconnect;

  # check if the session is ok
  # my $status = $session->conn_status;

  # run some SQL, not producing tuples
  my $result = $session->do($sql, ...);

  # run an SQL statement asynchronously
  my $result = $session->do_async($sql);

  # wait for and async SQL to complete
  $session->wait_for_completion;

  # set a password for a user
  my $result = $session->set_password($user, $password);

  # get some data
  my $result = $session->query($sql);

  # get a single value, default croaks if no value found
  my $val = $session->query_oneval($sql [, $missing_ok ]);

  #return lines of tuples like "psql -A -t"
  my @lines = $session->query_tuples($sql, ...);

=head1 DESCRIPTION

C<PostgreSQL::Test::Session> encapsulates a C<libpq> session for use in
PostgreSQL TAP tests, allowing the test to connect without having to spawn
C<psql> in a child process.

The session object is automatically closed when the object goes out of scope,
including at script end.

Several methods return a hashref as a result, which will have the following
fields:

=over

=item * status

=item * error_message (only if there is an error)

=item * names

=item * types

=item * rows

=item * psqlout

=back

The last 4 will be empty unless the SQL produces tuples.

=cut

package PostgreSQL::Test::Session;

use strict;
use warnings FATAL => 'all';

use Carp;
use Exporter qw(import);

my $setup_ok;

# Last connection error.  By default new() dies on a failed connection, but
# with "nofail => 1" it returns undef instead (callers such as
# poll_until_connection() rely on that).  The undef path loses the libpq error
# message, so stash it here for callers that want to report why.
our $connect_error;

BEGIN
{
	# Use the FFI libpq wrapper.  This will fail if the FFI libraries are not
	# available.
	#
	# The actual setup is done per session, because we get the libdir from
	# the node object (in most cases).
	require PostgreSQL::PqFFI;
	PostgreSQL::PqFFI->import;
}

# Re-export the libpq status constants (CONNECTION_*, PGRES_*, PQTRANS_*, ...)
# so a test that uses this class can compare $res->{status} or conn_status()
# against named constants without separately importing PostgreSQL::PqConstants.
our @EXPORT = @PostgreSQL::PqConstants::EXPORT;

# libpq is loaded into the process exactly once, from the first session's
# libdir; all later sessions reuse it regardless of their node.  A modern
# libpq can talk to older servers, so cross-version tests (pg_upgrade) just
# need their FIRST session to use the newest installation's libpq.
sub _setup
{
	return if $setup_ok;
	my $libdir = shift;
	PostgreSQL::PqFFI::setup($libdir);
	$setup_ok = 1;
}

=pod

=head1 METHODS

=over

=item PostgreSQL::Test::Session->new(node=> $node [, dbname=> $dbname ])

Set up a new session for the node, which must be a C<PostgreSQL::Test::Cluster>
instance. The default dbame is C<postgres>.

=item PostgreSQL::Test::Session->new(connstr => $connstr [, libdir => $libdir])

Set up a new session for the connection string. If using the FFI libpq wrapper,
C<$libdir> must point to the directory where the libpq library is installed.

By default C<new> dies if the connection cannot be established. Pass
C<< nofail => 1 >> to return C<undef> on failure instead (the libpq error is
then available in C<$PostgreSQL::Test::Session::connect_error>); the retrying
callers C<poll_until_connection> and C<poll_query_until> rely on this.

=cut

sub new
{
	my $class = shift;
	my $self = {};
	bless $self, $class;
	my %args = @_;
	my $node = $args{node};
	my $dbname = $args{dbname} || 'postgres';
	my $libdir = $args{libdir};
	my $connstr = $args{connstr};
	my $user = $args{user};
	my $wait = $args{wait} // 1;
	my $nofail = $args{nofail};
	unless ($setup_ok)
	{
		unless ($libdir)
		{
			croak "bad node" unless $node->isa("PostgreSQL::Test::Cluster");
			$libdir = $node->config_data($^O eq 'MSWin32' ? '--bindir' : '--libdir');
		}
		_setup($libdir);
	}
	unless ($connstr)
	{
		croak "bad node" unless $node->isa("PostgreSQL::Test::Cluster");
		$connstr = $node->connstr($dbname);
	}

	# Pin the connecting role unless the connection string already names one.
	# With no "user" in the connection string, libpq falls back to PGUSER (and
	# only then to the operating-system user).  A stray PGUSER in the
	# environment -- as set on the buildfarm -- would then select a role that
	# the cluster's authentication setup does not recognize; in particular the
	# SSPI usermap used on Windows is built around the OS user by
	# "pg_regress --config-auth", so a PGUSER of e.g. "buildfarm" fails
	# authentication.  Default to the OS user, which is how the rest of the
	# test framework connects, unless the caller (or the connection string)
	# requested a specific user.
	if ($connstr !~ /\buser\s*=/)
	{
		$user //= $^O eq 'MSWin32' ? $ENV{USERNAME} : (getpwuid($<))[0];
		$connstr .= " user='$user'" if defined $user && $user ne '';
	}
	$self->{connstr} = $connstr;
	$self->{notices} = [];
	$self->{timeout} = $args{timeout} // $PostgreSQL::Test::Utils::timeout_default;

	if ($wait)
	{
		$self->{conn} = PQconnectdb($connstr);
		# The destructor will clean up for us even if we fail
		unless (PQstatus($self->{conn}) == CONNECTION_OK)
		{
			$connect_error = PQerrorMessage($self->{conn});
			return undef if $nofail;
			croak "could not connect to \"$connstr\": $connect_error";
		}
		$self->_setup_notice_processor();
		return $self;
	}
	else
	{
		$self->{conn} = PQconnectStart($connstr);
		if (PQstatus($self->{conn}) == CONNECTION_BAD)
		{
			$connect_error = PQerrorMessage($self->{conn});
			return undef if $nofail;
			croak "could not start connection to \"$connstr\": $connect_error";
		}
		return $self;
	}
}

# Set up a notice processor to capture notices/warnings
sub _setup_notice_processor
{
	my $self = shift;

	# Only available with FFI backend
	return unless defined &create_notice_processor;

	my $notices = $self->{notices};

	# Create closure that captures notices into our array
	$self->{_notice_closure} = create_notice_processor(sub {
		my ($arg, $message) = @_;
		push @$notices, $message;
	});

	PQsetNoticeProcessor($self->{conn}, $self->{_notice_closure}, undef);
}


# for a connection started with PQconnectStart, wait until it is in CONNECTION_OK state.
# Uses PQconnectPoll to drive the async connection forward.
sub wait_connect
{
	my $self = shift;
	my $conn = $self->{conn};
	my $timeout = $PostgreSQL::Test::Utils::timeout_default;
	my $start = time;
	while (1)
	{
		my $poll_res = PQconnectPoll($conn);
		my $status = PQstatus($conn);

		# Connection is complete
		if ($poll_res == PGRES_POLLING_OK || $status == CONNECTION_OK)
		{
			$self->_setup_notice_processor();
			return;
		}

		# Connection failed
		if ($poll_res == PGRES_POLLING_FAILED || $status == CONNECTION_BAD)
		{
			die "connection failed: " . PQerrorMessage($conn);
		}

		die "timed out waiting for connection" if time - $start > $timeout;

		# Wait on socket based on what PQconnectPoll needs
		my $socket = PQsocket($conn);
		if ($socket >= 0)
		{
			my $forRead = ($poll_res == PGRES_POLLING_READING) ? 1 : 0;
			my $forWrite = ($poll_res == PGRES_POLLING_WRITING) ? 1 : 0;
			my $end_time = PQgetCurrentTimeUSec() + 1_000_000;  # 1 second
			PQsocketPoll($socket, $forRead, $forWrite, $end_time);
		}
	}
}

# Single step of async connection polling - drives the connection state machine
# forward without blocking. Returns the poll result (PGRES_POLLING_* constant).
sub poll_connect
{
	my $self = shift;
	my $conn = $self->{conn};
	return PQconnectPoll($conn);
}

=pod

=item $session->close()

Close the connection

=cut

sub close
{
	my $self = shift;
	PQfinish($self->{conn});
	delete $self->{conn};
}

# Alias for compatibility with BackgroundPsql
*quit = \&close;

# close the session if the object goes out of scope
sub DESTROY
{
	my $self = shift;

	# During global destruction the FFI::Platypus bindings (and any notice
	# processor closure registered with libpq) may already have been torn
	# down in an unpredictable order.  Calling PQfinish() then can invoke a
	# freed notice callback or a freed FFI thunk, dying with "Can't use an
	# undefined value as a subroutine reference".  The process is exiting
	# anyway, so just let the OS reclaim the socket.
	return if ${^GLOBAL_PHASE} eq 'DESTRUCT';

	$self->close if exists $self->{conn};
}

=pod

=item $session->reconnect()

Reopen the session using the original connstr. If the session is still open,
close it before reopening.

=cut

sub reconnect
{
	my $self = shift;
	$self->close if exists $self->{conn};
	$self->{conn} = PQconnectdb($self->{connstr});
	my $status = PQstatus($self->{conn});
	if ($status == CONNECTION_OK)
	{
		$self->_setup_notice_processor();
	}
	return $status;
}

=pod

=item $session->reconnect_and_clear()

Reconnect and clear all captured notices. Returns the connection status.

=cut

sub reconnect_and_clear
{
	my $self = shift;
	my $status = $self->reconnect();
	$self->clear_notices();
	return $status;
}

=pod

=item $session->get_notices_str()

Return all captured notices/warnings as a single string (joined by empty string).
This is similar to how BackgroundPsql's {stderr} field works.

=cut

sub get_notices_str
{
	my $self = shift;
	return join('', @{$self->{notices}});
}

=pod

=item $session->clear_notices()

Clear all captured notices/warnings.

=cut

sub clear_notices
{
	my $self = shift;
	# Clear in place - don't replace the array, as the notice processor
	# closure has a reference to it
	@{$self->{notices}} = ();
}

=pod

=item $session->get_stderr()

Return a stderr-like string containing all notices plus any error message
from the last query. This mimics BackgroundPsql's {stderr} behavior.

=cut

sub get_stderr
{
	my $self = shift;
	my $stderr = $self->get_notices_str();
	if (exists $self->{last_error} && defined $self->{last_error})
	{
		$stderr .= $self->{last_error};
	}
	return $stderr;
}

=pod

=item $session->clear_stderr()

Clear notices and last error, like setting $psql->{stderr} = ''.

=cut

sub clear_stderr
{
	my $self = shift;
	$self->clear_notices();
	delete $self->{last_error};
}

=pod

=item $session->conn_status()

Return the connection status. This will be a libpq status value like
C<CONNECTION_OK>.

=cut

sub conn_status
{
	my $self = shift;
	return exists $self->{conn} ? PQstatus($self->{conn}) : undef;
}

=pod

=item $session->connected()

Return true if the session has a live connection (status C<CONNECTION_OK>).

=cut

sub connected
{
	my $self = shift;
	return exists $self->{conn} && PQstatus($self->{conn}) == CONNECTION_OK;
}

=pod

=item $session->backend_pid()

Return the backend process ID for this connection.

=cut

sub backend_pid
{
	my $self = shift;
	return PQbackendPID($self->{conn});
}

=pod

=item $session->do($sql, ...)

Run one or more SQL statements synchronously (using C<PQexec>). The statements
should not return any tuples. Returns the status, which will be
C<PGRES_COMMAND_OK> (i.e. 1) in the case of success.

=cut

sub do
{
	my $self = shift;
	my $conn = $self->{conn};
	my $status;
	foreach my $sql (@_)
	{
		my $result = PQexec($conn, $sql);
		$status = PQresultStatus($result);
		PQclear($result);
		return $status unless $status == PGRES_COMMAND_OK;
	}
	return $status;
}

=pod

=item $session->do_async($sql)

Run a single statement asynchronously, using C<PQsendQuery>. The return value
is a boolean indicating success.

=cut

sub do_async
{
	my $self = shift;
	my $conn = $self->{conn};
	my $sql = shift;
	my $result = PQsendQuery($conn, $sql);
	return $result; # 1 or 0
}

# get the next resultset from some async commands
# wait if necessary using PQsocketPoll
# c.f. libpqsrv_get_result
sub _get_result
{
	my $conn = shift;
	my $timeout = shift // $PostgreSQL::Test::Utils::timeout_default;
	my $socket = PQsocket($conn);
	my $deadline = PQgetCurrentTimeUSec() + $timeout * 1_000_000;
	while (PQisBusy($conn))
	{
		# A query sent on a non-blocking connection may not be fully flushed
		# yet; PQflush() tells us whether we must also wait for the socket to
		# become writable (waiting only for readable would deadlock, since the
		# server can't reply to a request it hasn't received).
		my $flush = PQflush($conn);
		croak "PQflush failed: " . PQerrorMessage($conn) if $flush < 0;

		my $now = PQgetCurrentTimeUSec();
		croak "timed out waiting for query result" if $now >= $deadline;

		# Wake at least once a second so the deadline is rechecked even if the
		# socket never becomes ready.
		my $end = $now + 1_000_000;
		$end = $deadline if $deadline < $end;
		PQsocketPoll($socket, 1, ($flush > 0 ? 1 : 0), $end);

		last if PQconsumeInput($conn) == 0;
	}
	return PQgetResult($conn);
}

# Drain and discard any results still pending on the connection.
sub _drain_results
{
	my ($conn, $timeout) = @_;
	while (my $r = _get_result($conn, $timeout))
	{
		PQclear($r);
	}
}

=pod

=item $session->wait_for_completion()

Wait until all asynchronous SQL has completed

=cut

sub wait_for_completion
{
	# wait for all the resultsets and clear them
	# c.f. libpqsrv_get_result_last
	my $self = shift;
	_drain_results($self->{conn}, $self->{timeout});
}

=pod

=item $session->get_async_result()

Wait for and return the result of an async query as a result hash.
Clears any subsequent results.

=cut

sub get_async_result
{
	my $self = shift;
	my $conn = $self->{conn};
	my $result = _get_result($conn, $self->{timeout});
	return undef unless $result;
	my $res = _get_result_data($result, $conn);
	PQclear($result);
	_drain_results($conn, $self->{timeout});    # clear any remaining results
	return $res;
}

=pod

=item  $session->set_password($user, $password)

Set the user's password by calling C<PQchangePassword>.

Returns a result hash.

=cut

# set password for user
sub set_password
{
	my $self = shift;
	my $user = shift;
	my $password = shift;
	my $conn = $self->{conn};
	my $result = PQchangePassword($conn, $user, $password);
	my $ret = _get_result_data($result, $conn);
	PQclear($result);
	return $ret;
}

# Common internal routine to process result data.
# The returned object is dead and will be garbage collected as necessary.

sub _get_result_data
{
	my $result = shift;
	my $conn = shift;
	my $status = PQresultStatus($result);
	my $res = {	status => $status, names => [], types => [], rows => [],
			psqlout => ""};
	unless ($status == PGRES_TUPLES_OK || $status == PGRES_COMMAND_OK)
	{
		$res->{error_message} = PQerrorMessage($conn);
		return $res;
	}
	if ($status == PGRES_COMMAND_OK)
	{
		return $res;
	}
	my $ntuples = PQntuples($result);
	my $nfields = PQnfields($result);
	# assuming here that the strings returned by PQfname and PQgetvalue
	# are mapped into perl space using setsvpv or similar and thus won't
	# be affect by us calling PQclear on the result object.
	foreach my $field (0 .. $nfields-1)
	{
		push(@{$res->{names}}, PQfname($result, $field));
		push(@{$res->{types}}, PQftype($result, $field));
	}
	my @textrows;
	foreach my $nrow (0 .. $ntuples - 1)
	{
		my $row = [];
		foreach my $field ( 0 .. $nfields - 1)
		{
			my $val = PQgetvalue($result, $nrow, $field);
			if (($val // "") eq "")
			{
				$val = undef if PQgetisnull($result, $nrow, $field);
			}
			push(@$row, $val);
		}
		push(@{$res->{rows}}, $row);
		no warnings qw(uninitialized);
		push(@textrows, join('|', @$row));
	}
	$res->{psqlout} = join("\n",@textrows) if $ntuples;
	return $res;
}


=pod

=item $session->query($sql)

Runs sql that might return tuples.

Returns a result hash.

=cut

sub query
{
	my $self = shift;
	my $sql = shift;
	my $conn = $self->{conn};

	# Use PQsendQuery + PQgetResult to handle multi-statement SQL properly.
	# This collects results from all statements and returns the last one
	# that had tuples, similar to how psql works.
	PQsendQuery($conn, $sql) or do {
		return { status => -1, error_message => PQerrorMessage($conn),
				 names => [], types => [], rows => [], psqlout => "" };
	};

	my $final_res;
	my $last_error;
	my $error_status;
	my @all_psqlout;

	while (my $result = _get_result($conn, $self->{timeout}))
	{
		my $res = _get_result_data($result, $conn);
		PQclear($result);

		# Collect output from all statements
		push @all_psqlout, $res->{psqlout} if $res->{psqlout} ne "";

		# Track errors
		if (exists $res->{error_message})
		{
			$last_error = $res->{error_message};
			$error_status = $res->{status};
		}

		# Keep the last result that had tuples, or the last result overall
		if ($res->{status} == PGRES_TUPLES_OK || !defined $final_res)
		{
			$final_res = $res;
		}
	}

	$final_res //= { status => PGRES_COMMAND_OK, names => [], types => [],
					 rows => [], psqlout => "" };

	# Combine all output (like psql does)
	$final_res->{psqlout} = join("\n", @all_psqlout) if @all_psqlout;

	# If there was any error, include it in the result for query_safe
	$final_res->{error_message} = $last_error if defined $last_error;

	# If any statement errored, reflect that in the status even when an
	# earlier statement returned tuples, so callers that check status (not
	# just error_message) see the failure.
	$final_res->{status} = $error_status if defined $error_status;

	# Store error for get_stderr()
	$self->{last_error} = $last_error;

	# A multi-statement query that errors can leave the session in an open,
	# aborted transaction: libpq aborts processing of the query string at the
	# error, so a trailing COMMIT (e.g. "BEGIN; <error>; COMMIT") never runs.
	# Roll back so a later query on this reused session is not rejected with
	# "current transaction is aborted".
	if (PQtransactionStatus($conn) == PQTRANS_INERROR)
	{
		my $rb = PQexec($conn, "ROLLBACK");
		PQclear($rb) if $rb;
	}

	return $final_res;
}

=pod

=item $session->query_safe($sql)

Runs sql that might return tuples, croaking if there's an error.
Returns the psqlout string (like psql -At output) on success.

=cut

sub query_safe
{
	my $self = shift;
	my $sql = shift;
	my $res = $self->query($sql);
	if (exists $res->{error_message}) {
		# Debug: show where the error occurred
		my $short_sql = substr($sql, 0, 100);
		$short_sql =~ s/\s+/ /g;
		croak "query_safe failed on [$short_sql...]: $res->{error_message}";
	}
	return $res->{psqlout};
}

=pod

=item $session->query_oneval($sql [, $missing_ok ] )

Run a query that is expected to return no more than one tuple with one value;

If C<$missing_ok> is true, return undef if the query returns no tuple. Otherwise
croak if there is not exactly one tuple, or of the tuple does not have
exctly one value.

If none of these apply, return the single value from the query. A NULL value
will result in undef, so if C<$missing_ok> is true you won't be able to
distinguish between a null value and a missing tuple.

A non NULL value is returned as the string value obtained from C<PQgetvalue>.

=cut

sub query_oneval
{
	my $self = shift;
	my $sql = shift;
	my $missing_ok = shift; # default is not ok
	my $conn = $self->{conn};
	my $result = PQexec($conn, $sql);
	my $status = PQresultStatus($result);
	unless  ($status == PGRES_TUPLES_OK)
	{
		PQclear($result) if $result;
		croak PQerrorMessage($conn);
	}
	my $ntuples = PQntuples($result);
	return undef if ($missing_ok && !$ntuples);
	my $nfields = PQnfields($result);
	croak "$ntuples tuples != 1 or $nfields fields != 1"
	  if $ntuples != 1 || $nfields != 1;
	my $val = PQgetvalue($result, 0, 0);
	if ($val eq "")
	{
		$val = undef if PQgetisnull($result, 0, 0);
	}
	PQclear($result);
	return $val;
}

=pod

=item $session->query_tuples($sql, ...)

Run the sql commands and return the output as a single piece of text in the
same format as C<psql -A -t>.

Fields within tuples are separated by a "|", tuples are spearated by "\n"

=cut

sub query_tuples
{
	my $self = shift;
	# Use pipelined version for 4+ queries where the overhead is worth it
	return $self->query_tuples_pipelined(@_) if @_ >= 4;

	my @results;
	foreach my $sql (@_)
	{
		my $res = $self->query($sql);
		croak $res->{error_message}
		  unless $res->{status} == PGRES_TUPLES_OK;
		# query() already built psqlout in "psql -A -t" form.  Skip only when
		# there are no rows; a row that renders as the empty string is still
		# emitted (an empty result was previously skipped with "-- empty"
		# commented out, as that broke at least one test).
		push(@results, $res->{psqlout}) if @{$res->{rows}};
	}
	return join("\n", @results);
}

=pod

=item $session->query_tuples_pipelined($sql, ...)

Run multiple SQL queries using pipeline mode for efficiency. Returns output
in the same format as C<query_tuples> but with only one network round-trip
for all queries.

=cut

sub query_tuples_pipelined
{
	my $self = shift;
	my @queries = @_;
	my $conn = $self->{conn};
	my @results;

	# Enter pipeline mode
	PQenterPipelineMode($conn) or croak "Failed to enter pipeline mode";

	# Send all queries using PQsendQueryParams (PQsendQuery not allowed in pipeline mode)
	for my $sql (@queries)
	{
		PQsendQueryParams($conn, $sql, 0, undef, undef, undef, undef, 0) or do {
			PQexitPipelineMode($conn);
			croak "Failed to send query: " . PQerrorMessage($conn);
		};
	}

	# Mark end of pipeline
	PQpipelineSync($conn) or do {
		PQexitPipelineMode($conn);
		croak "Failed to sync pipeline";
	};

	# Collect results for each query
	for my $i (0 .. $#queries)
	{
		my $result = _get_result($conn, $self->{timeout});
		if (!$result)
		{
			PQexitPipelineMode($conn);
			croak "No result for query $i";
		}

		my $status = PQresultStatus($result);
		if ($status == PGRES_PIPELINE_ABORTED)
		{
			PQclear($result);
			PQexitPipelineMode($conn);
			croak "Pipeline aborted at query $i";
		}

		if ($status == PGRES_TUPLES_OK)
		{
			my $res = _get_result_data($result, $conn);
			my $rows = $res->{rows};
			if (@$rows)
			{
				no warnings qw(uninitialized);
				my @tuples = map { join('|', @$_); } @$rows;
				push(@results, join("\n", @tuples));
			}
		}
		elsif ($status != PGRES_COMMAND_OK)
		{
			my $err = PQerrorMessage($conn);
			PQclear($result);
			PQexitPipelineMode($conn);
			croak "Query $i failed: $err";
		}
		PQclear($result);

		# Consume the NULL result that marks end of this query's results
		while (my $extra = PQgetResult($conn))
		{
			PQclear($extra);
		}
	}

	# Consume the pipeline sync result
	my $sync_result = _get_result($conn, $self->{timeout});
	if ($sync_result)
	{
		my $status = PQresultStatus($sync_result);
		PQclear($sync_result);
		if ($status != PGRES_PIPELINE_SYNC)
		{
			PQexitPipelineMode($conn);
			croak "Expected PGRES_PIPELINE_SYNC, got $status";
		}
	}

	# Exit pipeline mode
	PQexitPipelineMode($conn) or croak "Failed to exit pipeline mode";

	return join("\n", @results);
}


sub setnonblocking
{
	my $self = shift;
	my $val = shift;
	my $res = PQsetnonblocking($self->{conn}, $val);
	croak "problem setting non-blocking"
	  if $res;
	return;
}

sub enterPipelineMode
{
	my $self = shift;
	return PQenterPipelineMode($self->{conn});
}

sub pipelineSync
{
	my $self = shift;
	return PQpipelineSync($self->{conn});
}


sub do_pipeline
{
	my $self = shift;
	my $statement = shift;
	my @args = @_;
	my $nargs = scalar(@args);
	my $res = PQsendQueryParams($self->{conn}, $statement, $nargs, undef, \@args, undef , undef, 0);
	return $res;
}

=pod

=item $session->get_notification()

Check for a pending notification and return it as a hashref with keys
C<channel>, C<pid>, and C<payload>. Returns undef if no notification is
available.

Automatically consumes any pending input before checking for notifications.

=cut

sub get_notification
{
	my $self = shift;
	my $conn = $self->{conn};

	# Consume any pending input
	PQconsumeInput($conn);

	my $notify = PQnotifies($conn);
	return undef unless $notify;

	my $result = {
		channel => PQnotify_channel($notify),
		pid     => PQnotify_be_pid($notify),
		payload => PQnotify_payload($notify),
	};

	PQnotify_free($notify);

	return $result;
}

=pod

=item $session->get_all_notifications()

Consume input and return all pending notifications as an arrayref of hashrefs.
Each hashref has keys C<channel>, C<pid>, and C<payload>.

=cut

sub get_all_notifications
{
	my $self = shift;
	my @notifications;

	while (my $notify = $self->get_notification())
	{
		push @notifications, $notify;
	}

	return \@notifications;
}

=pod

=back

=cut


1;
