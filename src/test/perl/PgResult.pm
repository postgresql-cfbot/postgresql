=pod

=head1 NAME

PgResult - class representing PostgreSQL result object

=head1 SYNOPSIS
  use Client;
  use Result;
  use Carp;

  my $conn = $server->get_new_session('postgres', 'session1');
  $result = $conn->exec('SELECT pg_backend_pid()');

  croak($conn->errorMessage())
     if ($result->resultStatus() ne "PGRES_TUPLES_OK");

  $ntuples = $result->getntuples();
  $nfields = $result->nfields();
  for $i (0 .. ($ntuples - 1))
  {
	$s = "";
	for $j (0 .. $nfields - 1)
	{
		$s .= $result->getvalue($i, $j);
	}
	print $s,"\n";
  }

  # get information.
  # see the corresponding functions of libpq. Several functions that
  # corresponding libpq function returns a enum value returns a string
  # representation

  $result->resultStatus()
  $result->clear()
  $result->getntuples()
  $result->nfields()
  $result->getvalue()
  $result->getlength()
  $result->getisnull()

=head1 DESCRIPTION

PgResult contains a set of routines to handle a result object obtained
from a query execution.

=cut

package PgResult;

use 5.016003;
use strict;
use warnings;

require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use PgResult ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw() ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw();

our $VERSION = '0.01';

require XSLoader;
XSLoader::load('PgResult', $VERSION);

# Preloaded methods go here.

1;
