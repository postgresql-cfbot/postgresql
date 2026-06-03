
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

=pod

=head1 NAME

PostgreSQL::FindLib - find shared libraries for PostgreSQL TAP tests

=head1 SYNOPSIS

  use PostgreSQL::FindLib;

  my $libpath = find_lib_or_die(
      lib     => 'pq',
      libpath => ['/usr/local/pgsql/lib'],
  );

=head1 DESCRIPTION

This module provides a simple mechanism to locate shared libraries,
used as a lightweight replacement for C<FFI::CheckLib>. It searches
for libraries in specified paths and common system locations.

=head1 EXPORTED FUNCTIONS

=over

=item find_lib_or_die(%args)

Searches for a shared library and returns its full path. Dies if the
library cannot be found.

Arguments:

=over

=item lib => $name

Required. The library name without prefix or suffix (e.g., C<'pq'> for
C<libpq.so>).

=item libpath => \@paths

Optional. Array of directories to search first.

=item systempath => \@paths

Optional. If set to an empty array C<[]>, system paths will not be searched.

=back

=back

=cut

package PostgreSQL::FindLib;

use strict;
use warnings FATAL => qw(all);

use Exporter qw(import);
use File::Spec;
use Config;

our @EXPORT = qw(find_lib_or_die);

sub find_lib_or_die
{
	my %args = @_;

	my $libname = $args{lib} or die "find_lib_or_die: 'lib' argument required";
	my $libpath = $args{libpath} // [];
	my $systempath = $args{systempath};

	my @search_paths = @$libpath;

	# Add system paths unless explicitly disabled
	unless (defined $systempath && ref($systempath) eq 'ARRAY' && @$systempath == 0)
	{
		push @search_paths, _get_system_lib_paths();
	}

	# Determine library file patterns based on OS
	my @patterns = _get_lib_patterns($libname);

	for my $dir (@search_paths)
	{
		next unless -d $dir;

		for my $pattern (@patterns)
		{
			my @matches = glob(File::Spec->catfile($dir, $pattern));
			for my $match (@matches)
			{
				return $match if -f $match && -r $match;
			}
		}
	}

	die "find_lib_or_die: unable to find lib$libname in: " . join(", ", @search_paths);
}

sub _get_lib_patterns
{
	my $libname = shift;

	if ($^O eq 'darwin')
	{
		return ("lib$libname.dylib", "lib$libname.*.dylib");
	}
	elsif ($^O eq 'MSWin32' || $^O eq 'cygwin')
	{
		return ("$libname.dll", "lib$libname.dll");
	}
	else
	{
		# Linux and other Unix-like systems
		return ("lib$libname.so", "lib$libname.so.*");
	}
}

sub _get_system_lib_paths
{
	my @paths;

	# Common system library paths
	push @paths, '/usr/lib', '/usr/local/lib', '/lib';

	# Add architecture-specific paths on Linux
	if ($^O eq 'linux')
	{
		push @paths, '/usr/lib/x86_64-linux-gnu', '/usr/lib/aarch64-linux-gnu';
		push @paths, '/usr/lib64', '/lib64';
	}

	# Add paths from LD_LIBRARY_PATH
	if ($ENV{LD_LIBRARY_PATH})
	{
		push @paths, split(/:/, $ENV{LD_LIBRARY_PATH});
	}

	# macOS specific
	if ($^O eq 'darwin')
	{
		push @paths, '/opt/homebrew/lib', '/usr/local/opt/libpq/lib';
		if ($ENV{DYLD_LIBRARY_PATH})
		{
			push @paths, split(/:/, $ENV{DYLD_LIBRARY_PATH});
		}
	}

	return @paths;
}

=pod

=head1 SEE ALSO

L<PostgreSQL::PqFFI>

=cut

1;
