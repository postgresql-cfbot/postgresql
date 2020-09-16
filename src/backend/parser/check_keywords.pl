#!/usr/bin/perl

# Check that the keyword lists in gram.y and kwlist.h are sane.
# Usage: check_keywords.pl gram.y kwlist.h

# src/backend/parser/check_keywords.pl
# Copyright (c) 2009-2020, PostgreSQL Global Development Group

use warnings;
use strict;

my $gram_filename   = $ARGV[0];
my $kwlist_filename = $ARGV[1];

my $errors = 0;

sub error
{
	print STDERR @_;
	$errors = 1;
	return;
}

sub check_alphabetical_order
{
	my ($listname, $list) = @_;
	my $prevkword = '';
	my $implicit_alias_kword;

	foreach my $kword (@$list)
	{

		# Some keyword have a _P suffix. Remove it for the comparison.
		$implicit_alias_kword = $kword;
		$implicit_alias_kword =~ s/_P$//;
		if ($implicit_alias_kword le $prevkword)
		{
			error
			  "'$implicit_alias_kword' after '$prevkword' in $listname list is misplaced";
		}
		$prevkword = $implicit_alias_kword;
	}
}

$, = ' ';     # set output field separator
$\ = "\n";    # set output record separator

my %keyword_categories;
$keyword_categories{'unreserved_keyword'}     = 'UNRESERVED_KEYWORD';
$keyword_categories{'col_name_keyword'}       = 'COL_NAME_KEYWORD';
$keyword_categories{'type_func_name_keyword'} = 'TYPE_FUNC_NAME_KEYWORD';
$keyword_categories{'reserved_keyword'}       = 'RESERVED_KEYWORD';

open(my $gram, '<', $gram_filename) || die("Could not open : $gram_filename");

my $kcat;
my $implicit_alias;
my $comment;
my @arr;
my %keywords;
my @implicit_alias;

line: while (my $S = <$gram>)
{
	chomp $S;    # strip record separator

	my $s;

	# Make sure any braces are split
	$s = '{', $S =~ s/$s/ { /g;
	$s = '}', $S =~ s/$s/ } /g;

	# Any comments are split
	$s = '[/][*]', $S =~ s#$s# /* #g;
	$s = '[*][/]', $S =~ s#$s# */ #g;

	if (!($kcat) && !($implicit_alias))
	{

		# Is this the beginning of a keyword list?
		foreach my $k (keys %keyword_categories)
		{
			if ($S =~ m/^($k):/)
			{
				$kcat = $k;
				next line;
			}
		}

		# Is this the beginning of the implicit_alias_keyword list?
		$implicit_alias = 1 if ($S =~ m/^implicit_alias_keyword:/);

		next line;
	}

	# Now split the line into individual fields
	my $n = (@arr = split(' ', $S));

	# Ok, we're in a keyword list. Go through each field in turn
	for (my $fieldIndexer = 0; $fieldIndexer < $n; $fieldIndexer++)
	{
		if ($arr[$fieldIndexer] eq '*/' && $comment)
		{
			$comment = 0;
			next;
		}
		elsif ($comment)
		{
			next;
		}
		elsif ($arr[$fieldIndexer] eq '/*')
		{

			# start of a multiline comment
			$comment = 1;
			next;
		}
		elsif ($arr[$fieldIndexer] eq '//')
		{
			next line;
		}

		if ($arr[$fieldIndexer] eq ';')
		{

			# end of keyword list
			undef $kcat;
			undef $implicit_alias;
			next;
		}

		if ($arr[$fieldIndexer] eq '|')
		{
			next;
		}

		# Put this keyword into the right list
		if ($implicit_alias)
		{
			push @implicit_alias, $arr[$fieldIndexer];
		}
		else
		{
			push @{ $keywords{$kcat} }, $arr[$fieldIndexer];
		}
	}
}
close $gram;

# Check that each keyword list is in alphabetical order (just for neatnik-ism)
check_alphabetical_order($_, $keywords{$_}) for (keys %keyword_categories);
check_alphabetical_order('implicit_alias_keyword', \@implicit_alias);

# Transform the keyword lists into hashes.
# kwhashes is a hash of hashes, keyed by keyword category id,
# e.g. UNRESERVED_KEYWORD.
# Each inner hash is keyed by keyword id, e.g. ABORT_P, with a dummy value.
my %kwhashes;
while (my ($kcat, $kcat_id) = each(%keyword_categories))
{
	@arr = @{ $keywords{$kcat} };

	my $hash;
	foreach my $item (@arr) { $hash->{$item} = 1; }

	$kwhashes{$kcat_id} = $hash;
}
my %implicit_alias = map { $_ => 1 } @implicit_alias;

# Now read in kwlist.h

open(my $kwlist, '<', $kwlist_filename)
  || die("Could not open : $kwlist_filename");

my $prevkwstring = '';
my $implicit_alias_kwname;
my %kwhash;
kwlist_line: while (<$kwlist>)
{
	my ($line) = $_;

	if ($line =~ /^PG_KEYWORD\(\"(.*)\", (.*), (.*), (.*)\)/)
	{
		my ($kwstring) = $1;
		my ($kwname)   = $2;
		my ($kwcat_id) = $3;
		my ($aliastype) = $4;

		# Check that the keyword label aliastype value matches treatment in gram.y
		if ($aliastype eq 'IMPLICIT_ALIAS')
		{
			unless ($implicit_alias{$kwname})
			{
				error
				  "'$kwstring' in kwlist.h is marked as implicit_alias, but is missing from gram.y's implicit_alias_keyword rule";
			}
		}
		elsif ($aliastype eq 'EXPLICIT_ALIAS')
		{
			if ($implicit_alias{$kwname})
			{
				error
				  "'$kwname' in kwlist.h is marked as explicit, but is listed in gram.y's implicit_alias_keyword rule";
			}
		}
		else
		{
			error
			  "'$aliastype' not recognized in kwlist.h.  Expected either 'IMPLICIT_ALIAS' or 'EXPLICIT_ALIAS'";
		}

		# Check that the list is in alphabetical order (critical!)
		if ($kwstring le $prevkwstring)
		{
			error
			  "'$kwstring' after '$prevkwstring' in kwlist.h is misplaced";
		}
		$prevkwstring = $kwstring;

		# Check that the keyword string is valid: all lower-case ASCII chars
		if ($kwstring !~ /^[a-z_]+$/)
		{
			error
			  "'$kwstring' is not a valid keyword string, must be all lower-case ASCII chars";
		}

		# Check that the keyword name is valid: all upper-case ASCII chars
		if ($kwname !~ /^[A-Z_]+$/)
		{
			error
			  "'$kwname' is not a valid keyword name, must be all upper-case ASCII chars";
		}

		# Check that the keyword string matches keyword name
		$implicit_alias_kwname = $kwname;
		$implicit_alias_kwname =~ s/_P$//;
		if ($implicit_alias_kwname ne uc($kwstring))
		{
			error
			  "keyword name '$kwname' doesn't match keyword string '$kwstring'";
		}

		# Check that the keyword is present in the grammar
		%kwhash = %{ $kwhashes{$kwcat_id} };

		if (!(%kwhash))
		{
			error "Unknown keyword category: $kwcat_id";
		}
		else
		{
			if (!($kwhash{$kwname}))
			{
				error "'$kwname' not present in $kwcat_id section of gram.y";
			}
			else
			{

				# Remove it from the hash, so that we can
				# complain at the end if there's keywords left
				# that were not found in kwlist.h
				delete $kwhashes{$kwcat_id}->{$kwname};
			}
		}
	}
}
close $kwlist;

# Check that we've paired up all keywords from gram.y with lines in kwlist.h
while (my ($kwcat, $kwcat_id) = each(%keyword_categories))
{
	%kwhash = %{ $kwhashes{$kwcat_id} };

	for my $kw (keys %kwhash)
	{
		error "'$kw' found in gram.y $kwcat category, but not in kwlist.h";
	}
}

exit $errors;
