
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test of the PG_SETLOCALE_MAP mechanism for renaming locales on the fly.  This
# always runs on Windows.  It is usually skipped on Unix.

# For the benefit of Unix-based developers, it also runs if PG_TEST_EXTRA
# contains DEBUG_SETLOCALE_MAP, and is expected to pass only if PostgreSQL was
# compiled with the macro DEBUG_SETLOCALE_MAP defined (see pg_config_manual.h).
# In that case, locales "en_US.UTF-8" and "fr_FR.UTF-8" are assumed to exist,
# which may require extra system packages on some systems.

# The assumption this test makes about locales is that "C" sorts lower case
# before upper case (this is true by definition), while natural language
# locales do not (true in practice).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (!$windows_os && $ENV{PG_TEST_EXTRA} !~ /\bDEBUG_SETLOCALE_MAP\b/)
{
	plan skip_all =>
	  'setlocale mapping on non-Windows requires DEBUG_SETLOCALE_MAP';
}

my ($encoding, $locale1, $locale2, $locale1_pattern);

if ($windows_os)
{
	# the purpose of this facility is to deal with changes in the spelling of
	# the locale name, by allowing people a way to transition to BCP 47 names,
	# so we'll use that in our example
	$encoding = 'WIN1252';
	$locale1 = 'English_United States.1252';
	$locale2 = 'en-US';
	$locale1_pattern = 'English_*.1252';
}
else
{
	# when testing on Unix, we don't expect there to be another way to spell
	# the same locale so we'll just pick another language (normally you would
	# never want to do this)
	$encoding = 'UTF8';
	$locale1 = 'en_US.UTF-8';
	$locale2 = 'fr_FR.UTF-8';
	$locale1_pattern = 'en_*.UTF-8';
}

my $pg_setlocale_map =
  $PostgreSQL::Test::Utils::tmp_check . "/my_setlocale.map";
$ENV{PG_SETLOCALE_MAP} = $pg_setlocale_map;
$ENV{PG_TEST_INITDB_EXTRA_OPTS} = "--locale='$locale1' --encoding=$encoding";

sub install_setlocale_map
{
	my $contents = shift;
	open my $f, ">", $pg_setlocale_map or die $!;
	print $f $contents;
	close $f;
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(force_initdb => 1);
$node->append_conf('postgresql.conf', 'lc_messages=C');
$node->append_conf('postgresql.conf', 'lc_monetary=C');
$node->append_conf('postgresql.conf', 'lc_numeric=C');
$node->append_conf('postgresql.conf', 'lc_time=C');

$node->start;
is($node->safe_psql('postgres', "select 'Z' > 'a'"),
	't', "natural language order");
$node->stop;

# install some non-matching mapping rules, see that nothing changes
install_setlocale_map <<"EOF";
foo*=C
EOF

$node->start;
is($node->safe_psql('postgres', "select 'Z' > 'a'"),
	't', "still natural language order");
$node->stop;

# install some matching mapping rules that will give us "C"; use the pattern
# format just to exercise the pattern matching code
install_setlocale_map <<"EOF";
$locale1_pattern=C
EOF

$node->start;
is($node->safe_psql('postgres', "select 'a' > 'Z'"), 't', "encoding order");
$node->stop;

# install a mapping to a second natural language, and check that we can
# create a database from template0
install_setlocale_map <<"EOF";
$locale1=$locale2
EOF

$node->start;
is($node->safe_psql('postgres', "select 'Z' > 'a'"),
	't', "natural language order via second locale name");
$node->safe_psql('postgres',
	"create database regression_db2 template=template0");

# unfortunately we can't create a database from the default template1
# because now the locale names don't match!
is($node->safe_psql('postgres', "select 'Z' > 'a'"),
	't', "natural language order via second locale name");
my ($result, $stdout, $stderr) =
  $node->psql('postgres', "create database regression_db3");
ok( $stderr =~
	  /ERROR:  new collation \($locale2\) is incompatible with the collation of the template database \($locale1\)/,
	"cannot create new database because of locale mismatch with template1");

# it's the same even if we explicitly give the locale name, old or new
($result, $stdout, $stderr) =
  $node->psql('postgres', "create database regression_db3 locale='$locale1'");
ok( $stderr =~
	  /ERROR:  new collation \($locale2\) is incompatible with the collation of the template database \($locale1\)/,
	"cannot create new database because of locale mismatch with template1, explicit old"
);
($result, $stdout, $stderr) =
  $node->psql('postgres', "create database regression_db3 locale='$locale2'");
ok( $stderr =~
	  /ERROR:  new collation \($locale2\) is incompatible with the collation of the template database \($locale1\)/,
	"cannot create new database because of locale mismatch with template1, explicit new"
);

# to fix that problem we have to do the renaming in the catalog (note that the
# mapping file still did something very useful by letting us log in to do this,
# in the case where $LOCALE1 is no longer recognized by the OS)
$node->safe_psql('postgres',
	"update pg_database set datcollate = '$locale2' where datlocprovider = 'c' and datcollate = '$locale1'"
);
$node->safe_psql('postgres',
	"update pg_database set datctype = '$locale2' where datlocprovider = 'c' and datctype = '$locale1'"
);

# now it should work, and we don't really need the mapping file anymore except
# to deal with uses of the old name in postgresql.conf
$node->safe_psql('postgres', "create database regression_db3");

$node->stop;

done_testing();
