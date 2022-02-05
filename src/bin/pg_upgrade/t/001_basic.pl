use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Test::More tests => 8;

program_help_ok('pg_upgrade');
program_version_ok('pg_upgrade');
program_options_handling_ok('pg_upgrade');
