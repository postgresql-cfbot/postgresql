use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('main');
$node->init();
$node->start();

# create a file under the test directory
# return its full path
sub create_test_file
{
	my ($fname, $contents) = @_;
	my $fn = $node->basedir . '/' . $fname;
	append_to_file($fn, $contents);
	return $fn;
}

# invoke psql
# - opts: space-separated options and arguments
#         -X is appended, unless opts starts with !
# - stat: expected exit status
# - in: input stream
# - out: list of re to check on stdout
# - err: list of re to check on stderr
# - name: of the test
# - more: more raw arguments
sub psql
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my ($opts, $stat, $in, $out, $err, $name, @more) = @_;
	$opts =~ s/^!// or push @more, '-X';
	my @cmd = ('psql', split(/\s+/, $opts), @more);
	$node->command_checks_all(\@cmd, $stat, $in, $out, $err, $name);
	return;
}

# invoke psql interactively, making it believe it is connected to a tty
# - opts: space-separated options and arguments
#         -X is appended, unless opts starts with !
# - stat: expected final exit status
# - timeout: for interactions, in second
# - inout: list of input expected re
# - name: of the test
# - remove: tell to remove init/start/end checks
sub ipsql
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my ($opts, $stat, $timeout, $inout, $name, $remove) = @_;

	# build command to run
	my @more = ();
	@more = ('-X') unless $opts =~ /^!/;
	$opts =~ s/^!//;
	my @cmd = ('psql', split(/\s+/, $opts), @more);

	# check it is running at start and end, and exit cleanly
	unless ($remove)
	{
		unshift @$inout, [ "\\echo STARTING :VERSION_NUM\n", qr/STARTING \d+.*postgres=\# /s ];
		push @$inout, [ "\\echo ENDING :VERSION_NUM\\q\n", qr/ENDING \d+$/s ];
	}

	# call the stuff
	$node->icommand_checks(
		\@cmd, $stat, $timeout,
		$remove ? undef : qr/psql .*Type "help" for help..*postgres=# /s,
		$inout, $name);
	return;
}

#
# check whether we have readline/history capability
#
my ($out, $err);
$node->psql('postgres', "\\s\n", 'stdout' => \$out, 'stderr' => \$err);
my $has_history = $err !~ /history is not supported by this installation/;

my $EMPTY = [ qr/^$/ ];

#
# check various options
#
# this cannot be tested from within a SQL script
psql('!--help', 0, '',
	 [ qr/interactive terminal/, qr/Usage:/, qr/Connection options:/ ],
	 $EMPTY, 'psql --help');
psql('!-?', 0, '',
	 [ qr/interactive terminal/, qr/Usage:/, qr/Connection options:/ ],
	 $EMPTY, 'psql -?');
psql('!--version', 0, '',
	 [ qr{^psql \(PostgreSQL\) \d+} ], $EMPTY, 'psql --version');
psql('--help=variables', 0, '',
	 [ qr{VERSION_NAME}, qr{unicode_border_linestyle}, qr{PSQL_EDITOR} ],
	 $EMPTY, 'psql --help=variables');
# look for plenty of backslash-commands
psql('--help=commands', 0, '',
	 [ qr{\\copyright}, qr{\\pset}, qr{(?!VERSION_NAME)} ],
	 $EMPTY, 'psql --help=commands');

# tested elsewhere: -a -A -d -e -h -L
psql('-b', 0, "SELECT 1/0;\n",
	 $EMPTY, [ qr{STATEMENT:  SELECT 1/0}, qr{ERROR:  division by zero} ], 'psql -b');
psql('-c \\q', 0, '', $EMPTY, $EMPTY, 'psql -c');
psql('-c \\l -c \\l', 0, '', [ qr{template0.*template0}s ], $EMPTY, 'psql -c -c');
psql('', 0, '', [ qr{(?!1234)}, qr{5678}s ], $EMPTY, 'psql -c ;;', ('-c', 'SELECT 1234; SELECT 5678;'));
psql('-E', 0, "\\l\n", [ qr{\* QUERY \*}, qr{template0} ], $EMPTY, 'psql -E');
psql('-A -F _', 0, "SELECT 54, 32;\n", [ qr{54_32} ], $EMPTY, 'psql -F _');
psql('-H', 0, "SELECT 5432 AS pg\n", [ qr{>5432<} ], $EMPTY, 'psql -H');
psql('-l', 0, '', [ qr{template0}, qr{template1} ], $EMPTY, 'psql -l');
psql('-n', 0, "\\q\n", $EMPTY, $EMPTY, 'psql -n');
psql('-P format=html', 0, "SELECT 5432 AS pg\n", [ qr{>5432<} ], $EMPTY, 'psql -P format=html');
psql('-P tuples_only', 0, "\\pset\n", [ qr{tuples_only.*on} ], $EMPTY, 'psql -P tuples_only');
psql('-A -R_', 0, "VALUES (1), (2);\n", [ qr{_1_2_} ], $EMPTY, 'psql -R _');
psql('-s', 0, "SELECT 1;\n", [ qr{verify command}, qr{SELECT 1}, qr{press return to proceed} ], $EMPTY, 'psql -s');
psql('-S', 0, "SELECT 1 AS one\nSELECT 2 AS two\n", [ qr{one.*1.*two.*2}s ], $EMPTY, 'psql -S');
psql('-H -T width=100', 0, "SELECT 1 AS one;\n", [ qr{table.*width=100} ], $EMPTY, 'psql -T ...');
psql('-X -V', 0, '', [ qr{^psql \(PostgreSQL\) \d+} ], $EMPTY, 'psql -X -V');
psql('', 0, "\\echo :VERSION_NUM\n", [ qr{\d{6}} ], $EMPTY, 'psql -- show VERSION_NUM');
psql('-v VERSION_NUM', 0, "\\echo :VERSION_NUM\n", [ qr{:VERSION_NUM} ], $EMPTY, 'psql -v ...');
psql('-v VERSION_NUM=bla', 0, "\\echo :VERSION_NUM\n", [ qr{bla} ], $EMPTY, 'psql -v ...=.');
psql('-w', 0, '', $EMPTY, $EMPTY, 'psql -w');
psql('-x', 0, "SELECT 1 AS one, 2 AS two;\n", [ qr{one \| 1.*two \| 2}s ], $EMPTY, 'psql -x');
psql('-1', 0, "SELECT 54;\nSELECT 32;\n", [ qr{54}, qr{32} ], $EMPTY, 'psql -1');
psql('-X -?', 0, '',
	 [ qr{interactive terminal}, qr{Usage:}, qr{Connection options:} ], $EMPTY, 'psql -X -?');
psql('--csv', 0, "SELECT 1 AS one, 2 AS two\n", [ qr{one,two}, qr{1,2} ], $EMPTY, 'psql --csv');

#
# create some objects to help \d tests later
#
psql('', 0, '-- create some objects
CREATE USER regress_psql_tap_1;
ALTER ROLE regress_psql_tap_1 SET enable_indexscan TO off;
CREATE TABLE regress_psql_tap_1_t1(data TEXT NOT NULL);
CREATE FUNCTION regress_psql_tap_1_f1() RETURNS INTEGER AS $$ SELECT 54321 $$ LANGUAGE SQL;
CREATE VIEW regress_psql_tap_1_v1 AS SELECT 54321 AS one;
', [ qr{CREATE ROLE}, qr{ALTER ROLE} ], [ qr{^$} ], 'psql -- create stuff');

#
# check interactive help
#
# this is mostly for coverage, including occasional negatives.
# we certainly do not want to have an output file with the whole doc.
my @backslash_help = (
	# \h command, list of stdout matches
	[ 'no-help', [ qr{No help available for} ] ],
	[ '' , [ qr{ABORT}, qr{ALTER}, qr{CREATE}, qr{DROP}, qr{VALUES}, qr{WITH} ] ],
	[ ' ' , [ qr{ABORT}, qr{ALTER}, qr{CREATE}, qr{DROP}, qr{VALUES}, qr{WITH} ] ],
	# lists
	[ 'ALTER', [ qr{AGGREGATE}, qr{ROLE}, qr{VIEW}, qr{(?!ACCESS METHOD)}, qr{(?!TRANSFORM)} ] ],
	[ 'ALTER TEXT SEARCH', [ qr{CONFIGURATION}, qr{DICTIONARY}, qr{PARSER}, qr{TEMPLATE} ] ],
	[ 'ALTER FOREIGN', [ qr{DATA WRAPPER}, qr{TABLE} ] ],
	[ 'CREATE', [ qr{ACCESS METHOD}, qr{TRANSFORM} ] ],
	[ 'DROP', [ qr{ACCESS METHOD}, qr{VIEW} ] ],
	# details
	[ 'ABORT', [ qr{TRANSACTION}, qr{CHAIN} ] ],
	[ 'ALTER AGGREGATE', [ qr{RENAME TO}, qr{(?!CONVERSION)} ] ],
	[ 'ALTER CONVERSION', [ qr{SET SCHEMA}, qr{(?!AGGREGATE)} ] ],
	[ 'ALTER COLLATION', [ qr{REFRESH VERSION} ] ],
	[ 'ALTER DATABASE', [ qr{CONNECTION LIMIT} ] ],
	[ 'ALTER DEFAULT PRIVILEGES', [ qr{GRANT} ] ],
	[ 'ALTER DOMAIN', [ qr{VALIDATE CONSTRAINT} ] ],
	[ 'ALTER EVENT TRIGGER', [ qr{DISABLE} ] ],
	[ 'ALTER EXTENSION', [ qr{OPERATOR FAMILY} ] ],
	[ 'ALTER FOREIGN DATA WRAPPER', [ qr{VALIDATOR} ] ],
	[ 'ALTER FOREIGN TABLE', [ qr{SET STORAGE} ] ],
	[ 'ALTER FUNCTION', [ qr{PARALLEL} ] ],
	[ 'ALTER GROUP', [ qr{DROP USER} ] ],
	[ 'ALTER INDEX', [ qr{ALL IN TABLESPACE} ] ],
	[ 'ALTER LANGUAGE', [ qr{PROCEDURAL}, qr{(?!INDEX)} ] ],
	[ 'ALTER LARGE OBJECT', [ qr{OWNER TO} ] ],
	[ 'ALTER MATERIALIZED VIEW', [ qr{SET TABLESPACE} ] ],
	[ 'ALTER OPERATOR', [ qr{JOIN =} ] ],
	[ 'ALTER OPERATOR CLASS', [ qr{SET SCHEMA}, qr{(?!LANGUAGE)} ] ],
	[ 'ALTER OPERATOR FAMILY', [ qr{FUNCTION} ] ],
	[ 'ALTER POLICY', [ qr{WITH CHECK} ] ],
	[ 'ALTER PROCEDURE', [ qr{DEPENDS ON EXTENSION} ] ],
	[ 'ALTER PUBLICATION', [ qr{DROP TABLE} ] ],
	[ 'ALTER ROLE', [ qr{BYPASSRLS} ] ],
	[ 'ALTER ROUTINE', [ qr{LEAKPROOF} ] ],
	[ 'ALTER RULE', [ qr{RENAME TO}, qr{(?!OWNER TO)} ] ],
	[ 'ALTER SCHEMA', [ qr{OWNER TO} ] ],
	[ 'ALTER SEQUENCE', [ qr{NO MAXVALUE} ] ],
	[ 'ALTER SERVER', [ qr{VERSION} ] ],
	[ 'ALTER STATISTICS', [ qr{SET SCHEMA} ] ],
	[ 'ALTER SUBSCRIPTION', [ qr{REFRESH PUBLICATION} ] ],
	[ 'ALTER SYSTEM', [ qr{RESET ALL} ] ],
	[ 'ALTER TABLE', [ qr{ALL IN TABLESPACE}, qr{DISABLE ROW LEVEL SECURITY} ] ],
	[ 'ALTER TABLESPACE', [ qr{RESET} ] ],
	[ 'ALTER TEXT SEARCH CONFIGURATION', [ qr{ALTER MAPPING FOR} ] ],
	[ 'ALTER TEXT SEARCH DICTIONARY', [ qr{OWNER TO} ] ],
	[ 'ALTER TEXT SEARCH PARSER', [ qr{RENAME TO}, qr{(?!OWNER TO)} ] ],
	[ 'ALTER TEXT SEARCH TEMPLATE', [ qr{RENAME TO}, qr{(?!OWNER TO)} ] ],
	[ 'ALTER TRIGGER', [ qr{DEPENDS ON EXTENSION} ] ],
	[ 'ALTER TYPE', [ qr{RENAME ATTRIBUTE} ] ],
	[ 'ALTER USER', [ qr{NOREPLICATION}, qr{(?!MAPPING)} ] ],
	[ 'ALTER USER MAPPING', [ qr{SERVER} ] ],
	[ 'ALTER VIEW', [ qr{DROP DEFAULT} ] ],
	[ 'ANALYZE', [ qr{VERBOSE}, qr{SKIP_LOCKED} ] ],
	[ 'BEGIN', [ qr{ISOLATION LEVEL} ] ],
	[ 'CALL', [] ],
	[ 'CHECKPOINT', [] ],
	[ 'CLOSE', [ qr{ALL} ] ],
	[ 'CLUSTER', [ qr{VERBOSE} ] ],
	[ 'COMMENT', [ qr{FOREIGN DATA WRAPPER} ] ],
	[ 'COMMIT', [ qr{TRANSACTION}, qr{CHAIN} ] ],
	[ 'COMMIT PREPARED', [] ],
	[ 'COPY', [ qr{FORMAT}, qr{FORCE_NOT_NULL} ] ],
	[ 'CREATE ACCESS METHOD', [ qr{HANDLER} ] ],
	[ 'CREATE AGGREGATE', [ qr{SSPACE}, qr{SORTOP}, qr{FINALFUNC} ] ],
	[ 'CREATE CAST', [ qr{AS IMPLICIT} ] ],
	[ 'CREATE COLLATION', [ qr{LC_CTYPE}, qr{DETERMINISTIC} ] ],
	[ 'CREATE CONVERSION', [ qr{FOR} ] ],
	[ 'CREATE DATABASE', [ qr{ALLOW_CONNECTIONS} ] ],
	[ 'CREATE DOMAIN', [ qr{COLLATE} ] ],
	[ 'CREATE EVENT TRIGGER', [ qr{EXECUTE} ] ],
	[ 'CREATE EXTENSION', [ qr{FROM} ] ],
	[ 'CREATE FOREIGN DATA WRAPPER', [ qr{NO VALIDATOR} ] ],
	[ 'CREATE FOREIGN TABLE', [ qr{SERVER} ] ],
	[ 'CREATE FUNCTION', [ qr{CALLED ON NULL INPUT}, qr{(?!PROCEDURE)} ] ],
	[ 'CREATE GROUP', [ qr{ADMIN} ] ],
	[ 'CREATE INDEX', [ qr{CONCURRENTLY}, qr{WHERE} ] ],
	[ 'CREATE LANGUAGE', [ qr{TRUSTED} ] ],
	[ 'CREATE MATERIALIZED VIEW', [ qr{DATA} ] ],
	[ 'CREATE OPERATOR', [ qr{LEFTARG} ] ],
	[ 'CREATE OPERATOR CLASS', [ qr{FOR ORDER BY} ] ],
	[ 'CREATE OPERATOR FAMILY', [ qr{USING} ] ],
	[ 'CREATE POLICY', [ qr{PERMISSIVE} ] ],
	[ 'CREATE PROCEDURE', [ qr{SECURITY INVOKER}, qr{(?!FUNCTION)} ] ],
	[ 'CREATE PUBLICATION', [ qr{FOR ALL TABLES} ] ],
	[ 'CREATE ROLE', [ qr{SYSID} ] ],
	[ 'CREATE RULE', [ qr{ALSO} ] ],
	[ 'CREATE SCHEMA', [ qr{AUTHORIZATION} ] ],
	[ 'CREATE SEQUENCE', [ qr{NO MINVALUE} ] ],
	[ 'CREATE SERVER', [ qr{FOREIGN DATA WRAPPER} ] ],
	[ 'CREATE STATISTICS', [ qr{FROM} ] ],
	[ 'CREATE SUBSCRIPTION', [ qr{CONNECTION} ] ],
	[ 'CREATE TABLE', [ qr{PARTITION BY} ] ],
	[ 'CREATE TABLE AS', [ qr{ON COMMIT} ] ],
	[ 'CREATE TABLESPACE', [ qr{LOCATION} ] ],
	[ 'CREATE TEXT SEARCH CONFIGURATION', [ qr{PARSER} ] ],
	[ 'CREATE TEXT SEARCH DICTIONARY', [ qr{TEMPLATE} ] ],
	[ 'CREATE TEXT SEARCH PARSER', [ qr{GETTOKEN} ] ],
	[ 'CREATE TEXT SEARCH TEMPLATE', [ qr{LEXIZE} ] ],
	[ 'CREATE TRANSFORM', [ qr{FROM SQL WITH FUNCTION} ] ],
	[ 'CREATE TRIGGER', [ qr{EXECUTE} ] ],
	[ 'CREATE TYPE', [ qr{SUBTYPE} ] ],
	[ 'CREATE USER', [ qr{PASSWORD NULL} ] ],
	[ 'CREATE USER MAPPING', [ qr{SERVER} ] ],
	[ 'CREATE VIEW', [ qr{CASCADED} ] ],
	[ 'DEALLOCATE', [ qr{PREPARE} ] ],
	[ 'DECLARE', [ qr{INSENSITIVE} ] ],
	[ 'DELETE', [ qr{WHERE CURRENT OF} ] ],
	[ 'DISCARD', [ qr{PLANS} ] ],
	[ 'DO', [ qr{LANGUAGE} ] ],
	[ 'DROP ACCESS METHOD', [ qr{CASCADE} ] ],
	[ 'DROP AGGREGATE', [ qr{ORDER BY} ] ],
	[ 'DROP CAST', [ qr{RESTRICT} ] ],
	[ 'DROP COLLATION', [ qr{RESTRICT} ] ],
	[ 'DROP CONVERSION', [ qr{RESTRICT} ] ],
	[ 'DROP DATABASE', [ qr{IF EXISTS} ] ],
	[ 'DROP DOMAIN', [ qr{RESTRICT} ] ],
	[ 'DROP EVENT TRIGGER', [ qr{RESTRICT} ] ],
	[ 'DROP EXTENSION', [ qr{RESTRICT} ] ],
	[ 'DROP FOREIGN DATA WRAPPER', [ qr{RESTRICT} ] ],
	[ 'DROP FOREIGN TABLE', [ qr{RESTRICT} ] ],
	[ 'DROP FUNCTION', [ qr{RESTRICT} ] ],
	[ 'DROP GROUP', [ qr{IF EXISTS} ] ],
	[ 'DROP INDEX', [ qr{RESTRICT} ] ],
	[ 'DROP LANGUAGE', [ qr{RESTRICT} ] ],
	[ 'DROP MATERIALIZED VIEW', [ qr{RESTRICT} ] ],
	[ 'DROP OPERATOR', [ qr{RESTRICT} ] ],
	[ 'DROP OPERATOR CLASS', [ qr{RESTRICT} ] ],
	[ 'DROP OPERATOR FAMILY', [ qr{RESTRICT} ] ],
	[ 'DROP OWNED', [ qr{CURRENT_USER} ] ],
	[ 'DROP POLICY', [ qr{RESTRICT} ] ],
	[ 'DROP PROCEDURE', [ qr{RESTRICT} ] ],
	[ 'DROP PUBLICATION', [ qr{RESTRICT} ] ],
	[ 'DROP ROLE', [ qr{IF EXISTS} ] ],
	[ 'DROP ROUTINE', [ qr{RESTRICT} ] ],
	[ 'DROP RULE', [ qr{RESTRICT} ] ],
	[ 'DROP SCHEMA', [ qr{RESTRICT} ] ],
	[ 'DROP SEQUENCE', [ qr{RESTRICT} ] ],
	[ 'DROP SERVER', [ qr{RESTRICT} ] ],
	[ 'DROP STATISTICS', [ qr{IF EXISTS} ] ],
	[ 'DROP SUBSCRIPTION', [ qr{RESTRICT} ] ],
	[ 'DROP TABLE', [ qr{RESTRICT} ] ],
	[ 'DROP TABLESPACE', [ qr{IF EXISTS} ] ],
	[ 'DROP TEXT SEARCH CONFIGURATION', [ qr{RESTRICT} ] ],
	[ 'DROP TEXT SEARCH DICTIONARY', [ qr{RESTRICT} ] ],
	[ 'DROP TEXT SEARCH PARSER', [ qr{RESTRICT} ] ],
	[ 'DROP TEXT SEARCH TEMPLATE', [ qr{RESTRICT} ] ],
	[ 'DROP TRANSFORM', [ qr{LANGUAGE} ] ],
	[ 'DROP TRIGGER', [ qr{RESTRICT} ] ],
	[ 'DROP TYPE', [ qr{RESTRICT} ] ],
	[ 'DROP USER', [ qr{IF EXISTS} ] ],
	[ 'DROP USER MAPPING', [ qr{SERVER} ] ],
	[ 'DROP VIEW', [ qr{RESTRICT} ] ],
	[ 'END', [ qr{TRANSACTION}, qr{CHAIN}, qr{(?!COMMIT)} ] ],
	[ 'EXECUTE', [ ] ],
	[ 'EXPLAIN', [ qr{VERBOSE} ] ],
	[ 'FETCH', [ qr{RELATIVE} ] ],
	[ 'GRANT', [ qr{ALL TABLES} ] ],
	[ 'IMPORT FOREIGN SCHEMA', [ qr{FROM SERVER} ] ],
	[ 'INSERT', [ qr{ON CONFLICT} ] ],
	[ 'LISTEN', [ ] ],
	[ 'LOAD', [ ] ],
	[ 'LOCK', [ qr{NOWAIT} ] ],
	[ 'MOVE', [ qr{FORWARD} ] ],
	[ 'NOTIFY', [ ] ],
	[ 'PREPARE', [ qr{AS} ] ],
	[ 'PREPARE TRANSACTION', [ ] ],
	[ 'REASSIGN OWNED', [ qr{SESSION_USER} ] ],
	[ 'REFRESH MATERIALIZED VIEW', [ qr{CONCURRENTLY} ] ],
	[ 'REINDEX', [ qr{DATABASE} ] ],
	[ 'RELEASE', [ qr{SAVEPOINT} ] ],
	[ 'RESET', [ qr{RESET ALL} ] ],
	[ 'REVOKE', [ qr{PRIVILEGES} ] ],
	[ 'ROLLBACK', [ qr{CHAIN} ] ],
	[ 'ROLLBACK PREPARED', [ ]],
	[ 'SAVEPOINT', [ ] ],
	[ 'SECURITY LABEL', [ qr{SUBSCRIPTION} ] ],
	[ 'SELECT', [ qr{RECURSIVE} ] ],
	[ 'SET', [ qr{SESSION} ] ],
	[ 'SET CONSTRAINTS', [ qr{IMMEDIATE} ] ],
	[ 'SET ROLE', [ qr{ROLE NONE} ] ],
	[ 'SET SESSION AUTHORIZATION', [ qr{DEFAULT} ] ],
	[ 'SET TRANSACTION', [ qr{SNAPSHOT} ] ],
	[ 'SHOW', [ qr{SHOW ALL} ] ],
	[ 'START TRANSACTION', [ qr{ISOLATION LEVEL}, qr{(?!BEGIN)} ] ],
	[ 'TABLE', [ qr{ONLY} ] ], # hmmm...
	[ 'TRUNCATE', [ qr{CONTINUE IDENTITY} ] ],
	[ 'UNLISTEN', [ ] ],
	[ 'UPDATE', [ qr{RETURNING} ] ],
	[ 'VACUUM', [ qr{FREEZE} ] ],
	[ 'VALUES', [ qr{ORDER BY} ] ],
	[ 'WITH', [ qr{RECURSIVE} ] ], # SELECT duplicate?
);

for my $h (@backslash_help)
{
	my ($cmd, $out) = @$h;
	push @$out, qr{$cmd};
	psql('', 0, "\\h $cmd\n", $out, [ qr{^$} ], "psql -- \\h $cmd");
}

# special cases
psql('', 0, "\\h ROLLBACK TO SAVEPOINT\n", [ qr{ROLLBACK}, qr{SAVEPOINT} ],
	 [ qr{^$} ], "psql -- \\h ROLLBACK TO SAVEPOINT");
psql('', 0, "\\h SELECT INTO\n", [ qr{SELECT}, qr{INTO} ],
	 [ qr{^$} ], "psql -- \\h SELECT INTO");

#
# check describe and other backslash commands
#
# the output can vary significantly, especially with +
my @backslash_out = (
	# empty
	[ "\\d\n", [ qr{List of relations}, qr{regress_psql_tap_1_t1}, qr{(?!pg_locks)} ] ],
	[ "\\dS\n", [ qr{List of relations}, qr{pg_locks} ] ],
	# aggregates
	[ "\\da\n", [ qr{List of aggregate functions}, qr{(?!Description)} ] ],
	[ "\\da+\n", [ qr{List of aggregate functions}, qr{(Description)} ] ],
	[ "\\daS\n", [ qr{array_agg}, qr{avg}, qr{bit_and}, qr{bit_or}, qr{count}, qr{max} ] ],
	# access methods
	[ "\\dA\n", [ qr{List of access methods}, qr{btree}, qr{hash}, qr{(?!Description)} ] ],
	[ "\\dA+\n", [ qr{List of access methods}, qr{btree}, qr{hash}, qr{Description} ] ],
	# tablespaces
	[ "\\db\n", [ qr{List of tablespaces}, qr{pg_default}, qr{pg_global}, qr{(?!Size)} ] ],
	[ "\\db+ pg_def*\n", [ qr{List of tablespaces}, qr{pg_default}, qr{(?!pg_global)}, qr{Size} ] ],
	# functions
	[ "\\df\n", [ qr{List of functions} ] ],
	[ "\\df+\n", [ qr{List of functions} ] ],
	[ "\\dftS\n", [ qr{RI_}, qr{Type}, qr{(?!Volatility)}, qr{(?!Parallel)} ] ],
	[ "\\dftS+\n", [ qr{Volatility}, qr{Parallel} ] ],
	[ "\\dfwS\n", [ qr{lag}, qr{dense_rank}, qr{(?!Owner)}, qr{(?!Language)} ] ],
	[ "\\dfwS+\n", [ qr{cume_dist}, qr{Owner}, qr{Language} ] ],
	[ "\\dfnS\n", [ qr{abbrev}, qr{(?!bit_or)} ] ],
	[ "\\dfaS\n", [ qr{bit_and}, qr{(?!abs)} ] ],
	[ "\\dfpS\n", [ qr{List of functions} ] ],
	[ "\\dfwtS\n", [ qr{RI_FKey_}, qr{last_value}, qr{nth_value}, qr{(?!bit_and)} ] ],
	# type
	[ "\\dTS charac*\n", [ qr{List of data types}, qr{character varying}, qr{(?!double)}, qr{(?!Size)} ] ],
	[ "\\dTS+\n", [ qr{aclitem}, qr{Size} ] ],
	# operator
	[ "\\doS\n", [ qr{List of operators}, qr{!~\*} ] ],
	[ "\\doS+\n", [ qr{<->}, qr{Result type}] ],
	# databases
	[ "\\l\n", [ qr{List of databases}, qr{template0}, qr{template1}, qr{(?!Size)} ] ],
	[ "\\l a*\n", [ qr{(?!template[01])} ] ],
	[ "\\l+\n", [ qr{Size}, qr{Encoding} ] ],
	[ "\\list\n", [ qr{List of databases} ] ],
	# permissions: well tested
	# default acls (empty)
	[ "\\ddp\n", [ qr{Default access privileges} ] ],
	# descriptions (empty)
	[ "\\dd\n", [ qr{Object descriptions} ] ],
	# tables: well tested
	# roles/users/groups
	[ "\\du\n", [ qr{Superuser}, qr{regress_psql_tap_1}, qr{List of roles}, qr{(?!Description)} ] ],
	[ "\\du+\n", [ qr{Superuser}, qr{List of roles}, qr{Description} ] ],
	[ "\\dg\n", [ qr{Superuser}, qr{List of roles} ] ],
	# role settings
	[ "\\drds\n", [ qr{List of settings} ] ],
	[ "\\drds regress_* *\n", [ qr{List of settings} ] ],
	# index/...
	[ "\\diS\n", [ qr{List of relations}, qr{pg_am_oid_index} ] ],
	[ "\\diS+\n", [ qr{Table} ] ],
	# partition tables: well tested
	# large objects
	[ "\\dl", [ qr{Large objects} ] ],
	# languages
	[ "\\dL\n", [ qr{List of languages}, qr{plpgsql}, qr{(?!Trusted)} ] ],
	[ "\\dL+\n", [ qr{plpgsql}, qr{Trusted} ] ],
	# domains
	[ "\\dD\n", [ qr{List of domains}, qr{(?!Access privileges)} ] ],
	[ "\\dD+\n", [ qr{List of domains}, qr{Access privileges} ] ],
	# conversions
	[ "\\dcS\n", [ qr{List of conversions}, qr{UTF8}, qr{(?!Description)} ] ],
	[ "\\dcS+\n", [ qr{List of conversions}, qr{LATIN1}, qr{Description} ] ],
	# event triggers (empty)
	[ "\\dy\n", [ qr{List of event triggers}, qr{(?!Description)} ] ],
	[ "\\dy+\n", [ qr{List of event triggers}, qr{Description} ] ],
	# casts
	[ "\\dC\n", [ qr{List of casts}, qr{timestamp without time zone}, qr{(?!Description)} ] ],
	[ "\\dC+\n", [ qr{Function}, qr{Implicit}, qr{Description} ] ],
	# collations
	[ "\\dOS\n", [ qr{List of collations}, qr{Deterministic}, qr{(?!Description)} ] ],
	[ "\\dOS+\n", [ qr{POSIX}, qr{Description} ] ],
	# schemas
	[ "\\dn\n", [ qr{List of schemas}, qr{Owner}, qr{(?!Description)} ] ],
	[ "\\dn+\n", [ qr{Access privileges}, qr{Description} ] ],
	# text search misc.
	[ "\\dFp\n", [ qr{List of text search parsers}, qr{default} ] ],
	[ "\\dFp+\n", [ qr{hword}, qr{numword} ] ],
	[ "\\dFd\n", [ qr{List of text search dictionaries}, qr{simple} ] ],
	[ "\\dFd+\n", [ qr{Init options}, qr{Description} ] ],
	[ "\\dFt\n", [ qr{List of text search templates}, qr{simple} ] ],
	[ "\\dFt+\n", [ qr{Lexize}, qr{Description} ] ],
	[ "\\dF\n", [ qr{List of text search configurations}, qr{simple} ] ],
	[ "\\dF+\n", [ qr{email}, qr{url_path} ] ],
	# extensions
	[ "\\dx\n", [ qr{List of installed extensions}, qr{Version}, qr{plpgsql} ] ],
	[ "\\dx+\n", [ qr{Objects in extension}, qr{plpgsql_call_handler} ] ],
	# other backslash commands
	[ "\\f\n", [ qr{Field separator is} ] ],
	[ "\\H\\H\n", [ qr{Output format is html.* is aligned}s ] ],
	[ "\\\?\n", [ qr{General}, qr{Help}, qr{Variables}, qr{Large Objects} ] ],
	[ "\\C foobar\n", [ qr{Title is "foobar"} ] ],
	[ "\\cd /\n\\cd\n", [ qr{^$} ] ],
	[ "\\conninfo\n", [ qr{You are connected to database} ] ],
	[ "\\copy (SELECT 5432 UNION SELECT 2345 ORDER BY 1) TO STDOUT\n", [ qr/\b2345\b.*\b5432\b/s ] ],
	[ "\\copyright\n", [ qr/The Regents of the University of California/ ] ],
	[ "\\encoding\n", [ qr/./ ] ],
	[ "\\encoding :ENCODING\n", $EMPTY ],
	[ "\\errverbose\n", [ qr{There is no previous error} ] ],
	[ "\\lo_list\n", [ qr{Large objects} ] ],
	[ "\\if true\\q\\endif\n", $EMPTY ],
	[ "\\set\n", [ qr{ENCODING = }, qr{VERSION_NUM = } ] ],
	[ "\\set COMP_KEYWORD_CASE preserve-lower\n\\set COMP_KEYWORD_CASE lower\n" .
	  "\\set COMP_KEYWORD_CASE upper\n\\echo :COMP_KEYWORD_CASE", [ qr/upper/ ] ],
	[ "\\set HISTCONTROL ignorespace\n\\set HISTCONTROL ignoredups\n" .
	  "\\set HISTCONTROL ignoreboth\n\\echo :HISTCONTROL", [ qr/ignoreboth/ ] ],
	[ "\\set ECHO_HIDDEN on\n\\l\n", [ qr/\* QUERY \*/, qr/pg_catalog\.pg_database/, qr/template0/ ] ],
	[ "\\set ECHO_HIDDEN noexec\n\\l\n", [ qr/\* QUERY \*/, qr/pg_catalog\.pg_database/, qr/(?!template0)/ ] ],
	[ "\\set LAST_ERROR_MESSAGE foo bla\n\\echo :LAST_ERROR_MESSAGE\n", [ qr{foobla} ] ],
	[ "\\set AUTOCOMMIT off\n\\set ON_ERROR_ROLLBACK on\nSELECT 5432 AS pg;\nSELECT 2345 AS pg;\n",
		[ qr{5432}, qr{2345} ] ],
	[ "\\set ON_ERROR_ROLLBACK interactive\n\\echo :ON_ERROR_ROLLBACK\n", [ qr{interactive} ] ],
	[ "\\set VERBOSITY verbose\n\\echo :VERBOSITY\n", [ qr{verbose} ] ],
	[ "\\set foo bla\n\\echo :foo\\unset foo\\echo :foo\n", [ qr{^bla.*:foo}s ] ],
	[ "\\set foo bla\n\\echo :'foo' :\"foo\"\n", [ qr{'bla'}, qr{"bla"} ] ],
	[ "\\setenv FOO bla\n\\setenv FOO\n", [ qr{^$} ] ],
	[ "\\timing\nSELECT 1;\n\\timing\n", [ qr{Timing is on.*Timing is off}s, qr(Time: \d+\.\d{3} ms) ] ],
	[ "\\timing ON\n\\timing OFF\n", [ qr{Timing is on.*Timing is off}s ] ],
	[ "\\T foo\n", [ qr{Table attributes are "foo"} ] ],
	[ "\\T\n", [ qr{Table attributes unset} ] ],
	# help variants
	[ "\\?\n",
		[ qr{General}, qr{Help}, qr{Query}, qr{Input}, qr{Conditional},
		  qr{Informational}, qr{Formatting}, qr{Connection},
		  qr{Operating System}, qr{Variables}, qr{Large Objects} ] ],
	[ "\\? commands\n",
		[ qr{General}, qr{Help}, qr{Query}, qr{Input}, qr{Conditional},
		  qr{Informational}, qr{Formatting}, qr{Connection},
		  qr{Operating System}, qr{Variables}, qr{Large Objects} ] ],
	[ "\\? bad\n", [ qr{General} ] ],
	[ "\\? variables\n",
		  [ qr{psql variables:}, qr{Display settings:}, qr{Environment variables} ] ],
	[ "\\? options\n",
		  [ qr{interactive terminal}, qr{Report bugs} ] ],
	# pset format
	[ "\\pset format aligned\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is aligned}, qr{   1 \|   2} ] ],
	[ "\\pset format asciidoc\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is asciidoc}, qr{\|1 \|2} ] ],
	[ "\\pset format csv\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is csv}, qr{1,2} ] ],
	[ "\\pset format html\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is html}, qr{<td align="right">1</td>} ] ],
	[ "\\pset format latex\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is latex}, qr{1 \& 2} ] ],
	[ "\\pset format latex-longtable\n\\pset format\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is latex-longtable}, qr{\\raggedright\{1\}} ] ],
	[ "\\pset format troff-ms\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is troff-ms}, qr{1\t2} ] ],
	[ "\\pset format unaligned\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is unaligned}, qr{1\|2} ] ],
	[ "\\pset format wrapped\nSELECT 1 AS one, 2 AS two;\n",
		[ qr{Output format is wrapped}, qr{   1 \|   2} ] ],
	[ "\\pset format\n", [ qr{Output format is aligned} ] ],
	# pset linestyle
	[ "\\pset linestyle ascii\n", [ qr{Line style is ascii} ] ],
	[ "\\pset linestyle old-ascii\n", [ qr{Line style is old-ascii} ] ],
	[ "\\pset linestyle unicode\n", [ qr{Line style is unicode} ] ],
	# pset unicode_*_linestyle
	[ "\\pset unicode_header_linestyle single\n", [ qr{"single"} ] ],
	[ "\\pset unicode_header_linestyle double\n", [ qr{"double"} ] ],
	[ "\\pset unicode_border_linestyle single\n", [ qr{"single"} ] ],
	[ "\\pset unicode_border_linestyle double\n", [ qr{"double"} ] ],
	[ "\\pset unicode_column_linestyle single\n", [ qr{"single"} ] ],
	[ "\\pset unicode_column_linestyle double\n", [ qr{"double"} ] ],
);

for my $h (@backslash_out)
{
	my ($in, $out) = @$h;
	psql('', 0, $in, $out, [ qr{^$} ], "psql -- $in");
}

# test some errors
my @backslash_err = (
	[ "\\unknown stuff\n", [ qr/invalid command / ] ],
	[ "\\cd /no/such/dir\n", [ qr/could not change directory to/ ] ],
	[ "\\copy\n", [ qr/arguments required/ ] ],
	[ "\\copy bad stuff on the line\n", [ qr/parse error at \"/ ] ],
	[ "\\copy binary (\n", [ qr/parse error at end of line/ ] ],
	[ "\\copy public.regress_psql_tap_1_t1(data) to '/no/such/file'", [ qr{/no/such/file} ] ],
	[ "\\encoding no-such-encoding\n", [ qr/invalid encoding name/ ] ],
	[ "BAD;\n\\errverbose\n", [ qr/syntax error at or near "BAD"/, qr/\b42601\b/ ] ],
	[ "\\dfX\n", [ qr/invalid command/ ] ],
	[ "\\dPX\n", [ qr/invalid command/ ] ],
	[ "\\drX\n", [ qr/invalid command/ ] ],
	[ "\\dRX\n", [ qr/invalid command/ ] ],
	[ "\\dFX\n", [ qr/invalid command/ ] ],
	[ "\\deX\n", [ qr/invalid command/ ] ],
	[ "\\dX\n", [ qr/invalid command/ ] ],
	[ "\\i\n", [ qr/missing required argument/ ] ],
	[ "\\ir\n", [ qr/missing required argument/] ],
	[ "\\lo_bad\n", [ qr/invalid command/ ] ],
	[ "\\lo_export\n", [ qr/missing required argument/ ] ],
	[ "\\lo_import\n", [ qr/missing required argument/ ] ],
	[ "\\lo_import /no/such/file\n", [ qr{no/such/file} ] ],
	[ "\\lo_unlink\n", [ qr/missing required argument/ ] ],
	[ "\\lo_unlink -1\n", [ qr/large object 4294967295 does not exist/ ] ],
	[ "\\setenv\n", [ qr/missing required argument/ ] ],
	[ "\\setenv PG_REGRESS_TEST=1\n", [ qr/environment variable name must not contain "="/ ] ],
	[ "\\unset\n", [ qr/missing required argument/ ] ],
	[ "\\set foo \`no-such-command\`\n", [ qr/no-such-command/ ] ],
	[ "\\set === xxx\n", [ qr/invalid variable name/ ] ],
	[ "\\w\n", [ qr/missing required argument/ ] ],
	[ "\\pset format bad\n", [ qr/allowed formats are aligned, asciidoc/ ] ],
	[ "\\pset linestyle bad\n", [ qr/allowed line styles are ascii, old-ascii, unicode/ ] ],
	[ "\\pset unicode_border_linestyle bad\n", [ qr/allowed Unicode border line styles are single, double/ ] ],
	[ "\\pset unicode_column_linestyle bad\n", [ qr/allowed Unicode column line styles are single, double/ ] ],
	[ "\\pset unicode_header_linestyle bad\n", [ qr/allowed Unicode header line styles are single, double/ ] ],
	[ "COPY regress_psql_tap_1_t1 FROM STDIN \\watch 0.01\n", [ qr/watch cannot be used with COPY/ ] ],
	[ "\\watch 0.01\n", [ qr/watch cannot be used with an empty query/ ] ],
	# check variable constraints values
	[ "\\set ECHO bad\n", [ qr{none, errors, queries, all} ] ],
	[ "\\set ECHO_HIDDEN bad\n", [ qr{on, off, noexec} ] ],
	[ "\\set ON_ERROR_ROLLBACK bad\n", [ qr{on, off, interactive} ] ],
	[ "\\set COMP_KEYWORD_CASE bad\n", [ qr{lower, upper, preserve-lower, preserve-upper} ] ],
	[ "\\set HISTCONTROL bad\n", [ qr{none, ignorespace, ignoredups, ignoreboth} ] ],
	[ "\\set VERBOSITY bad\n", [ qr{default, verbose, terse, sqlstate} ] ],
	[ "\\set SHOW_CONTEXT bad\n", [ qr{never, errors, always} ] ],
);

for my $h (@backslash_err)
{
	my ($in, $err) = @$h;
	psql('-L /dev/null', 0, $in, [ qr{^$} ], $err, "psql -- $in");
}

# other errors

# \timing
psql('', 0, "\\timing bad\n",
	 [ qr{Timing is off} ], [ qr{unrecognized value} ], 'psql \timing error');

# check stdout vs stderr output
psql('', 0, "\\echo hello\n\\warn world\n\\q\n",
	 [ qr{^hello$} ], [ qr{^world$} ], 'psql in/out/err');

# \watch test
psql('', 0,
	 "CREATE TEMP SEQUENCE tmp_seq MAXVALUE 3 NO CYCLE;\n" .
	 "\\t on\n" .
	 "\\timing\n" .
	 "SELECT 'x=' || NEXTVAL('tmp_seq') \\watch 0.01\n",
	 [ qr/x=1\b.*x=2\b.*x=3\b/s, qr/(?!x=[04-9])/ ],
	 [ qr/nextval: reached maximum value of sequence/ ],
	 'psql watch');

# guess whether it looks like unix enough for testing purposes
my $is_like_unix = -e "/dev/null" and -d "/tmp" and -x "/bin/cat" and -x "/bin/echo";

# skip un*x-specific zone if it does not look like un*x
goto END_UNIX_ZONE
	unless $is_like_unix;

# this probably only works under UN*X-like systems
psql('', 0, "\\setenv PSQL_EDITOR echo\nSELECT 1 AS one;\n\\e\n\\p\n",
	 [ qr/one.*one/s, qr/SELECT 1/ ], $EMPTY, 'psql edit');
psql('', 0,
	 "\\setenv PSQL_EDITOR echo\n\\ef regress_psql_tap_1_f1\n",
	 [ qr/No changes/ ], $EMPTY, 'psql \ef');
psql('', 0,
	 "\\setenv PSQL_EDITOR echo\n\\ev regress_psql_tap_1_v1\n",
	 [ qr/No changes/ ], $EMPTY, 'psql \ev');
psql('', 0, "\\setenv FOO bla\\set foo \`echo \$FOO\`\n\\echo :foo\n",
	 [ qr/bla/ ], $EMPTY, 'psql backtick');
psql('', 0, "\\setenv FOO blabla\n\\! echo \$FOO\n",
	 [ qr/blabla/ ], $EMPTY, 'psql !');

psql('-o /dev/null', 0, "SELECT 5432 AS pg\n", $EMPTY, $EMPTY, 'psql -o null');
psql('', 0, "\\o /dev/null\nSELECT 1;\n", $EMPTY, $EMPTY, 'psql \o null');
psql('', 0, "\\o | cat\nSELECT 5432 AS pg;\n\\o\n",
	 [ qr/\b5432\b/ ], $EMPTY, 'psql \o cat');

psql('', 0, "SELECT 5432 AS pg;\n\\w /dev/null\n",
	 [ qr/pg.*\b5432\b/s ], $EMPTY, 'psql \w null');

psql('', 0, "SELECT 1\\g /dev/null\n",
	 $EMPTY, $EMPTY, 'psql \g null');
psql('', 0, "SELECT 5432\\g | cat\n",
	 [ qr/\b5432\n/ ], $EMPTY, 'psql \g cat');

psql('--log-file=/dev/null', 0, "SELECT 5432 AS pg\n",
	 [ qr/\b5432\b/ ], $EMPTY, 'psql -L null');

psql('', 0, "\\copy public.regress_psql_tap_1_t1(data) FROM PROGRAM 'echo moe'\n",
	[ qr/COPY 1\b/ ], $EMPTY, 'psql copy echo');
psql('', 0, "\\copy public.regress_psql_tap_1_t1(data) TO PROGRAM 'cat'\n",
	[ qr/COPY 1\b/ ], $EMPTY, 'psql copy cat'); # :-)

END_UNIX_ZONE:

psql('', 0, "\\i /no/such/file\n",
	 [ qr{^$} ], [ qr{/no/such/file} ], 'psql \i error');
psql('', 0, "\\i ~/there/is/no/such/file\n",
	 [ qr{^$} ], [ qr{there/is/no/such/file} ], 'psql \i tilde');

# no such file
psql('', 0, "\\o /no/such/file\n",
	 [ qr{^$} ], [ qr{/no/such/file} ], 'psql \o error');

psql('', 0, "\\w /no/such/file\n",
	 [ qr{^$} ], [ qr{/no/such/file} ], 'psql \w error');

psql('-L /no/such/file', 1, '',
	 [ qr{^$} ], [ qr{could not open log file} ], 'psql -L bad');

# \copy errors
psql('', 0, "\\copy regress_psql_tap_1_t1 to program '/no/such/prgm'",
	 [ qr/COPY 1/ ], [ qr{/no/such/prgm} ], 'psql copy to bad prgm');
psql('', 0, "\\copy regress_psql_tap_1_t1 from program '/no/such/prgm'",
	 [ qr/COPY 0/ ], [ qr{/no/such/prgm} ], 'psql copy from bad prgm');
psql('', 0, "\\copy regress_psql_tap_1_t1 to '/tmp'",
	 $EMPTY, [ qr{directory} ], 'psql copy to tmp');
psql('', 0, "\\copy regress_psql_tap_1_t1 from '/tmp'",
	 $EMPTY, [ qr/directory/ ], 'psql copy to tmp');

# misc errors
psql('-P format=bad', 1, '', $EMPTY,
	 [ qr/allowed formats are aligned/, qr/could not set printing parameter/ ], 'psql -P bad');
psql('-Q', 1, '', $EMPTY, [ qr/Try ".* --help" for more information/ ], 'psql -BAD');

#
# .psqlrc
#
# note: reading the system wide psqlrc cannot be avoided
#
my $psqlrc = create_test_file('regress_psql_tap_1_psqlrc', "SELECT 1 AS one\n");
$ENV{PSQLRC} = $psqlrc;
psql('!', 0, "SELECT 2 AS two\n", [ qr{one.*1.*two.*2}s ], [ qr{^$} ], 'psql .psqlrc');
delete $ENV{PSQLRC};
ok(unlink $psqlrc, "unlink $psqlrc");

#
# large objects
#
my $lofile = create_test_file('regress_psql_tap_1_lo', '0123456789ABCDEF' x 1024);
my $esc_lofile = $lofile;
$esc_lofile =~ s/'/''/g;
psql('-H', 0,
	 "\\lo_import '$esc_lofile' 'regress psql TAP lo'\n\\lo_list\n",
	 [ qr{lo_import \d+}, qr{Large objects}, qr{regress psql TAP lo} ], [ qr{^$} ], 'psql lo 1');
ok(unlink $lofile, "unlink $lofile (1)");
psql('', 0,
	 "SELECT oid AS loboid\n" .
	 "FROM pg_catalog.pg_largeobject_metadata\n" .
	 "WHERE pg_catalog.obj_description(oid, 'pg_largeobject') = 'regress psql TAP lo'\\gset\n" .
	 "\\echo loboid = :loboid\n" .
	 "\\pset tuples_only on\n" .
	 "SELECT 'lo extract: ' || lo_get(:loboid, 826, 3)::TEXT AS extract;\n" .
	 "\\lo_export :loboid '/no/such/dir/stuff'\n" . # this one fails
	 "\\lo_export :loboid '$esc_lofile'\n" .
	 "\\lo_unlink :loboid\n",
	 [ qr{loboid = \d+}, qr{lo extract: \\x414243} ],
	 [ qr{could not open file "/no/such/dir/stuff"} ],
	 'psql lo 2');
ok(-e $lofile, "re-created $lofile");
ok(unlink $lofile, "unlink $lofile (2)");

#
# INTERACTIVE STUFF
#
# tab-completion checks are not very fast.
# disable pager to work around pagination interactions.

# basic test
ipsql('-P pager', 0, 5,
	  [
		[ "\\echo pg :VERSION_NUM\n", qr/pg \d+.*postgres=\# /s ],
		[ "\\echo hello     world\n", qr/hello world.*postgres=\# /s ],
		[ "help\n", qr/You are using.*\\copyright.*quit.*postgres=\# /s ],
		[ "SELECT\nhelp\n\\r\n", qr/help or press control-[CD].*postgres=\# /s ],
		[ "SELECT\nquit  ;  \n\\r\n", qr/Use \\q to quit.*buffer reset.*postgres=\# /s ],
		[ "SELECT (\nquit  ;  \n\\r\n", qr/Use \\q to quit.*buffer reset.*postgres=\# /s ],
		[ "SELECT \$\$\nquit\n\$\$\\r\n", qr/Use control-[CD] to quit.*buffer reset.*postgres=\# /s ],
		[ "SELECT '\nquit'\\r\n", qr/quit'.*postgres=\# /s ],
		[ "SELECT '\n\\q\n", qr/Use control-[CD] to quit.*postgres'\# /s ],
		[ "'\\r\n", qr/buffer reset.*postgres=\# /s ],
		[ "SELECT 1\n\\p\n\\r\n", qr/SELECT 1.*SELECT 1.*buffer reset.*postgres=\#/s ],
		#[ "\\prompt tmp_var\nfoobla\n\\echo hello :tmp_var\n\\unset foo\n\\echo world :tmp_var\n",
		#	qr/hello foobla.*world :tmp_var.*postgres=\# /s ],
		[ "\\password\n", qr/Enter new password: / ],
		[ "foo\n", qr/Enter it again: / ],
		[ "bla\n", qr/Passwords didn't match.*postgres=\# /s ],
		[ "exit\n" ],
	  ],
	  'ipsql basic', 1);

# prompt
ipsql('-P pager', 0, 5,
	  [ # default prompts
		[ "SELECT 1 +\n", qr/postgres-\# / ],
		[ "(2 * \n", qr/postgres\(\# / ],
		[ "3) + '\n", qr/postgres\'\# / ],
		[ "4' + \$\$\n", qr/postgres\$\# /],
		[ "5\$\$ AS \"\n", qr/postgres\"\# / ],
		[ "seize\" /*\n", qr/postgres\*\# / ],
		[ " ignored */;\n", qr/\bseize\b.*\b16\b.*postgres=\# /s ],
		[ "\\if false\n", qr/postgres\@\# / ],
		[ "\\echo IGNORED\n", qr/command ignored.*if block.*postgres\@\# /s ],
		# BUG: prompt with - instead of @
		[ "SELECT 1;\n", qr/query ignored.*if block.*postgres.\# /s ],
		[ "\\endif\n", qr/postgres=\# / ],
		[ "\\set SINGLELINE ON\n", qr/postgres\^\# / ],
		[ "\\set SINGLELINE OFF\n", qr/postgres=\# / ],
		[ "COPY regress_psql_tap_1_t1(data) FROM STDIN;\n", qr/>> / ],
		[ "hello\nworld\n\\.\n", qr/COPY 2\b.*postgres=\# /s ],
		# set new prompts (not tested: %`)
		[ "\\set PROMPT1 '%%%[%]%?%M%m%>%n%/%~%#%p%R%x%l%:QUIET:%040'\n", qr/%.*\d+(on|off) /s ],
		[ "\\set PROMPT1 '1> '\n\\set PROMPT2 '2> '\n\\set PROMPT3 '3> '\n", qr/\b1> / ],
		[ "COPY regress_psql_tap_1_t1(data)\n", qr/\b2> / ],
		[ "FROM STDIN;\n", qr/\b3> / ],
		[ "calvin\nhobbes\nsusie\nmoe\n\\.\n", qr/COPY 4\b.*\b1> /s ],
		# reset prompts
		[ "\\set PROMPT1 '%/%R%# '\n", qr/PROMPT1.*postgres=# /s ],
		[ "\\set PROMPT2 '%/%R%# '\n", qr/PROMPT2.*postgres=# /s ],
		[ "\\set PROMPT3 '>> '\n", qr/PROMPT3.*postgres=# /s ],
	  ],
	  'ipsql prompt 1');

goto END_HISTORY_ZONE
  unless $has_history;

# signals
my $stop = $is_like_unix ? "\004" : "\003";
ipsql('-P pager', 0, 5,
	  [
		[ "\\echo pg :VERSION_NUM\n", qr/pg \d+.*postgres=\# /s ],
		[ $stop, qr/\\q/ ],
	  ],
	  'ipsql signals 1', 1);

psql('', 0, "\\s /dev/null\n", $EMPTY, $EMPTY, 'psql \s null');

# tab-completion tests
ipsql('-P pager', 0, 5,
	  [ # commands
		[ "SEL\t", qr/SELECT / ],
		[ "* FROM pg_catalog.pg_ca\t", qr/pg_catalog\.pg_cast / ],
		[ "LIMIT 17;\n", qr/\b17 rows.*postgres=\# /s ],
		# backslash commands
		[ "\\d\t", qr/(\\da.*\\dy.*postgres=# )?\\d/s ],
		[ "y\n", qr/List of event triggers.*postgres=\# /s ],
		# pset
		[ "\\pset \t", qr/(unicode_column_linestyle.*postgres=\# )?\\pset/s ],
		[ "tu\t", qr/tuples_only / ],
		[ "off\n", qr/postgres=\# / ],
		[ "\\pset unicode_b\t", qr/unicode_border_linestyle / ],
		[ "\t", qr/(double.*single.*postgres=\# )?/s ],
		[ "double\n", qr/Unicode border line style is "double".*postgres=\# /s ],
		# set
		[ "\\set VERB\t", qr/VERBOSITY / ],
		[ "\t", qr/(default.*verbose.*postgres=\# )?/s ],
		[ "d\t\n", qr/default.*postgres=\# /s ],
		# misc
		[ "\\cd \t", qr/(.*postgres=\# )?\\cd /s ],
		[ "\\r\n", qr/Query buffer reset.*postgres=\# /s ],
		# object type completion,
		# strange: the list is not seen from the test driver
		# but the coverage works as expected.
		[ "CREATE \t\\r\n", qr/Query buffer reset.*postgres=\# /s ],
		[ "DROP \t\\r\n", qr/Query buffer reset.*postgres=\# /s ],
		[ "ALTER \t\\r\n", qr/Query buffer reset.*postgres=\# /s ],
		[ "ANALYZE (\t", qr/(SKIP_LOCKED.*postgres=\# )?ANALYZE \(/s ],
		[ "V\tOF\t", qr/VERBOSE OFF/ ],
		[ "\\r\n", qr/Query buffer reset.*postgres=\# /s ],
		# SET
		[ "SET \t", qr/(zero_damaged_pages.*postgres=\# )?SET /s ],
		[ "da\t", qr/datestyle / ],
		[ "\t", qr/TO / ],
		[ "\tDE\t", qr/DEFAULT/ ],
		[ ";\n", qr/SET.*postgres=\# /s ],
		# psql variables
		[ "\\ec\t", qr/\\echo / ],
		[ ":q\t", qr/QUIET/ ],
		[ ":'PO\t", qr/:'PORT'/ ],
		[ ":\"HISTC\t", qr/:"HISTCONTROL"/ ],
		[ "\n", qr/(on|off).*'\d+'.*"[a-z]+".*postgres=\# /s ],
		# file
		[ "COPY regress_psql_tap_1_t1 FROM \t", qr/(postgres=\# COPY .* FROM )?/ ],
		[ "\\r\n", qr/Query buffer reset.*postgres=\# /s ],
	  ],
	  'ipsql tab 1');

END_HISTORY_ZONE:

# final cleanup
psql('', 0, '-- cleanup objects
DROP VIEW regress_psql_tap_1_v1;
DROP FUNCTION regress_psql_tap_1_f1;
DROP TABLE regress_psql_tap_1_t1;
DROP USER regress_psql_tap_1;
', [ qr{DROP VIEW}, qr{DROP FUNCTION}, qr{DROP TABLE}, qr{DROP ROLE} ], [ qr{^$} ], 'psql -- drop stuff');

$node->stop();
done_testing();
