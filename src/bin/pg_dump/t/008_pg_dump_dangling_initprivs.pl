# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Tests that pg_dump silently skips pg_init_privs entries that reference
# roles no longer present in pg_authid, rather than emitting invalid GRANT
# statements with numeric OIDs as role names.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE regress_dangling');

# --- Setup ---
# Simulate dangling pg_init_privs entries by inserting grants for a role
# and then deleting the role directly from pg_authid (bypassing pg_shdepend).
$node->safe_psql(
	'regress_dangling',
	q{
SET allow_system_table_mods = true;

-- Roles for testing
CREATE ROLE ghost_grantee;
CREATE ROLE ghost_grantor;
CREATE ROLE "007";

-- Case 1: dangling grantee (function)
CREATE FUNCTION public.test_func_grantee() RETURNS int LANGUAGE sql AS 'SELECT 1';
INSERT INTO pg_init_privs (objoid, classoid, objsubid, privtype, initprivs)
SELECT p.oid,
       (SELECT oid FROM pg_class WHERE relname = 'pg_proc'),
       0, 'e',
       ARRAY[('ghost_grantee=X/' || current_user)::aclitem]
FROM pg_proc p
WHERE p.proname = 'test_func_grantee'
  AND p.pronamespace = 'public'::regnamespace;

-- Case 2: dangling grantor (function)
CREATE FUNCTION public.test_func_grantor() RETURNS int LANGUAGE sql AS 'SELECT 2';
INSERT INTO pg_init_privs (objoid, classoid, objsubid, privtype, initprivs)
SELECT p.oid,
       (SELECT oid FROM pg_class WHERE relname = 'pg_proc'),
       0, 'e',
       ARRAY[(current_user || '=X/ghost_grantor')::aclitem]
FROM pg_proc p
WHERE p.proname = 'test_func_grantor'
  AND p.pronamespace = 'public'::regnamespace;

-- Case 3: dangling column-level grantee (table)
CREATE TABLE public.test_tbl (id int, secret text);
INSERT INTO pg_init_privs (objoid, classoid, objsubid, privtype, initprivs)
SELECT c.oid,
       (SELECT oid FROM pg_class WHERE relname = 'pg_class'),
       2, 'e',
       ARRAY[('ghost_grantee=r/' || current_user)::aclitem]
FROM pg_class c
WHERE c.relname = 'test_tbl'
  AND c.relnamespace = 'public'::regnamespace;

-- Case 4: spurious REVOKE -- all-dangling initprivs on a catalog function
-- with NULL proacl (simulates the spurious-selection scenario)
CREATE FUNCTION public.test_func_revoke() RETURNS int LANGUAGE sql AS 'SELECT 3';
INSERT INTO pg_init_privs (objoid, classoid, objsubid, privtype, initprivs)
SELECT p.oid,
       (SELECT oid FROM pg_class WHERE relname = 'pg_proc'),
       0, 'e',
       ARRAY[('ghost_grantee=X/' || current_user)::aclitem]
FROM pg_proc p
WHERE p.proname = 'test_func_revoke'
  AND p.pronamespace = 'public'::regnamespace;

-- Case 5: valid all-digit role "007" with a grant (must NOT be filtered)
CREATE FUNCTION public.test_func_007() RETURNS int LANGUAGE sql AS 'SELECT 7';
GRANT EXECUTE ON FUNCTION public.test_func_007() TO "007";

-- Now delete the ghost roles to create dangling OIDs
DELETE FROM pg_authid WHERE rolname = 'ghost_grantee';
DELETE FROM pg_authid WHERE rolname = 'ghost_grantor';


});

my $tempdir   = PostgreSQL::Test::Utils::tempdir;
my $dump_file = "$tempdir/dangling.sql";

# pg_dump must succeed even with dangling pg_init_privs entries.
command_ok(
	[
		'pg_dump',
		'--port'        => $node->port,
		'--schema-only',
		'-f'            => $dump_file,
		'regress_dangling',
	],
	'pg_dump succeeds with dangling pg_init_privs entries');

my $dump = slurp_file($dump_file);

# --- Case 1: dangling grantee ---
like($dump, qr/CREATE FUNCTION public\.test_func_grantee/,
	'case 1: function is present in dump');
unlike($dump, qr/GRANT\b.*\btest_func_grantee/,
	'case 1: no GRANT for function with dangling grantee');

# --- Case 2: dangling grantor ---
like($dump, qr/CREATE FUNCTION public\.test_func_grantor/,
	'case 2: function is present in dump');
unlike($dump, qr/GRANT\b.*\btest_func_grantor/,
	'case 2: no GRANT for function with dangling grantor');

# --- Case 3: column-level dangling ---
like($dump, qr/CREATE TABLE public\.test_tbl/,
	'case 3: table is present in dump');
unlike($dump, qr/GRANT\b.*\btest_tbl\b.*secret/i,
	'case 3: no column GRANT for dangling column-level entry');

# --- Case 4: spurious REVOKE ---
like($dump, qr/CREATE FUNCTION public\.test_func_revoke/,
	'case 4: function is present in dump');
unlike($dump, qr/REVOKE\b.*\btest_func_revoke/,
	'case 4: no spurious REVOKE for function with all-dangling initprivs');

# --- Case 5: valid all-digit role "007" ---
like($dump, qr/CREATE FUNCTION public\.test_func_007/,
	'case 5: function is present in dump');
like($dump, qr/GRANT\b.*\btest_func_007\b.*TO\s+"007"/,
	'case 5: GRANT to valid all-digit role "007" is preserved');

# --- General: no numeric OID as role name (other than the valid "007") ---
# Match any GRANT ... TO "digits" where the digits are NOT "007".
unlike($dump, qr/GRANT\b.*\bTO\s+"(?!007")[0-9]+"/,
	'no GRANT with bare numeric OID as role name (other than valid "007")');

done_testing();
