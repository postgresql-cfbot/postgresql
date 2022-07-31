-- Before going ahead with the to-be-tested installations, verify that
-- only superusers can install plperlu.
CREATE USER regress_user1;

SET ROLE regress_user1;
CREATE EXTENSION plperlu;  -- fail
RESET ROLE;

DO $$
begin
  execute format('grant create on database %I to regress_user1',
                 current_database());
end;
$$;

SET ROLE regress_user1;
CREATE EXTENSION plperlu;  -- fail
RESET ROLE;

DO $$
begin
  execute format('revoke create on database %I from regress_user1',
                 current_database());
end;
$$;

DROP ROLE regress_user1;

-- Use ONLY plperlu tests here. For plperl/plerlu combined tests
-- see plperl_plperlu.sql
CREATE EXTENSION plperlu;

-- This test tests setting on_plperlu_init after loading plperl
LOAD 'plperl';

-- Test plperl.on_plperlu_init gets run
SET plperl.on_plperlu_init = '$_SHARED{init} = 42';
DO $$ warn $_SHARED{init} $$ language plperlu;

--
-- Test compilation of unicode regex - regardless of locale.
-- This code fails in plain plperl in a non-UTF8 database.
--
CREATE OR REPLACE FUNCTION perl_unicode_regex(text) RETURNS INTEGER AS $$
  return ($_[0] =~ /\x{263A}|happy/i) ? 1 : 0; # unicode smiley
$$ LANGUAGE plperlu;
