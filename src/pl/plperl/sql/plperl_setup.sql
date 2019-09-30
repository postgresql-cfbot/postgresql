--
-- Install the plperl and plperlu extensions
--

-- Before going ahead with the to-be-tested installations, verify that
-- a non-superuser is allowed to install plperl (but not plperlu) when
-- the trusted-extensions configuration permits.

CREATE USER regress_user1;
CREATE USER regress_user2;

SET trusted_extensions_anyone = '^$';

SET ROLE regress_user1;

CREATE EXTENSION plperl;  -- fail
CREATE EXTENSION plperlu;  -- fail

SET trusted_extensions_anyone = '^plperl';  -- fail

RESET ROLE;

SET trusted_extensions_anyone = '^plperl';

SET ROLE regress_user1;

CREATE EXTENSION plperl;
CREATE EXTENSION plperlu;  -- fail

CREATE FUNCTION foo1() returns int language plperl as '1;';
SELECT foo1();

-- Should be able to change privileges on the language
revoke all on language plperl from public;

SET ROLE regress_user2;

CREATE FUNCTION foo2() returns int language plperl as '2;';  -- fail

SET ROLE regress_user1;

grant usage on language plperl to regress_user2;

SET ROLE regress_user2;

CREATE FUNCTION foo2() returns int language plperl as '2;';
SELECT foo2();

SET ROLE regress_user1;

-- Should be able to drop the extension, but not the language per se
DROP LANGUAGE plperl CASCADE;
DROP EXTENSION plperl CASCADE;

-- Clean up
RESET ROLE;
DROP USER regress_user1;
DROP USER regress_user2;

-- Now install the versions that will be used by subsequent test scripts.
CREATE EXTENSION plperl;
CREATE EXTENSION plperlu;
