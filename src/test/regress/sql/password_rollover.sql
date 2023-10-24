--
-- Tests for password rollovers
--

SET password_encryption = 'md5';

-- Create a role, as usual
CREATE ROLE regress_password_rollover1 PASSWORD 'p1' LOGIN;

-- the rolpassword field should be non-null, and others should be null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- Add another password that the role can use for authentication.
ALTER ROLE regress_password_rollover1 ADD SECOND PASSWORD 'p2';

-- the rolpassword and rolsecondpassword fields should be non-null, and others should be null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- Set second password's expiration time.
ALTER ROLE regress_password_rollover1 SECOND PASSWORD VALID UNTIL '2021/01/01';

-- the rolvaliduntil field should be null, and other should be non-null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

ALTER ROLE regress_password_rollover1 FIRST PASSWORD VALID UNTIL '2022/01/01';

-- All fields should be non-null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- Setting a password to null does not set its expiration time to null
ALTER ROLE regress_password_rollover1 PASSWORD NULL;

-- the rolpassword field should be null, and others should be non-null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- If, for some reason, the role wants to get rid of the latest password added.
ALTER ROLE regress_password_rollover1 DROP SECOND PASSWORD;

-- the rolpassword and rolsecondpassword fields should be null, and others should be non-null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- Add a new password in 'second' slot
ALTER ROLE regress_password_rollover1 ADD SECOND PASSWORD 'p3' SECOND PASSWORD VALID UNTIL '2023/01/01';

-- the rolpassword field should be null, and others should be non-null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- VALID UNTIL must not be allowed when ADDing a password, to avoid the
-- confusing invocation where the command may seem to do one thing but actually
-- does something else. The following may seem like it will add a 'second'
-- password with a new expiration, but, if allowed, this will set the expiration
-- time on the _first_ password.
ALTER ROLE regress_password_rollover1 ADD SECOND PASSWORD 'p4' VALID UNTIL '2023/01/01';

-- Even though both, the password and the expiration, refer to the first
-- password, we disallow it to be consistent with the previous command's
-- behaviour.
ALTER ROLE regress_password_rollover1 ADD FIRST PASSWORD 'p4' VALID UNTIL '2023/01/01';

-- Set the first password
ALTER ROLE regress_password_rollover1 ADD FIRST PASSWORD 'p5';

-- Attempting to add a password while the respective slot is occupied
-- results in error
ALTER ROLE regress_password_rollover1 ADD FIRST PASSWORD 'p6';

ALTER ROLE regress_password_rollover1 ADD SECOND PASSWORD 'p6';

ALTER ROLE regress_password_rollover1 DROP SECOND PASSWORD;

-- The rolsecondpassword field should be null, and others should be non-null
SELECT rolname, rolpassword, rolvaliduntil, rolsecondpassword, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- Use scram-sha-256 for password storage
SET password_encryption = 'scram-sha-256';

-- Trying to add a scram-sha-256 based password, while the other password uses
-- md5, should raise an error.
ALTER ROLE regress_password_rollover1 ADD SECOND PASSWORD 'p7'
    SECOND PASSWORD VALID UNTIL 'Infinity';

-- Drop the first password
ALTER ROLE regress_password_rollover1 DROP FIRST PASSWORD;

-- Adding a scram-sha-256 based password should now be allowed.
ALTER ROLE regress_password_rollover1 ADD SECOND PASSWORD 'p7'
    SECOND PASSWORD VALID UNTIL 'Infinity';

-- The rolsecondpassword field should now contain a SCRAM secret, and the rolpassword field should now be null.
SELECT rolname, rolpassword, rolvaliduntil, regexp_replace(rolsecondpassword, '(SCRAM-SHA-256)\$(\d+):([a-zA-Z0-9+/=]+)\$([a-zA-Z0-9+=/]+):([a-zA-Z0-9+/=]+)', '\1$\2:<salt>$<storedkey>:<serverkey>') as rolsecondpassword_masked, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- Adding another scram-sha-256 based password should also be allowed.
ALTER ROLE regress_password_rollover1 ADD FIRST PASSWORD 'p8'
    FIRST PASSWORD VALID UNTIL 'Infinity';

-- The rolpassword and rolsecondpassword field should now contain a SCRAM secret
SELECT rolname, regexp_replace(rolpassword, '(SCRAM-SHA-256)\$(\d+):([a-zA-Z0-9+/=]+)\$([a-zA-Z0-9+=/]+):([a-zA-Z0-9+/=]+)', '\1$\2:<salt>$<storedkey>:<serverkey>') as rolpassword_masked, rolvaliduntil, regexp_replace(rolsecondpassword, '(SCRAM-SHA-256)\$(\d+):([a-zA-Z0-9+/=]+)\$([a-zA-Z0-9+=/]+):([a-zA-Z0-9+/=]+)', '\1$\2:<salt>$<storedkey>:<serverkey>') as rolsecondpassword_masked, rolsecondvaliduntil
    FROM pg_authid
    WHERE rolname LIKE 'regress_password_rollover%';

-- Switch back to md5 for password storage
SET password_encryption = 'md5';

-- Ensure the second password is empty, before we populate it
ALTER ROLE regress_password_rollover1 DROP SECOND PASSWORD;

-- Trying to add an md5 based password, while the other password uses
-- scram-sha-256, should raise an error.
ALTER ROLE regress_password_rollover1 ADD SECOND PASSWORD 'p9'
    SECOND PASSWORD VALID UNTIL 'Infinity';

DROP ROLE regress_password_rollover1;
