\set HIDE_COLUMN_ENCRYPTION false

CREATE ROLE regress_enc_user1;

CREATE COLUMN MASTER KEY cmk1 WITH (
    realm = 'test'
);

COMMENT ON COLUMN MASTER KEY cmk1 IS 'column master key';

CREATE COLUMN MASTER KEY cmk1a WITH (
    realm = 'test'
);

CREATE COLUMN MASTER KEY cmk2 WITH (
    realm = 'test2'
);

CREATE COLUMN MASTER KEY cmk2a WITH (
    realm = 'testx'
);

ALTER COLUMN MASTER KEY cmk2a (realm = 'test2');

CREATE COLUMN ENCRYPTION KEY fail WITH VALUES (
    column_master_key = cmk1,
    algorithm = 'foo',  -- invalid
    encrypted_value = '\xDEADBEEF'
);

CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (
    column_master_key = cmk1,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
);

COMMENT ON COLUMN ENCRYPTION KEY cek1 IS 'column encryption key';

ALTER COLUMN ENCRYPTION KEY cek1 ADD VALUE (
    column_master_key = cmk1a,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
);

-- duplicate
ALTER COLUMN ENCRYPTION KEY cek1 ADD VALUE (
    column_master_key = cmk1a,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
);

ALTER COLUMN ENCRYPTION KEY fail ADD VALUE (
    column_master_key = cmk1a,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
);

CREATE COLUMN ENCRYPTION KEY cek2 WITH VALUES (
    column_master_key = cmk2,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
),
(
    column_master_key = cmk2a,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
);

CREATE COLUMN ENCRYPTION KEY cek4 WITH VALUES (
    column_master_key = cmk1,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
);

CREATE TABLE tbl_fail (
    a int,
    b text,
    c text ENCRYPTED WITH (column_encryption_key = notexist)
);

CREATE TABLE tbl_fail (
    a int,
    b text,
    c text ENCRYPTED WITH (column_encryption_key = cek1, algorithm = 'foo')
);

CREATE TABLE tbl_fail (
    a int,
    b text,
    c text ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = wrong)
);

CREATE TABLE tbl_29f3 (
    a int,
    b text,
    c text ENCRYPTED WITH (column_encryption_key = cek1)
);

\d tbl_29f3
\d+ tbl_29f3

CREATE TABLE tbl_447f (
    a int,
    b text
);

ALTER TABLE tbl_447f ADD COLUMN c text ENCRYPTED WITH (column_encryption_key = cek1);

\d tbl_447f
\d+ tbl_447f

DROP COLUMN MASTER KEY cmk1 RESTRICT;  -- fail

ALTER COLUMN MASTER KEY cmk2 RENAME TO cmk3;
ALTER COLUMN MASTER KEY cmk1 RENAME TO cmk3;  -- fail
ALTER COLUMN MASTER KEY cmkx RENAME TO cmky;  -- fail

ALTER COLUMN ENCRYPTION KEY cek2 RENAME TO cek3;
ALTER COLUMN ENCRYPTION KEY cek1 RENAME TO cek3;  -- fail
ALTER COLUMN ENCRYPTION KEY cekx RENAME TO ceky;  -- fail

SET SESSION AUTHORIZATION 'regress_enc_user1';
DROP COLUMN ENCRYPTION KEY cek3;  -- fail
DROP COLUMN MASTER KEY cmk3;  -- fail
RESET SESSION AUTHORIZATION;
ALTER COLUMN MASTER KEY cmk3 OWNER TO regress_enc_user1;
ALTER COLUMN ENCRYPTION KEY cek3 OWNER TO regress_enc_user1;
SET SESSION AUTHORIZATION 'regress_enc_user1';
DROP COLUMN ENCRYPTION KEY cek3;  -- ok now
DROP COLUMN MASTER KEY cmk3;  -- ok now
RESET SESSION AUTHORIZATION;

ALTER COLUMN ENCRYPTION KEY cek1 DROP VALUE (column_master_key = cmk1a);
ALTER COLUMN ENCRYPTION KEY cek1 DROP VALUE (column_master_key = cmk1a);  -- fail
ALTER COLUMN ENCRYPTION KEY cek1 DROP VALUE (column_master_key = fail);  -- fail
ALTER COLUMN ENCRYPTION KEY cek1 DROP VALUE (column_master_key = cmk1a, algorithm = 'foo');  -- fail

DROP COLUMN ENCRYPTION KEY cek4;
DROP COLUMN ENCRYPTION KEY fail;
DROP COLUMN ENCRYPTION KEY IF EXISTS nonexistent;

DROP COLUMN MASTER KEY cmk1a;
DROP COLUMN MASTER KEY fail;
DROP COLUMN MASTER KEY IF EXISTS nonexistent;

DROP ROLE regress_enc_user1;
