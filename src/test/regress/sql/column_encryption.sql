\set HIDE_COLUMN_ENCRYPTION false

CREATE COLUMN MASTER KEY cmk1 WITH (
    realm = 'test'
);

CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (
    column_master_key = cmk1,
    algorithm = 'RSAES_OAEP_SHA_1',
    encrypted_value = '\xDEADBEEF'
);

CREATE TABLE tbl_fail (
    a int,
    b text,
    c text ENCRYPTED WITH (column_encryption_key = notexist)
);

CREATE TABLE tbl_29f3 (
    a int,
    b text,
    c text ENCRYPTED WITH (column_encryption_key = cek1)
);

\d tbl_29f3
\d+ tbl_29f3

DROP TABLE tbl_29f3;  -- FIXME: needs pg_dump support for pg_upgrade tests
