-- This is the first test, so make sure the test extension is there.
CREATE EXTENSION IF NOT EXISTS buffile;

BEGIN;
SELECT buffile_create();
SELECT buffile_seek(0, 1);
SELECT buffile_write('abc');
SELECT buffile_seek(0, 0);
-- Check that the trailing zeroes are not fetched.
SELECT buffile_read(16);
-- Adjust the number of useful bytes.
SELECT buffile_write('abc');
-- ... and check again.
SELECT buffile_seek(0, 0);
SELECT buffile_read(16);
SELECT buffile_close();
COMMIT;
