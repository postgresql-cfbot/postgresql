BEGIN;
SELECT buffile_create();
SELECT buffile_write('abcd');
-- Seek beyond EOF not followed by write.
SELECT buffile_seek(0, 5);
-- Nothing should be fetched.
SELECT buffile_read(8);
SELECT buffile_close();
COMMIT;
