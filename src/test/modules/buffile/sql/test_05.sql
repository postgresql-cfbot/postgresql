BEGIN;
SELECT buffile_create();
-- Seek does not extend the file if it's not followed by write.
SELECT buffile_seek(0, 1);
SELECT buffile_seek(0, 0);
SELECT buffile_read(2);
SELECT buffile_close();
COMMIT;
