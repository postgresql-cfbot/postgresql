BEGIN;
SELECT buffile_create();
-- Read from an empty file.
SELECT buffile_seek(0, 8);
SELECT buffile_read(16);
SELECT buffile_close();
COMMIT;
