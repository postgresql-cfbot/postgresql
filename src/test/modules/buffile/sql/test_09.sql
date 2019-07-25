BEGIN;
SELECT buffile_create();
-- Write data across buffer boundary and try to read it.
SELECT buffile_seek(0, 8190);
SELECT buffile_write('abcd');
SELECT buffile_seek(0, 8190);
SELECT buffile_read(8);
SELECT buffile_close();
COMMIT;
