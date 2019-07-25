BEGIN;

-- Use a small segment, not to waste disk space and time.
SET buffile_max_filesize TO 8192;

SELECT buffile_create();
-- Write data at component file boundary and try to read it.
SELECT buffile_seek(0, 8192);
SELECT buffile_write('abcd');
SELECT buffile_seek(0, 8192);
SELECT buffile_read(8);
SELECT buffile_close();
COMMIT;
