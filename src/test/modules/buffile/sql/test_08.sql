BEGIN;
SELECT buffile_create();

-- Neither disk space nor time needs to be wasted.
SET buffile_max_filesize TO 8192;

-- Write data across component file boundary and try to read it.
SELECT buffile_seek(0, 8190);
SELECT buffile_write('abcd');
SELECT buffile_seek(0, 8190);
SELECT buffile_read(8);
SELECT buffile_close();
COMMIT;
