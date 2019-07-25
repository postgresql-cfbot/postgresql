-- This test shows that the if first component file (segment) stays empty,
-- read stops prematurely even if it starts within that segment, although it'd
-- otherwise receive some data from the following one.
BEGIN;

-- Neither disk space nor time needs to be wasted.
SET buffile_max_filesize TO 8192;

SELECT buffile_create();
SELECT buffile_seek(0, 8192);
SELECT buffile_write('a');
SELECT buffile_seek(0, 8191);
SELECT buffile_read(2);
SELECT buffile_close();
COMMIT;
