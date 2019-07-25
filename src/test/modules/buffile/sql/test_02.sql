BEGIN;
SELECT buffile_create();
SELECT buffile_seek(0, 8189);
-- Initialize the last 3 positions of the first buffer and the initial 3
-- positions of the 2nd buffer.
SELECT buffile_write('abcdef');
SELECT buffile_seek(0, 0);
-- Read the first buffer.
SELECT length(buffile_read(8192));
-- Only 3 bytes of the 2nd buffer should be fetched.
SELECT length(buffile_read(8192));
SELECT buffile_close();
COMMIT;
