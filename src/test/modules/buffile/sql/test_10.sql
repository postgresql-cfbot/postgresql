BEGIN;
SELECT buffile_create();
-- Write some data at the end of the buffer.
SELECT buffile_seek(0, 8188);
SELECT buffile_write('abcd');
SELECT buffile_seek(0, 8189);
-- Enforce flush with the write position not at the end of the buffer. This is
-- special by not moving curOffset to the next buffer.
SELECT buffile_read(1);

-- Therefore the next writes should eventually affect the original data. (Here
-- we also test going directly from read to write and vice versa.)
SELECT buffile_write('x');
SELECT buffile_read(1);

-- Start a new buffer, i.e. force flushing of the previous one.
SELECT buffile_write('z');

-- Check that the 'x' and 'y' letters are in the first buffer, not in the
-- 2nd. (We read enough data to find any non-zero bytes in the 2nd buffer.)
SELECT buffile_seek(0, 8188);
SELECT buffile_read(4 + 8192);

SELECT buffile_close();
COMMIT;
