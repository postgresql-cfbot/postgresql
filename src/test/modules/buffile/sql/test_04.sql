BEGIN;
SELECT buffile_create();
-- Write something near the end of the first buffer, but leave some trailing
-- space.
SELECT buffile_seek(0, 8184);
SELECT buffile_write('abcd');
-- Leave the 2nd buffer empty, as well as a few leading bytes. Thus we should
-- get a hole that spans the whole 2nd buffer as well as a few adjacent bytes
-- on each side.
SELECT buffile_seek(0, 2 * 8192 + 4);
SELECT buffile_write('efgh');
-- Check the initial part of the hole, which crosses the boundary of the 1st
-- and the 2nd buffer.
SELECT buffile_seek(0, 8184);
SELECT buffile_read(16);
-- Check the trailing part of the whole, which crosses the boundary of the 2nd
-- and the 3rd buffer.
SELECT buffile_seek(0, 2 * 8192 - 8);
SELECT buffile_read(16);
-- Check that the hole contains nothing but zeroes.
SELECT buffile_seek(0, 8192 - 4);
SELECT btrim(buffile_read(8192 + 8), '\x00');

SELECT buffile_close();
COMMIT;
