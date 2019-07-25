-- Use transaction block so that the file does not closed automatically at
-- command boundary.
BEGIN;
SELECT buffile_open_transient('trans1', true, false);
SELECT buffile_write('01234567');
SELECT buffile_close_transient();

-- Open for reading.
SELECT buffile_open_transient('trans1', false, false);
SELECT length(buffile_read(65536));
SELECT buffile_close_transient();

-- Open for writing in append mode.
SELECT buffile_open_transient('trans1', true, true);
-- Add BLCKSZ bytes, so that buffer boundary is crossed.
SELECT buffile_write(repeat('x', 8192));
SELECT buffile_close_transient();

-- Open for reading and verify the valid part.
SELECT buffile_open_transient('trans1', false, false);
SELECT length(buffile_read(65536));
SELECT buffile_close_transient();

SELECT buffile_delete_file('trans1');
COMMIT;
