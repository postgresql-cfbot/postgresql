
\getenv abs_builddir PG_ABS_BUILDDIR
\set filename :abs_builddir '/results/deparse_test_commands.data'

SET allow_system_table_mods = on;
CREATE TABLE pg_catalog.deparse_test_commands (
  id int,
  test_name text,
  deparse_time timestamptz,
  original_command TEXT,
  command TEXT
);

COPY pg_catalog.deparse_test_commands FROM :'filename';
ALTER SYSTEM SET test_deparser.deparse_mode = on;
SELECT pg_reload_conf();
