# Copyright (c) 2022-2024, PostgreSQL Global Development Group

tests += {
  'name': 'test_misc',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_constraint_validation.pl',
      't/002_tablespace.pl',
      't/003_check_guc.pl',
      't/004_io_direct.pl',
      't/099_case_15598.pl',
      't/051_dropdb_force.pl',
      't/099_pgbench_with_server_off.pl',
      't/099_ecpg_16678.pl',
      't/099_commit_ts_002_standby.pl',
    ],
  },
}
