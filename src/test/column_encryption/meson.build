column_encryption_test_client = executable('test_client',
  files('test_client.c'),
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += column_encryption_test_client

tests += {
  'name': 'column_encryption',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_column_encryption.pl',
      't/002_cmk_rotation.pl',
    ],
  },
}
