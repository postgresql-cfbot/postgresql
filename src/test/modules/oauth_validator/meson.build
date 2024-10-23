# Copyright (c) 2024, PostgreSQL Global Development Group

validator_sources = files(
  'validator.c',
)

if host_system == 'windows'
  validator_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'validator',
    '--FILEDESC', 'validator - test OAuth validator module',])
endif

validator = shared_module('validator',
  validator_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += validator

tests += {
  'name': 'oauth_validator',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_server.pl',
    ],
    'env': {
      'PYTHON': python.path(),
      'with_oauth': oauth_library,
      'with_python': 'yes',
    },
  },
}
