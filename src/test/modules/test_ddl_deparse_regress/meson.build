# FIXME: prevent install during main install, but not during test :/

test_ddl_deparse_regress_sources = files(
  'test_ddl_deparse_regress.c',
)

if host_system == 'windows'
  test_ddl_deparse_regress_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_ddl_deparse_regress',
    '--FILEDESC', 'test_ddl_deparse_regress - regression testing for DDL deparsing',])
endif

test_ddl_deparse_regress = shared_module('test_ddl_deparse_regress',
  test_ddl_deparse_regress_sources,
  kwargs: pg_mod_args,
)
testprep_targets += test_ddl_deparse_regress

install_data(
  'test_ddl_deparse_regress.control',
  'test_ddl_deparse_regress--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'test_ddl_deparse_regress',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_ddl_deparse',
      'create_extension',
      'create_schema',
      'create_table',
    ],
  },
  'tap': {
    'tests': [
      't/001_compare_dumped_results.pl',
    ],
  },
}
