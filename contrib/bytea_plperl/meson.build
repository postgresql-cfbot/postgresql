# Copyright (c) 2023, PostgreSQL Global Development Group

if not perl_dep.found()
  subdir_done()
endif

bytea_plperl_sources = files(
  'bytea_plperl.c',
)

if host_system == 'windows'
  bytea_plperl_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'bytea_plperl',
    '--FILEDESC', 'bytea_plperl - bytea transform for plperl',])
endif

bytea_plperl = shared_module('bytea_plperl',
  bytea_plperl_sources,
  include_directories: [plperl_inc],
  kwargs: contrib_mod_args + {
    'dependencies': [perl_dep, contrib_mod_args['dependencies']],
    'install_rpath': ':'.join(mod_install_rpaths + ['@0@/CORE'.format(archlibexp)]),
    'build_rpath': '@0@/CORE'.format(archlibexp),
  },
)
contrib_targets += bytea_plperl

install_data(
  'bytea_plperl.control',
  'bytea_plperl--1.0.sql',
  kwargs: contrib_data_args,
)

install_data(
  'bytea_plperlu.control',
  'bytea_plperlu--1.0.sql',
  kwargs: contrib_data_args,
)


tests += {
  'name': 'bytea_plperl',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'bytea_plperl',
      'bytea_plperlu',
    ],
  },
}
