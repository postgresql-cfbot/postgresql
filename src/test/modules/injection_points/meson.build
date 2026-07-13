# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if not get_option('injection_points')
  subdir_done()
endif

injection_points_sources = files(
  'injection_points.c',
  'regress_injection.c',
)

if host_system == 'windows'
  injection_points_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'injection_points',
    '--FILEDESC', 'injection_points - facility for injection points',])
endif

injection_points = shared_module('injection_points',
  injection_points_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += injection_points

test_install_data += files(
  'injection_points.control',
  'injection_points--1.0.sql',
)

tests += {
  'name': 'injection_points',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'injection_points',
      'hashagg',
      'key_join',
      'reindex_conc',
      'vacuum',
    ],
    'regress_args': ['--dlpath', meson.project_build_root() / 'src/test/regress'],
    # The injection points are cluster-wide, so disable installcheck
    'runningcheck': false,
  },
  'isolation': {
    'specs': [
      'basic',
      'inplace',
      'repack',
      'repack_temporal',
      'repack_temporal_multirange',
      'repack_toast',
      'syscache-update-pruned',
      'heap_lock_update',
      'key_join_proof_race_constraints',
      'key_join_proof_race_filter_function_prelock',
      'key_join_proof_race_filter_operator_prelock',
      'key_join_proof_race_function_prelock',
      'key_join_proof_race_indexes',
      'key_join_proof_race_operator_cascade',
      'key_join_proof_race_operator_prelock',
      'key_join_proof_race_producer_view',
      'key_join_proof_race_relation_state',
      'key_join_cached_plan_inherit',
    ],
    'runningcheck': false, # see syscache-update-pruned
    # Some tests wait for all snapshots, so avoid parallel execution
    'runningcheck-parallel': false,
    # some tests require wal_level=replica
    'regress_args': [
      '--temp-config', files('extra.conf'),
    ],
  },
}
