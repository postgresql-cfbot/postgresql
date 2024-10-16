# Copyright (c) 2022-2024, PostgreSQL Global Development Group

pgoutput_cluster_sources = files(
  'pgoutput_cluster.c',
)

if host_system == 'windows'
  pgoutput_cluster_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pgoutput_cluster',
    '--FILEDESC', 'pgoutput_cluster - logical replication output plugin for CLUSTER command',])
endif

pgoutput_cluster = shared_module('pgoutput_cluster',
  pgoutput_cluster_sources,
  kwargs: pg_mod_args,
)

backend_targets += pgoutput_cluster
