# Copyright (c) 2022-2023, PostgreSQL Global Development Group

wait_event_names = custom_target('wait_event_names',
  input: files('./wait_event_names.txt'),
  output: ['pgstat_wait_event.c'],
  command: [
    perl, files('./generate-wait_event_types.pl'),
    '-o', '@OUTDIR@', '--code',
    '@INPUT@'
  ],
  install: true,
  install_dir: [false],
)

backend_sources += files(
  'backend_progress.c',
  'backend_status.c',
  'pgstat.c',
  'pgstat_archiver.c',
  'pgstat_bgwriter.c',
  'pgstat_checkpointer.c',
  'pgstat_database.c',
  'pgstat_function.c',
  'pgstat_io.c',
  'pgstat_relation.c',
  'pgstat_replslot.c',
  'pgstat_shmem.c',
  'pgstat_slru.c',
  'pgstat_subscription.c',
  'pgstat_wal.c',
  'pgstat_xact.c',
)

# these include .c files generated in ../../../include/activity, seems nicer to not
# add that as an include path for the whole backend
waitevent_sources = files(
  'wait_event.c',
)

backend_link_with += static_library('wait_event_names',
  waitevent_sources,
  dependencies: [backend_code],
  include_directories: include_directories('../../../include/utils'),
  kwargs: internal_lib_args,
)
