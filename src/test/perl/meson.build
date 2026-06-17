# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# could use install_data's preserve_path option in >=0.64.0

install_data(
  'PostgreSQL/Version.pm',
  'PostgreSQL/FindLib.pm',
  'PostgreSQL/PqFFI.pm',
  'PostgreSQL/PqConstants.pm',
  'PostgreSQL/PGTypes.pm',
  install_dir: dir_pgxs / 'src/test/perl/PostgreSQL')

install_data(
  'PostgreSQL/Test/Utils.pm',
  'PostgreSQL/Test/SimpleTee.pm',
  'PostgreSQL/Test/RecursiveCopy.pm',
  'PostgreSQL/Test/Kerberos.pm',
  'PostgreSQL/Test/Cluster.pm',
  'PostgreSQL/Test/BackgroundPsql.pm',
  'PostgreSQL/Test/AdjustDump.pm',
  'PostgreSQL/Test/AdjustUpgrade.pm',
  'PostgreSQL/Test/Session.pm',
  install_dir: dir_pgxs / 'src/test/perl/PostgreSQL/Test')
