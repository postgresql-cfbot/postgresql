
=pod

=head1 NAME

PostgresNodePath - subclass of PostgresNode using a given Postgres install

=head1 SYNOPSIS

  use lib '/path/to/postgres/src/test/perl';
  use PostgresNodePath;

  my $node = get_new_node('/path/to/binary/installation','my_node');

  or

  my $node = PostgresNodePath->get_new_node('/path/to/binary/installation',
                                            'my_node');

  $node->init();
  $node->start();

  ...

=head1 DESCRIPTION

PostgresNodePath is a subclass of PostgresNode which runs commands in the
context of a given PostgreSQL install path.  The given path is
is expected to have a standard install, with bin and lib
subdirectories.

The only API difference between this and PostgresNode is
that get_new_node() takes an additional parameter in position
1 that contains the install path. Everything else is either
inherited from PostgresNode or overridden but with identical
parameters.

As with PostgresNode, the environment variable PG_REGRESS
must point to a binary of pg_regress, in order to init() a
node.

=cut



package PostgresNodePath;

use strict;
use warnings;

use parent qw(PostgresNode);

use Exporter qw(import);
our @EXPORT = qw(get_new_node);

use Config;
use TestLib();

sub get_new_node
{
    my $class = __PACKAGE__;
    die 'no args' unless scalar(@_);
    my $installpath = shift;
    # check if we got a class name before the installpath
    if ($installpath =~ /^[A-Za-z0-9_](::[A-Za-z0-9_])*/
        && !-e "$installpath/bin")
    {
        $class       = $installpath;
        $installpath = shift;
    }
    my $node = PostgresNode->get_new_node(@_);
    bless $node, $class;    # re-bless
    $node->{_installpath} = $installpath;
    return $node;
}


# class methods we don't override:
# new() # should probably be hidden
# get_free_port()
# can_bind()


# instance methods we don't override because we don't need to:
# port() host() basedir() name() logfile() connstr() group_access() data_dir()
# archive_dir() backup_dir() dump_info() ? set_replication_conf() append_conf()
# backup_fs_hot() backup_fs_cold() _backup_fs() init_from_backup()
# rotate_logfile() enable_streaming() enable_restoring() set_recovery_mode()
# set_standby_mode() enable_archiving() _update_pid teardown_node()
# clean_node() lsn() wait_for_catchup() wait_for_slot_catchup() query_hash()
# slot()

# info is special - we add in the installpath spec
# no need for environment override
sub info
{
    my $node = shift;
    my $inst = $node->{_installpath};
    my $res  = $node->SUPER::info();
    $res .= "Install Path: $inst\n";
    return $res;
}

BEGIN
{

    # putting this in a BEGIN block means it's run and checked by perl -c


    # everything other than info and get_new_node that we need to override.
    # they are all instance methods, so we can use the same template for all.
    my @instance_overrides = qw(init backup start kill9 stop reload restart
      promote logrotate safe_psql psql background_psql
      interactive_psql poll_query_until command_ok
      command_fails command_like command_checks_all
      issues_sql_like run_log pg_recvlogical_upto
    );

    my $dylib_name =
      $Config{osname} eq 'darwin' ? "DYLD_LIBRARY_PATH" : "LD_LIBRARY_PATH";

    my $template = <<'    EOSUB';

    sub SUBNAME
    {
        my $node=shift;
        my $inst = $node->{_installpath};
        local %ENV = %ENV;
        if ($TestLib::windows_os)
        {
            # Windows picks up DLLs from the PATH rather than *LD_LIBRARY_PATH
            # choose the right path separator
            if ($Config{osname} eq 'MSWin32')
            {
               $ENV{PATH} = "$inst/bin;$inst/lib;$ENV{PATH}";
            }
            else
            {
               $ENV{PATH} = "$inst/bin:$inst/lib:$ENV{PATH}";
            }
        }
        else
        {
            $ENV{PATH} = "$inst/bin:$ENV{PATH}";
            if (exists $ENV{DYLIB})
            {
                $ENV{DYLIB} = "$inst/lib:$ENV{DYLIB}";
            }
            else
            {
                $ENV{DYLIB} = "$inst/lib";
            }
        }
        $node->SUPER::SUBNAME(@_);
    }

    EOSUB

    foreach my $subname (@instance_overrides)
    {
        my $code = $template;
        $code =~ s/SUBNAME/$subname/g;
        $code =~ s/DYLIB/$dylib_name/g;
        ## no critic (ProhibitStringyEval)
        eval($code);
    }
}

1;
