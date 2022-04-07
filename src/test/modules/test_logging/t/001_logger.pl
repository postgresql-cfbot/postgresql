
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;

use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

my $node = PostgreSQL::Test::Cluster->new('testlogging');
$node->init;
$node->append_conf('postgresql.conf', qq{
  log_destination = 'csvlog, jsonlog, stderr'
  log_min_messages = NOTICE
  log_directory = '$tempdir'
  logging_collector = on
  log_filename = 'postgresql.log'
  log_error_verbosity = verbose
});
$node->start;

$node->safe_psql('postgres',
  'create extension test_logging;');
$node->safe_psql('postgres',
  "SELECT test_logging(18, 'This is a test message',
  '{\"tag1\": \"value1\", \"tag2\": \"value2\"}');");

my $logcontents;

# Check the plaintext log file
$logcontents = slurp_file($tempdir . '/' . 'postgresql.log');
like($logcontents, qr/This is a test message/,
  "Found expected message in log");
like($logcontents, qr/DETAIL:  NB TAGS: 2 TAGS: tag1: value1 tag2: value2/,
  "Found expected tags in log");

# This is not optimal, but check the logs using regexpes to avoid
# adding a dependency on Text::CSV and JSON::XS

# Check the csv log file.
$logcontents = slurp_file($tempdir . '/' . 'postgresql.csv');

like($logcontents, qr/,"\{""tag1"":""value1"",""tag2"":""value2""}"/,
  "CSV log contains tag");
like($logcontents, qr/,,"test_logging, test_logging.c:\d+",/,
  "CSV log contains filename and linenumber");

# Check the json log file.
$logcontents = slurp_file($tempdir . '/' . 'postgresql.json');
like($logcontents, qr/,"tags":\{"tag1":"value1","tag2":"value2"}/,
  "JSON log contains tags");
like($logcontents, qr/,"file_name":"test_logging.c","file_line_num":\d+,/,
  "JSON log contains filename and linenumber");

$node->stop;
