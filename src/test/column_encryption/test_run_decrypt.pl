#!/usr/bin/perl

# Test/sample command for libpq cmklookup run scheme
#
# This just places the data into temporary files and runs the openssl
# command on it.  (In practice, this could more simply be written as a
# shell script, but this way it's more portable.)

# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;

use MIME::Base64;

my ($tmpdir, $cmkname, $alg, $b64data) = @ARGV;

die unless $alg eq 'RSAES_OAEP_SHA_1';

open my $fh, '>:raw', "${tmpdir}/input.tmp" or die;
print $fh decode_base64($b64data);
close $fh;

system('openssl', 'pkeyutl', '-decrypt',
	'-inkey', "${tmpdir}/${cmkname}.pem", '-pkeyopt', 'rsa_padding_mode:oaep',
	'-in', "${tmpdir}/input.tmp", '-out', "${tmpdir}/output.tmp") == 0 or die;

open $fh, '<:raw', "${tmpdir}/output.tmp" or die;
my $data = '';

while (1) {
	my $success = read $fh, $data, 100, length($data);
	die $! if not defined $success;
	last if not $success;
}

close $fh;

unlink "${tmpdir}/input.tmp", "${tmpdir}/output.tmp";

print encode_base64($data);
