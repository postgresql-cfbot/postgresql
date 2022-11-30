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

die unless $alg =~ 'RSAES_OAEP_SHA';

my $digest = $alg;
$digest =~ s/.*(?=SHA)//;
$digest =~ s/_//g;

my $openssl = $ENV{OPENSSL};

open my $fh, '>:raw', "${tmpdir}/input.tmp" or die $!;
print $fh decode_base64($b64data);
close $fh;

my @cmd = (
	$openssl, 'pkeyutl', '-decrypt',
	'-inkey', "${tmpdir}/${cmkname}.pem", '-pkeyopt', 'rsa_padding_mode:oaep',
	'-in', "${tmpdir}/input.tmp", '-out', "${tmpdir}/output.tmp"
);

if ($digest ne 'SHA1')
{
	# These options require OpenSSL >=1.1.0, so if the digest is
	# SHA1, which is the default, omit the options.
	push @cmd,
	  '-pkeyopt', "rsa_mgf1_md:$digest",
	  '-pkeyopt', "rsa_oaep_md:$digest";
}

system(@cmd) == 0 or die "system failed: $?";

open $fh, '<:raw', "${tmpdir}/output.tmp" or die $!;
my $data = '';

while (1) {
	my $success = read $fh, $data, 100, length($data);
	die $! if not defined $success;
	last if not $success;
}

close $fh;

unlink "${tmpdir}/input.tmp", "${tmpdir}/output.tmp";

print encode_base64($data);
