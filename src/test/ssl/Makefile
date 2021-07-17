#-------------------------------------------------------------------------
#
# Makefile for src/test/ssl
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/ssl/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/ssl
top_builddir = ../../..

clean_intermediates := yes
include $(top_builddir)/src/Makefile.global

export with_ssl

#
# To add a new server or client certificate, add a new <name>.config file in
# this directory, then add <name> to either SERVERS or CLIENTS below. A
# key/certificate pair will be generated for you, signed by the appropriate CA.
#
SERVERS := server-cn-and-alt-names \
	server-cn-only server-single-alt-name server-multiple-alt-names \
	server-no-names server-revoked
CLIENTS := client client-dn client-revoked

#
# To add a new non-standard key, add it to SPECIAL_KEYS and then add a recipe
# for creating it to the "Special-case keys" section below.
#
SPECIAL_KEYS := ssl/server-password.key \
	ssl/client-der.key ssl/client-encrypted-pem.key ssl/client-encrypted-der.key

#
# These files are just concatenations of other files. You can add new ones to
# COMBINATIONS here, then declare the constituent files as dependencies in the
# "Combined files" section below.
#
COMBINATIONS := \
	ssl/both-cas-1.crt ssl/both-cas-2.crt \
	ssl/root+server_ca.crt ssl/root+server.crl \
	ssl/root+client_ca.crt ssl/root+client.crl \
	ssl/client+client_ca.crt

CERTIFICATES := root_ca server_ca client_ca $(SERVERS) $(CLIENTS)
STANDARD_CERTS := $(CERTIFICATES:%=ssl/%.crt)
STANDARD_KEYS := $(CERTIFICATES:%=ssl/%.key)
CRLS := ssl/root.crl ssl/client.crl ssl/server.crl

SSLFILES := $(STANDARD_CERTS) $(STANDARD_KEYS) $(SPECIAL_KEYS) $(COMBINATIONS) $(CRLS)
SSLDIRS := ssl/client-crldir ssl/server-crldir \
	ssl/root+client-crldir ssl/root+server-crldir

# This target re-generates all the key and certificate files. Usually we just
# use the ones that are committed to the tree without rebuilding them.
#
.PHONY: sslfiles
sslfiles: $(SSLFILES) $(SSLDIRS)

#
# Special-case certificates
#

# Root CA is self-signed.
ssl/root_ca.crt: ssl/root_ca.key root_ca.config
	openssl req -new -x509 -config root_ca.config -days 10000 -key $< -out $@

#
# Special-case keys
#
# All targets here are contained in $(SPECIAL_KEYS).
#

# Password-protected version of server-cn-only.key
ssl/server-password.key: ssl/server-cn-only.key
	openssl rsa -aes256 -in $< -out $@ -passout 'pass:secret1'

# DER-encoded version of client.key
ssl/client-der.key: ssl/client.key
	openssl rsa -in $< -outform DER -out $@

# Convert client.key to encrypted PEM (X.509 text) and DER (X.509 ASN.1) formats
# to test libpq's support for the sslpassword= option.
ssl/client-encrypted-pem.key: outform := PEM
ssl/client-encrypted-der.key: outform := DER
ssl/client-encrypted-pem.key ssl/client-encrypted-der.key: ssl/client.key
	openssl rsa -in $< -outform $(outform) -aes128 -passout 'pass:dUmmyP^#+' -out $@

#
# Combined files
#
# All targets in $(COMBINATIONS) share a single recipe; just declare the
# necessary dependencies and they'll be smashed together.
#

# Root certificate file that contains both CA certificates, for testing
# that multiple certificates can be used.
ssl/both-cas-1.crt: ssl/root_ca.crt ssl/client_ca.crt ssl/server_ca.crt

# The same, but the certs are in different order
ssl/both-cas-2.crt: ssl/root_ca.crt ssl/server_ca.crt ssl/client_ca.crt

# A root certificate file for the client, to validate server certs.
ssl/root+server_ca.crt: ssl/root_ca.crt ssl/server_ca.crt

# and for the server, to validate client certs
ssl/root+client_ca.crt: ssl/root_ca.crt ssl/client_ca.crt

# and for the client, to present to the server
ssl/client+client_ca.crt: ssl/client.crt ssl/client_ca.crt

# If a CRL is used, OpenSSL requires a CRL file for *all* the CAs in the
# chain, even if some of them are empty.
ssl/root+server.crl: ssl/root.crl ssl/server.crl
ssl/root+client.crl: ssl/root.crl ssl/client.crl

$(COMBINATIONS):
	cat $^ > $@

#
# Standard keys
#

$(STANDARD_KEYS):
	openssl genrsa -out $@ 2048
	chmod 0600 $@

#
# Standard certificates
#

SERVER_CERTS := $(SERVERS:%=ssl/%.crt)
CLIENT_CERTS := $(CLIENTS:%=ssl/%.crt)

ssl/server_ca.crt ssl/client_ca.crt: ca := root_ca
$(SERVER_CERTS): ca := server_ca
$(CLIENT_CERTS): ca := client_ca

ca_state_files = ssl/$(ca)-certindex ssl/$(ca)-certindex.attr ssl/$(ca).srl

# This is the workhorse recipe. `openssl ca` can't be safely run from parallel
# processes, so we must mark the entire Makefile .NOTPARALLEL. Enable
# .SECONDEXPANSION so that the correct dependencies are determined by the value
# of $(ca).
.NOTPARALLEL:
.SECONDEXPANSION: $(STANDARD_CERTS)
ssl/%.crt: ssl/%.csr %.config ssl/$$(ca).crt | ssl/new_certs_dir $$(ca_state_files)
	openssl ca -batch -config cas.config -name $(ca) -notext -in $< -out $@

ssl/%.csr: ssl/%.key %.config
	openssl req -new -key $< -out $@ -config $*.config

#
# CA State
#
# All of these are intended to be order-only dependencies; additionally, the
# pattern recipes are intended to be intermediates, so do not name them
# explicitly somewhere else in the Makefile. The goal is for Make to create the
# state files once for each CA, allow them to accumulate whatever state is
# needed, and then automatically remove them at the end of the run.
#

# OpenSSL requires a directory to put all generated certificates in. We don't
# use this for anything, but we need a location.
ssl/new_certs_dir:
	mkdir $@

ssl/%-certindex:
	touch $@

ssl/%-certindex.attr:
	echo "unique_subject=no" > $@

# The first serial number for each CA is based on the current timestamp, to
# avoid collisions across Make runs.
ssl/%.srl:
	date +%Y%m%d%H%M%S00 > $@

#
# CRLs
#

ssl/root.crl: revoked :=
ssl/root.crl: ca := root_ca

ssl/client.crl: revoked := ssl/client-revoked.crt
ssl/client.crl: ca := client_ca

ssl/server.crl: revoked := ssl/server-revoked.crt
ssl/server.crl: ca := server_ca

.SECONDEXPANSION: $(CRLS)
ssl/%.crl: ssl/$$(ca).crt $$(revoked) | $$(ca_state_files)
	$(foreach cert,$(revoked),openssl ca -config cas.config -name $(ca) -revoke $(cert) &&) true
	openssl ca -config cas.config -name $(ca) -gencrl -out $@

#
# CRL hash directories
#

ssl/root+server-crldir: ssl/server.crl ssl/root.crl
ssl/root+client-crldir: ssl/client.crl ssl/root.crl
ssl/server-crldir: ssl/server.crl
ssl/client-crldir: ssl/client.crl

crlhashfile = $(shell openssl crl -hash -noout -in $(1)).r0

ssl/%-crldir:
	mkdir -p $@
	rm -f $@/*.r0
	$(foreach crl,$^,cp $(crl) $@/$(call crlhashfile,$(crl)) &&) true
	touch $@

.PHONY: sslfiles-clean
sslfiles-clean:
	rm -f $(SSLFILES) ssl/*.old ssl/*.csr ssl/*.srl ssl/*-certindex*
	rm -rf $(SSLDIRS) ssl/new_certs_dir

clean distclean maintainer-clean:
	rm -rf tmp_check
	rm -rf ssl/*.old ssl/new_certs_dir ssl/client*_tmp.key

# Doesn't depend on $(SSLFILES) because we don't rebuild them by default
check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
