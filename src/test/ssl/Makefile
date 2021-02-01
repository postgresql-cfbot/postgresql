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
include $(top_builddir)/src/Makefile.global

export with_ssl

CERTIFICATES := server_ca server-cn-and-alt-names \
	server-cn-only server-single-alt-name server-multiple-alt-names \
	server-no-names server-revoked server-ss \
	client_ca client client-revoked \
	root_ca

SSLFILES := $(CERTIFICATES:%=ssl/%.key) $(CERTIFICATES:%=ssl/%.crt) \
	ssl/server-password.key \
	ssl/client.crl ssl/server.crl ssl/root.crl \
	ssl/both-cas-1.crt ssl/both-cas-2.crt \
	ssl/root+server_ca.crt ssl/root+server.crl \
	ssl/root+client_ca.crt ssl/root+client.crl \
	ssl/client+client_ca.crt ssl/client-der.key \
	ssl/client-encrypted-pem.key ssl/client-encrypted-der.key

# Even though we in practice could get away with far fewer NSS databases, they
# are generated to mimic the setup for the OpenSSL tests in order to ensure
# we isolate the same behavior between the backends. The database name should
# contain the files included for easier test suite code reading.
NSSFILES := ssl/nss/client_ca.crt.db \
	ssl/nss/server_ca.crt.db \
	ssl/nss/root+server_ca.crt.db \
	ssl/nss/root+client_ca.crt.db \
	ssl/nss/client.crt__client.key.db \
	ssl/nss/client-revoked.crt__client-revoked.key.db \
	ssl/nss/server-cn-only.crt__server-password.key.db \
	ssl/nss/server-cn-only.crt__server-cn-only.key.db \
	ssl/nss/root.crl \
	ssl/nss/server.crl \
	ssl/nss/client.crl \
	ssl/nss/server-multiple-alt-names.crt__server-multiple-alt-names.key.db \
	ssl/nss/server-single-alt-name.crt__server-single-alt-name.key.db \
	ssl/nss/server-cn-and-alt-names.crt__server-cn-and-alt-names.key.db \
	ssl/nss/server-no-names.crt__server-no-names.key.db \
	ssl/nss/server-revoked.crt__server-revoked.key.db \
	ssl/nss/root+client.crl \
	ssl/nss/client+client_ca.crt__client.key.db \
	ssl/nss/client.crt__client-encrypted-pem.key.db \
	ssl/nss/root+server_ca.crt__server.crl.db \
	ssl/nss/root+server_ca.crt__root+server.crl.db \
	ssl/nss/native_ca-root.db \
	ssl/nss/native_server-root.db \
	ssl/nss/native_client-root.db

# This target re-generates all the key and certificate files. Usually we just
# use the ones that are committed to the tree without rebuilding them.
#
# This target will fail unless preceded by sslfiles-clean.
#
sslfiles: $(SSLFILES)

# Generate NSS certificate databases corresponding to the OpenSSL certificates.
# This target will fail unless preceded by nssfiles-clean.
nssfiles: $(NSSFILES)

# OpenSSL requires a directory to put all generated certificates in. We don't
# use this for anything, but we need a location.
ssl/new_certs_dir:
	mkdir ssl/new_certs_dir

# Rule for creating private/public key pairs.
ssl/%.key:
	openssl genrsa -out $@ 2048
	chmod 0600 $@

# Root CA certificate
ssl/root_ca.crt: ssl/root_ca.key cas.config
	touch ssl/root_ca-certindex
	openssl req -new -out ssl/root_ca.crt -x509 -config cas.config -config root_ca.config -key ssl/root_ca.key -days 10000 -extensions v3_ca
	echo "01" > ssl/root_ca.srl

# Client and server CAs
ssl/%_ca.crt: ssl/%_ca.key %_ca.config ssl/root_ca.crt ssl/new_certs_dir
	touch ssl/$*_ca-certindex
	echo "unique_subject=no" > ssl/$*_ca-certindex.attr
	openssl req -new -out ssl/temp_ca.crt -config cas.config -config $*_ca.config -key ssl/$*_ca.key
# Sign the certificate with the root CA
	openssl ca -name root_ca -batch -config cas.config -in ssl/temp_ca.crt -out ssl/temp_ca_signed.crt -extensions v3_ca
	openssl x509 -in ssl/temp_ca_signed.crt -out ssl/$*_ca.crt # to keep just the PEM cert
	rm ssl/temp_ca.crt ssl/temp_ca_signed.crt
	echo "01" > ssl/$*_ca.srl

ssl/nss/%_ca.crt.db: ssl/%_ca.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n $*_ca.crt -i ssl/$*_ca.crt -t "CT,C,C"

ssl/nss/root+server_ca.crt__server.crl.db: ssl/root+server_ca.crt ssl/nss/server.crl
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/root+server_ca.crt -i ssl/root+server_ca.crt -t "CT,C,C"
	crlutil -I -i ssl/nss/server.crl -d $@ -B

ssl/nss/root+server_ca.crt__root+server.crl.db: ssl/root+server_ca.crt ssl/nss/root.crl ssl/nss/server.crl
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/root+server_ca.crt -i ssl/root+server_ca.crt -t "CT,C,C"
	crlutil -I -i ssl/nss/root.crl -d $@ -B
	crlutil -I -i ssl/nss/server.crl -d $@ -B

# Server certificates, signed by server CA:
ssl/server-%.crt: ssl/server-%.key ssl/server_ca.crt server-%.config
	openssl req -new -key ssl/server-$*.key -out ssl/server-$*.csr -config server-$*.config
	openssl ca -name server_ca -batch -config cas.config -in ssl/server-$*.csr -out ssl/temp.crt  -extensions v3_req -extfile server-$*.config
	openssl x509 -in ssl/temp.crt -out ssl/server-$*.crt # to keep just the PEM cert
	rm ssl/server-$*.csr

# Self-signed version of server-cn-only.crt
ssl/server-ss.crt: ssl/server-cn-only.key ssl/server-cn-only.crt server-cn-only.config
	openssl req -new -key ssl/server-cn-only.key -out ssl/server-ss.csr -config server-cn-only.config
	openssl x509 -req -days 10000 -in ssl/server-ss.csr -signkey ssl/server-cn-only.key -out ssl/server-ss.crt  -extensions v3_req -extfile server-cn-only.config
	rm ssl/server-ss.csr

# pk12util won't preserve the password when importing the password protected
# key, the password must be set on the database *before* importing it as the
# password in the pkcs12 envelope will be dropped.
ssl/nss/server-cn-only.crt__server-password.key.db: ssl/server-cn-only.crt
	$(MKDIR_P) $@
	echo "secret1" > password.txt
	certutil -d "sql:$@" -N -f password.txt
	certutil -d "sql:$@" -A -n ssl/server-cn-only.crt -i ssl/server-cn-only.crt -t "CT,C,C" -f password.txt
	certutil -d "sql:$@" -A -n server_ca.crt -i ssl/server_ca.crt -t "CT,C,C" -f password.txt
	certutil -d "sql:$@" -A -n root_ca.crt -i ssl/root_ca.crt -t "CT,C,C" -f password.txt
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C" -f password.txt
	openssl pkcs12 -export -out ssl/nss/server-password.pfx -inkey ssl/server-password.key -in ssl/server-cn-only.crt -certfile ssl/server_ca.crt -passin 'pass:secret1' -passout 'pass:secret1'
	pk12util -i ssl/nss/server-password.pfx -d "sql:$@" -W 'secret1' -K 'secret1'

ssl/nss/server-cn-only.crt__server-cn-only.key.db: ssl/server-cn-only.crt ssl/server-cn-only.key
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/server-cn-only.crt -i ssl/server-cn-only.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n server_ca.crt -i ssl/server_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/server-cn-only.pfx -inkey ssl/server-cn-only.key -in ssl/server-cn-only.crt -certfile ssl/server_ca.crt -passout pass:
	pk12util -i ssl/nss/server-cn-only.pfx -d "sql:$@" -W ''

ssl/nss/server-multiple-alt-names.crt__server-multiple-alt-names.key.db: ssl/server-multiple-alt-names.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/server-multiple-alt-names.crt -i ssl/server-multiple-alt-names.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n server_ca.crt -i ssl/server_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n root_ca.crt -i ssl/root_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/server-multiple-alt-names.pfx -inkey ssl/server-multiple-alt-names.key -in ssl/server-multiple-alt-names.crt -certfile ssl/server-multiple-alt-names.crt -passout pass:
	pk12util -i ssl/nss/server-multiple-alt-names.pfx -d "sql:$@" -W ''

ssl/nss/server-single-alt-name.crt__server-single-alt-name.key.db: ssl/server-single-alt-name.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/server-single-alt-name.crt -i ssl/server-single-alt-name.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n server_ca.crt -i ssl/server_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n root_ca.crt -i ssl/root_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/server-single-alt-name.pfx -inkey ssl/server-single-alt-name.key -in ssl/server-single-alt-name.crt -certfile ssl/server-single-alt-name.crt -passout pass:
	pk12util -i ssl/nss/server-single-alt-name.pfx -d "sql:$@" -W ''

ssl/nss/server-cn-and-alt-names.crt__server-cn-and-alt-names.key.db: ssl/server-cn-and-alt-names.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/server-cn-and-alt-names.crt -i ssl/server-cn-and-alt-names.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n server_ca.crt -i ssl/server_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n root_ca.crt -i ssl/root_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/server-cn-and-alt-names.pfx -inkey ssl/server-cn-and-alt-names.key -in ssl/server-cn-and-alt-names.crt -certfile ssl/server-cn-and-alt-names.crt -passout pass:
	pk12util -i ssl/nss/server-cn-and-alt-names.pfx -d $@ -W ''

ssl/nss/server-no-names.crt__server-no-names.key.db: ssl/server-no-names.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/server-no-names.crt -i ssl/server-no-names.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n server_ca.crt -i ssl/server_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n root_ca.crt -i ssl/root_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/server-no-names.pfx -inkey ssl/server-no-names.key -in ssl/server-no-names.crt -certfile ssl/server-no-names.crt -passout pass:
	pk12util -i ssl/nss/server-no-names.pfx -d "sql:$@" -W ''

ssl/nss/server-revoked.crt__server-revoked.key.db: ssl/server-revoked.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/server-revoked.crt -i ssl/server-revoked.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n server_ca.crt -i ssl/server_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n root_ca.crt -i ssl/root_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/server-revoked.pfx -inkey ssl/server-revoked.key -in ssl/server-revoked.crt -certfile ssl/server-revoked.crt -passout pass:
	pk12util -i ssl/nss/server-revoked.pfx -d "sql:$@" -W ''

# Password-protected version of server-cn-only.key
ssl/server-password.key: ssl/server-cn-only.key
	openssl rsa -aes256 -in $< -out $@ -passout 'pass:secret1'

# Client certificate, signed by the client CA:
ssl/client.crt: ssl/client.key ssl/client_ca.crt
	openssl req -new -key ssl/client.key -out ssl/client.csr -config client.config
	openssl ca -name client_ca -batch -out ssl/temp.crt -config cas.config -infiles ssl/client.csr
	openssl x509 -in ssl/temp.crt -out ssl/client.crt # to keep just the PEM cert
	rm ssl/client.csr ssl/temp.crt

# Client certificate, signed by client CA
ssl/nss/client.crt__client.key.db: ssl/client.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/client.crt -i ssl/client.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n root+client_ca.crt -i ssl/root+client_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/client.pfx -inkey ssl/client.key -in ssl/client.crt -certfile ssl/client_ca.crt -passout pass:
	pk12util -i ssl/nss/client.pfx -d "sql:$@" -W ''

# Client certificate with encrypted key, signed by client CA
ssl/nss/client.crt__client-encrypted-pem.key.db: ssl/client.crt
	$(MKDIR_P) $@
	echo 'dUmmyP^#+' > $@.pass
	certutil -d "sql:$@" -N -f $@.pass
	certutil -d "sql:$@" -A -f $@.pass -n ssl/client.crt -i ssl/client.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -f $@.pass -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -f $@.pass -n root+server_ca.crt -i ssl/root+server_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/client-encrypted-pem.pfx -inkey ssl/client-encrypted-pem.key -in ssl/client.crt -certfile ssl/client_ca.crt -passin pass:'dUmmyP^#+' -passout pass:'dUmmyP^#+'
	pk12util -i ssl/nss/client-encrypted-pem.pfx -d "sql:$@" -W 'dUmmyP^#+' -k $@.pass

# Another client certificate, signed by the client CA. This one is revoked.
ssl/client-revoked.crt: ssl/client-revoked.key ssl/client_ca.crt client.config
	openssl req -new -key ssl/client-revoked.key -out ssl/client-revoked.csr -config client.config
	openssl ca -name client_ca -batch -out ssl/temp.crt -config cas.config -infiles ssl/client-revoked.csr
	openssl x509 -in ssl/temp.crt -out ssl/client-revoked.crt # to keep just the PEM cert
	rm ssl/client-revoked.csr ssl/temp.crt

ssl/nss/client-revoked.crt__client-revoked.key.db: ssl/client-revoked.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/client-revoked.crt -i ssl/client-revoked.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n client_ca.crt -i ssl/client_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/client-revoked.pfx -inkey ssl/client-revoked.key -in ssl/client-revoked.crt -certfile ssl/client_ca.crt -passout pass:
	pk12util -i ssl/nss/client-revoked.pfx -d "sql:$@" -W ''

# Convert the key to DER, to test our behaviour there too
ssl/client-der.key: ssl/client.key
	openssl rsa -in ssl/client.key -outform DER -out ssl/client-der.key

# Convert the existing key to encrypted PEM (X.509 text) and DER (X.509 ASN.1) formats
# to test libpq's support for the sslpassword= option.
ssl/client-encrypted-pem.key: ssl/client.key
	openssl rsa -in ssl/client.key -outform PEM -aes128 -passout 'pass:dUmmyP^#+' -out ssl/client-encrypted-pem.key

ssl/client-encrypted-der.key: ssl/client.key
	openssl rsa -in ssl/client.key -outform DER -aes128 -passout 'pass:dUmmyP^#+' -out ssl/client-encrypted-der.key

# Root certificate files that contains both CA certificates, for testing
# that multiple certificates can be used.
ssl/both-cas-1.crt: ssl/root_ca.crt ssl/client_ca.crt ssl/server_ca.crt
	cat $^ > $@

# The same, but the certs are in different order
ssl/both-cas-2.crt: ssl/root_ca.crt ssl/server_ca.crt ssl/client_ca.crt
	cat $^ > $@

# A root certificate file for the client, to validate server certs.
ssl/root+server_ca.crt: ssl/root_ca.crt ssl/server_ca.crt
	cat $^ > $@

# and for the server, to validate client certs
ssl/root+client_ca.crt: ssl/root_ca.crt ssl/client_ca.crt
	cat $^ > $@

ssl/client+client_ca.crt: ssl/client.crt ssl/client_ca.crt
	cat $^ > $@

# Client certificate, signed by client CA
ssl/nss/client+client_ca.crt__client.key.db: ssl/client+client_ca.crt
	$(MKDIR_P) $@
	certutil -d "sql:$@" -N --empty-password
	certutil -d "sql:$@" -A -n ssl/client+client_ca.crt -i ssl/client+client_ca.crt -t "CT,C,C"
	certutil -d "sql:$@" -A -n ssl/root+server_ca.crt -i ssl/root+server_ca.crt -t "CT,C,C"
	openssl pkcs12 -export -out ssl/nss/client.pfx -inkey ssl/client.key -in ssl/client.crt -certfile ssl/client_ca.crt -passout pass:
	pk12util -i ssl/nss/client.pfx -d "sql:$@" -W ''

#### CRLs

ssl/client.crl: ssl/client-revoked.crt
	openssl ca -config cas.config -name client_ca -revoke ssl/client-revoked.crt
	openssl ca -config cas.config -name client_ca -gencrl -out ssl/client.crl

ssl/nss/client.crl: ssl/client.crl
	openssl crl -in $^ -outform der -out $@

ssl/server.crl: ssl/server-revoked.crt
	openssl ca -config cas.config -name server_ca -revoke ssl/server-revoked.crt
	openssl ca -config cas.config -name server_ca -gencrl -out ssl/server.crl

ssl/nss/server.crl: ssl/server.crl
	openssl crl -in $^ -outform der -out $@

ssl/root.crl: ssl/root_ca.crt
	openssl ca -config cas.config -name root_ca -gencrl -out ssl/root.crl

ssl/nss/root.crl: ssl/root.crl
	openssl crl -in $^ -outform der -out $@

ssl/nss/root+client.crl: ssl/root+client.crl
	openssl crl -in $^ -outform der -out $@

# If a CRL is used, OpenSSL requires a CRL file for *all* the CAs in the
# chain, even if some of them are empty.
ssl/root+server.crl: ssl/root.crl ssl/server.crl
	cat $^ > $@
ssl/root+client.crl: ssl/root.crl ssl/client.crl
	cat $^ > $@

#### NSS specific certificates and keys

#NSSFILES2 := ssl/nss/native_ca-root.db \
#	ssl/nss/native_server-root.db \
#	ssl/nss/native_client-root.db

#nssfiles2: $(NSSFILES2)

ssl/nss/native_ca-%.db:
	$(MKDIR_P) ssl/nss/native_ca-$*.db
	certutil -N -d "sql:ssl/nss/native_ca-$*.db/" --empty-password
	echo y > nss_ca_params.txt
	echo 10 >> nss_ca_params.txt
	echo y >> nss_ca_params.txt
	cat nss_ca_params.txt | certutil -S -d "sql:ssl/nss/native_ca-$*.db/" -n ca-$* \
	-s "CN=Test CA for PostgreSQL SSL regression tests,OU=PostgreSQL test suite" \
	-x -k rsa -g 2048 -m 5432 -t CTu,CTu,CTu \
	--keyUsage certSigning -2 --nsCertType sslCA,smimeCA,objectSigningCA \
	-z Makefile -Z SHA256
	rm nss_ca_params.txt

ssl/nss/native_ca-%.pem: ssl/nss/native_ca-%.db
	certutil -L -d "sql:ssl/nss/native_ca-$*.db/" -n ca-$* -a > ssl/nss/native_ca-$*.pem

# Create and sign a server certificate
ssl/nss/native_server-%.db: ssl/nss/native_ca-%.pem
	$(MKDIR_P) ssl/nss/native_server-$*.db
	certutil -N -d "sql:ssl/nss/native_server-$*.db/" --empty-password
	certutil -R -d "sql:ssl/nss/native_server-$*.db/" \
		-s "CN=common-name.pg-ssltest.test,OU=PostgreSQL test suite" \
		-o ssl/nss/native_server-$*.csr -g 2048 -Z SHA256 -z Makefile
	echo 1 > nss_server_params.txt
	echo 9 >> nss_server_params.txt
	cat nss_server_params.txt | certutil -C -d "sql:ssl/nss/native_ca-$*.db/" -c ca-root -i ssl/nss/native_server-$*.csr \
		-o ssl/nss/native_server_$*.der -m 5433 --keyUsage dataEncipherment,digitalSignature,keyEncipherment \
		--nsCertType sslServer --certVersion 1 -Z SHA256
	certutil -A -d "sql:ssl/nss/native_server-$*.db/" -n ca-$* -t CTu,CTu,CTu -a -i ssl/nss/native_ca-$*.pem
	certutil -A -d "sql:ssl/nss/native_server-$*.db/" -n ssl/native_server-$*.crt -t CTu,CTu,CTu -i ssl/nss/native_server_$*.der
	rm nss_server_params.txt

# Create and sign a client certificate
ssl/nss/native_client-%.db: ssl/nss/native_ca-%.pem
	$(MKDIR_P) ssl/nss/native_client-$*.db
	certutil -N -d "sql:ssl/nss/native_client-$*.db/" --empty-password
	certutil -R -d "sql:ssl/nss/native_client-$*.db/" -s "CN=ssltestuser,OU=PostgreSQL test suite" \
		-o ssl/nss/native_client-$*.csr -g 2048 -Z SHA256 -z Makefile
	certutil -C -d "sql:ssl/nss/native_ca-$*.db/" -c ca-$* -i ssl/nss/native_client-$*.csr -o ssl/nss/native_client-$*.der \
		-m 5434 --keyUsage keyEncipherment,dataEncipherment,digitalSignature --nsCertType sslClient \
		--certVersion 1 -Z SHA256
	certutil -A -d "sql:ssl/nss/native_client-$*.db" -n ca-$* -t CTu,CTu,CTu -a -i ssl/nss/native_ca-$*.pem
	certutil -A -d "sql:ssl/nss/native_client-$*.db" -n native_client-$* -t CTu,CTu,CTu -i ssl/nss/native_client-$*.der

nssfiles2clean:
	rm -rf ssl/nss

.PHONY: sslfiles-clean
sslfiles-clean:
	rm -f $(SSLFILES) ssl/client_ca.srl ssl/server_ca.srl ssl/client_ca-certindex* ssl/server_ca-certindex* ssl/root_ca-certindex* ssl/root_ca.srl ssl/temp_ca.crt ssl/temp_ca_signed.crt

.PHONY: nssfiles-clean
nssfiles-clean:
	rm -rf ssl/nss

clean distclean maintainer-clean:
	rm -rf tmp_check
	rm -rf ssl/*.old ssl/new_certs_dir ssl/client*_tmp.key
	rm -rf ssl/nss

# Doesn't depend on $(SSLFILES) because we don't rebuild them by default
check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
