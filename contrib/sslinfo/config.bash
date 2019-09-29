#!/bin/bash

function sslinfo_prepare() {

echo "#BEGIN SSLINFO CONF : BEGIN ANCHOR##" >> $PGDATA/postgresql.conf
echo "ssl=on" >> $PGDATA/postgresql.conf
echo "ssl_ciphers='HIGH:MEDIUM:+3DES:!aNULL'" >> $PGDATA/postgresql.conf
echo "ssl_cert_file='server.crt'" >> $PGDATA/postgresql.conf
echo "ssl_key_file='server.key'" >> $PGDATA/postgresql.conf
echo "ssl_ca_file='root.crt'" >> $PGDATA/postgresql.conf
echo "#END SSLINFO CONF : END ANCHOR##" >> $PGDATA/postgresql.conf

echo "preparing CRTs and KEYs"
cp -f data/root.crt   $PGDATA/
cp -f data/server.crt $PGDATA/
cp -f data/server.key $PGDATA/
chmod 400 $PGDATA/server.key
chmod 644 $PGDATA/server.crt
chmod 644 $PGDATA/root.crt

mkdir -p ~/.postgresql
cp -f data/root.crt         ~/.postgresql/
cp -f data/postgresql.crt   ~/.postgresql/
cp -f data/postgresql.key   ~/.postgresql/
chmod 400 ~/.postgresql/postgresql.key
chmod 644 ~/.postgresql/postgresql.crt
chmod 644 ~/.postgresql/root.crt
}

function sslinfo_clean() {
sed -i '/#BEGIN SSLINFO CONF : BEGIN ANCHOR##/,/#END SSLINFO CONF : END ANCHOR##/d' $PGDATA/postgresql.conf
}

case "$1" in
prepare)
    sslinfo_prepare
    ;;
clean)
    sslinfo_clean
    ;;
*)
    echo "$0 { prepare | clean }"
    exit 1
esac
