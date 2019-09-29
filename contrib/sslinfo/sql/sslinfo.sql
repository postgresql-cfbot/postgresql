\! bash config.bash prepare
-- start_ignore
\! pg_ctl restart 2>&1 >/dev/null
-- end_ignore
\! echo "restart code = $?"

\c - - localhost

CREATE EXTENSION sslinfo;

SHOW ssl;
SELECT ssl_is_used();
SELECT ssl_version();
SELECT ssl_cipher();
SELECT ssl_client_cert_present();
SELECT ssl_client_serial();
SELECT ssl_client_dn();
SELECT ssl_issuer_dn();
SELECT ssl_client_dn_field('CN') AS client_dn_CN;
SELECT ssl_client_dn_field('C') AS client_dn_C;
SELECT ssl_client_dn_field('ST') AS client_dn_ST;
SELECT ssl_client_dn_field('L') AS client_dn_L;
SELECT ssl_client_dn_field('O') AS client_dn_O;
SELECT ssl_client_dn_field('OU') AS client_dn_OU;
SELECT ssl_issuer_field('CN') AS issuer_CN;
SELECT ssl_issuer_field('C') AS issuer_C;
SELECT ssl_issuer_field('ST') AS issuer_ST;
SELECT ssl_issuer_field('L') AS issuer_L;
SELECT ssl_issuer_field('O') AS issuer_O;
SELECT ssl_issuer_field('OU') AS issuer_OU;

DROP EXTENSION sslinfo;

-- start_ignore
\! bash config.bash clean
\! pg_ctl restart 2>&1 >/dev/null
-- end_ignore
\! echo "restart code = $?"
