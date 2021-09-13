-- Superuser DBA
CREATE ROLE regress_admin SUPERUSER;
-- Perform all operations as user 'regress_admin' --
SET SESSION AUTHORIZATION regress_admin;
-- PGC_BACKEND / DEVELOPER_OPTIONS
SET ignore_system_indexes = OFF;  -- fail, cannot be set after connection start
RESET ignore_system_indexes;  -- fail, cannot be set after connection start
ALTER SYSTEM SET ignore_system_indexes = OFF;  -- ok
ALTER SYSTEM RESET ignore_system_indexes;  -- ok
SET post_auth_delay = 50;  -- fail, cannot be set after connection start
RESET post_auth_delay;  -- fail, cannot be set after connection start
ALTER SYSTEM SET post_auth_delay = 50;  -- ok
ALTER SYSTEM RESET post_auth_delay;  -- ok
-- PGC_INTERNAL / PRESET_OPTIONS
SET block_size = 50;  -- fail, cannot be changed
RESET block_size;  -- fail, cannot be changed
ALTER SYSTEM SET block_size = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET block_size;  -- fail, cannot be changed
SET data_checksums = OFF;  -- fail, cannot be changed
RESET data_checksums;  -- fail, cannot be changed
ALTER SYSTEM SET data_checksums = OFF;  -- fail, cannot be changed
ALTER SYSTEM RESET data_checksums;  -- fail, cannot be changed
SET data_directory_mode = 50;  -- fail, cannot be changed
RESET data_directory_mode;  -- fail, cannot be changed
ALTER SYSTEM SET data_directory_mode = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET data_directory_mode;  -- fail, cannot be changed
SET debug_assertions = OFF;  -- fail, cannot be changed
RESET debug_assertions;  -- fail, cannot be changed
ALTER SYSTEM SET debug_assertions = OFF;  -- fail, cannot be changed
ALTER SYSTEM RESET debug_assertions;  -- fail, cannot be changed
SET in_hot_standby = OFF;  -- fail, cannot be changed
RESET in_hot_standby;  -- fail, cannot be changed
ALTER SYSTEM SET in_hot_standby = OFF;  -- fail, cannot be changed
ALTER SYSTEM RESET in_hot_standby;  -- fail, cannot be changed
SET integer_datetimes = OFF;  -- fail, cannot be changed
RESET integer_datetimes;  -- fail, cannot be changed
ALTER SYSTEM SET integer_datetimes = OFF;  -- fail, cannot be changed
ALTER SYSTEM RESET integer_datetimes;  -- fail, cannot be changed
SET lc_collate = 'en_US.UTF-8';  -- fail, cannot be changed
RESET lc_collate;  -- fail, cannot be changed
ALTER SYSTEM SET lc_collate = 'en_US.UTF-8';  -- fail, cannot be changed
ALTER SYSTEM RESET lc_collate;  -- fail, cannot be changed
SET lc_ctype = 'en_US.UTF-8';  -- fail, cannot be changed
RESET lc_ctype;  -- fail, cannot be changed
ALTER SYSTEM SET lc_ctype = 'en_US.UTF-8';  -- fail, cannot be changed
ALTER SYSTEM RESET lc_ctype;  -- fail, cannot be changed
SET max_function_args = 50;  -- fail, cannot be changed
RESET max_function_args;  -- fail, cannot be changed
ALTER SYSTEM SET max_function_args = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET max_function_args;  -- fail, cannot be changed
SET max_identifier_length = 50;  -- fail, cannot be changed
RESET max_identifier_length;  -- fail, cannot be changed
ALTER SYSTEM SET max_identifier_length = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET max_identifier_length;  -- fail, cannot be changed
SET max_index_keys = 50;  -- fail, cannot be changed
RESET max_index_keys;  -- fail, cannot be changed
ALTER SYSTEM SET max_index_keys = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET max_index_keys;  -- fail, cannot be changed
SET segment_size = 50;  -- fail, cannot be changed
RESET segment_size;  -- fail, cannot be changed
ALTER SYSTEM SET segment_size = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET segment_size;  -- fail, cannot be changed
SET server_encoding = 'UTF8';  -- fail, cannot be changed
RESET server_encoding;  -- fail, cannot be changed
ALTER SYSTEM SET server_encoding = 'UTF8';  -- fail, cannot be changed
ALTER SYSTEM RESET server_encoding;  -- fail, cannot be changed
SET server_version = '9.1';  -- fail, cannot be changed
RESET server_version;  -- fail, cannot be changed
ALTER SYSTEM SET server_version = '9.1';  -- fail, cannot be changed
ALTER SYSTEM RESET server_version;  -- fail, cannot be changed
SET server_version_num = 50;  -- fail, cannot be changed
RESET server_version_num;  -- fail, cannot be changed
ALTER SYSTEM SET server_version_num = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET server_version_num;  -- fail, cannot be changed
SET ssl_library = 'OpenSSL';  -- fail, cannot be changed
RESET ssl_library;  -- fail, cannot be changed
ALTER SYSTEM SET ssl_library = 'OpenSSL';  -- fail, cannot be changed
ALTER SYSTEM RESET ssl_library;  -- fail, cannot be changed
SET wal_block_size = 50;  -- fail, cannot be changed
RESET wal_block_size;  -- fail, cannot be changed
ALTER SYSTEM SET wal_block_size = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET wal_block_size;  -- fail, cannot be changed
SET wal_segment_size = 50;  -- fail, cannot be changed
RESET wal_segment_size;  -- fail, cannot be changed
ALTER SYSTEM SET wal_segment_size = 50;  -- fail, cannot be changed
ALTER SYSTEM RESET wal_segment_size;  -- fail, cannot be changed
-- PGC_INTERNAL / UNGROUPED
SET is_superuser = OFF;  -- fail, cannot be changed
RESET is_superuser;  -- fail, cannot be changed
ALTER SYSTEM SET is_superuser = OFF;  -- fail, cannot be changed
ALTER SYSTEM RESET is_superuser;  -- fail, cannot be changed
-- PGC_POSTMASTER / AUTOVACUUM
SET autovacuum_freeze_max_age = 1000050000;  -- fail, requires restart
RESET autovacuum_freeze_max_age;  -- fail, requires restart
ALTER SYSTEM SET autovacuum_freeze_max_age = 1000050000;  -- ok
ALTER SYSTEM RESET autovacuum_freeze_max_age;  -- ok
SET autovacuum_max_workers = 50;  -- fail, requires restart
RESET autovacuum_max_workers;  -- fail, requires restart
ALTER SYSTEM SET autovacuum_max_workers = 50;  -- ok
ALTER SYSTEM RESET autovacuum_max_workers;  -- ok
SET autovacuum_multixact_freeze_max_age = 1000005000;  -- fail, requires restart
RESET autovacuum_multixact_freeze_max_age;  -- fail, requires restart
ALTER SYSTEM SET autovacuum_multixact_freeze_max_age = 1000005000;  -- ok
ALTER SYSTEM RESET autovacuum_multixact_freeze_max_age;  -- ok
-- PGC_POSTMASTER / CLIENT_CONN_PRELOAD
SET jit_provider = 'llvmjit';  -- fail, requires restart
RESET jit_provider;  -- fail, requires restart
ALTER SYSTEM SET jit_provider = 'llvmjit';  -- ok
ALTER SYSTEM RESET jit_provider;  -- ok
SET shared_preload_libraries = 'iconv, pcre';  -- fail, requires restart
RESET shared_preload_libraries;  -- fail, requires restart
ALTER SYSTEM SET shared_preload_libraries = 'iconv, pcre';  -- ok
ALTER SYSTEM RESET shared_preload_libraries;  -- ok
-- PGC_POSTMASTER / CONN_AUTH_SETTINGS
SET bonjour = OFF;  -- fail, requires restart
RESET bonjour;  -- fail, requires restart
ALTER SYSTEM SET bonjour = OFF;  -- ok
ALTER SYSTEM RESET bonjour;  -- ok
SET bonjour_name = 'BonneNuit';  -- fail, requires restart
RESET bonjour_name;  -- fail, requires restart
ALTER SYSTEM SET bonjour_name = 'BonneNuit';  -- ok
ALTER SYSTEM RESET bonjour_name;  -- ok
SET listen_addresses = 'localhost';  -- fail, requires restart
RESET listen_addresses;  -- fail, requires restart
ALTER SYSTEM SET listen_addresses = 'localhost';  -- ok
ALTER SYSTEM RESET listen_addresses;  -- ok
SET max_connections = 50;  -- fail, requires restart
RESET max_connections;  -- fail, requires restart
ALTER SYSTEM SET max_connections = 50;  -- ok
ALTER SYSTEM RESET max_connections;  -- ok
SET port = 50;  -- fail, requires restart
RESET port;  -- fail, requires restart
ALTER SYSTEM SET port = 50;  -- ok
ALTER SYSTEM RESET port;  -- ok
SET superuser_reserved_connections = 50;  -- fail, requires restart
RESET superuser_reserved_connections;  -- fail, requires restart
ALTER SYSTEM SET superuser_reserved_connections = 50;  -- ok
ALTER SYSTEM RESET superuser_reserved_connections;  -- ok
SET unix_socket_directories = '/tmp';  -- fail, requires restart
RESET unix_socket_directories;  -- fail, requires restart
ALTER SYSTEM SET unix_socket_directories = '/tmp';  -- ok
ALTER SYSTEM RESET unix_socket_directories;  -- ok
SET unix_socket_group = 'tenant';  -- fail, requires restart
RESET unix_socket_group;  -- fail, requires restart
ALTER SYSTEM SET unix_socket_group = 'tenant';  -- ok
ALTER SYSTEM RESET unix_socket_group;  -- ok
SET unix_socket_permissions = 50;  -- fail, requires restart
RESET unix_socket_permissions;  -- fail, requires restart
ALTER SYSTEM SET unix_socket_permissions = 50;  -- ok
ALTER SYSTEM RESET unix_socket_permissions;  -- ok
-- PGC_POSTMASTER / DEVELOPER_OPTIONS
SET ignore_invalid_pages = OFF;  -- fail, requires restart
RESET ignore_invalid_pages;  -- fail, requires restart
ALTER SYSTEM SET ignore_invalid_pages = OFF;  -- ok
ALTER SYSTEM RESET ignore_invalid_pages;  -- ok
-- PGC_POSTMASTER / ERROR_HANDLING_OPTIONS
SET data_sync_retry = OFF;  -- fail, requires restart
RESET data_sync_retry;  -- fail, requires restart
ALTER SYSTEM SET data_sync_retry = OFF;  -- ok
ALTER SYSTEM RESET data_sync_retry;  -- ok
-- PGC_POSTMASTER / FILE_LOCATIONS
SET config_file = '/usr/local/data/postgresql.conf';  -- fail, requires restart
RESET config_file;  -- fail, requires restart
ALTER SYSTEM SET config_file = '/usr/local/data/postgresql.conf';  -- fail, cannot be changed
ALTER SYSTEM RESET config_file;  -- fail, cannot be changed
SET data_directory = '/usr/local/data';  -- fail, requires restart
RESET data_directory;  -- fail, requires restart
ALTER SYSTEM SET data_directory = '/usr/local/data';  -- fail, cannot be changed
ALTER SYSTEM RESET data_directory;  -- fail, cannot be changed
SET external_pid_file = '/var/postgres/master.pid';  -- fail, requires restart
RESET external_pid_file;  -- fail, requires restart
ALTER SYSTEM SET external_pid_file = '/var/postgres/master.pid';  -- ok
ALTER SYSTEM RESET external_pid_file;  -- ok
-- PGC_POSTMASTER / LOCK_MANAGEMENT
SET max_locks_per_transaction = 50;  -- fail, requires restart
RESET max_locks_per_transaction;  -- fail, requires restart
ALTER SYSTEM SET max_locks_per_transaction = 50;  -- ok
ALTER SYSTEM RESET max_locks_per_transaction;  -- ok
SET max_pred_locks_per_transaction = 50;  -- fail, requires restart
RESET max_pred_locks_per_transaction;  -- fail, requires restart
ALTER SYSTEM SET max_pred_locks_per_transaction = 50;  -- ok
ALTER SYSTEM RESET max_pred_locks_per_transaction;  -- ok
-- PGC_POSTMASTER / LOGGING_WHERE
SET event_source = 'PostgreSQL';  -- fail, requires restart
RESET event_source;  -- fail, requires restart
ALTER SYSTEM SET event_source = 'PostgreSQL';  -- ok
ALTER SYSTEM RESET event_source;  -- ok
SET logging_collector = OFF;  -- fail, requires restart
RESET logging_collector;  -- fail, requires restart
ALTER SYSTEM SET logging_collector = OFF;  -- ok
ALTER SYSTEM RESET logging_collector;  -- ok
-- PGC_POSTMASTER / PROCESS_TITLE
SET cluster_name = 'BonCluster';  -- fail, requires restart
RESET cluster_name;  -- fail, requires restart
ALTER SYSTEM SET cluster_name = 'BonCluster';  -- ok
ALTER SYSTEM RESET cluster_name;  -- ok
-- PGC_POSTMASTER / REPLICATION_SENDING
SET max_replication_slots = 50;  -- fail, requires restart
RESET max_replication_slots;  -- fail, requires restart
ALTER SYSTEM SET max_replication_slots = 50;  -- ok
ALTER SYSTEM RESET max_replication_slots;  -- ok
SET max_wal_senders = 50;  -- fail, requires restart
RESET max_wal_senders;  -- fail, requires restart
ALTER SYSTEM SET max_wal_senders = 50;  -- ok
ALTER SYSTEM RESET max_wal_senders;  -- ok
SET track_commit_timestamp = OFF;  -- fail, requires restart
RESET track_commit_timestamp;  -- fail, requires restart
ALTER SYSTEM SET track_commit_timestamp = OFF;  -- ok
ALTER SYSTEM RESET track_commit_timestamp;  -- ok
-- PGC_POSTMASTER / REPLICATION_STANDBY
SET hot_standby = OFF;  -- fail, requires restart
RESET hot_standby;  -- fail, requires restart
ALTER SYSTEM SET hot_standby = OFF;  -- ok
ALTER SYSTEM RESET hot_standby;  -- ok
-- PGC_POSTMASTER / REPLICATION_SUBSCRIBERS
SET max_logical_replication_workers = 50;  -- fail, requires restart
RESET max_logical_replication_workers;  -- fail, requires restart
ALTER SYSTEM SET max_logical_replication_workers = 50;  -- ok
ALTER SYSTEM RESET max_logical_replication_workers;  -- ok
-- PGC_POSTMASTER / RESOURCES_ASYNCHRONOUS
SET max_worker_processes = 50;  -- fail, requires restart
RESET max_worker_processes;  -- fail, requires restart
ALTER SYSTEM SET max_worker_processes = 50;  -- ok
ALTER SYSTEM RESET max_worker_processes;  -- ok
SET old_snapshot_threshold = 50;  -- fail, requires restart
RESET old_snapshot_threshold;  -- fail, requires restart
ALTER SYSTEM SET old_snapshot_threshold = 50;  -- ok
ALTER SYSTEM RESET old_snapshot_threshold;  -- ok
-- PGC_POSTMASTER / RESOURCES_KERNEL
SET max_files_per_process = 1073741855;  -- fail, requires restart
RESET max_files_per_process;  -- fail, requires restart
ALTER SYSTEM SET max_files_per_process = 1073741855;  -- ok
ALTER SYSTEM RESET max_files_per_process;  -- ok
-- PGC_POSTMASTER / RESOURCES_MEM
SET dynamic_shared_memory_type = 'posix';  -- fail, requires restart
RESET dynamic_shared_memory_type;  -- fail, requires restart
ALTER SYSTEM SET dynamic_shared_memory_type = 'posix';  -- ok
ALTER SYSTEM RESET dynamic_shared_memory_type;  -- ok
SET huge_pages = 'try';  -- fail, requires restart
RESET huge_pages;  -- fail, requires restart
ALTER SYSTEM SET huge_pages = 'try';  -- ok
ALTER SYSTEM RESET huge_pages;  -- ok
SET max_prepared_transactions = 50;  -- fail, requires restart
RESET max_prepared_transactions;  -- fail, requires restart
ALTER SYSTEM SET max_prepared_transactions = 50;  -- ok
ALTER SYSTEM RESET max_prepared_transactions;  -- ok
SET min_dynamic_shared_memory = 50;  -- fail, requires restart
RESET min_dynamic_shared_memory;  -- fail, requires restart
ALTER SYSTEM SET min_dynamic_shared_memory = 50;  -- ok
ALTER SYSTEM RESET min_dynamic_shared_memory;  -- ok
SET shared_buffers = 50;  -- fail, requires restart
RESET shared_buffers;  -- fail, requires restart
ALTER SYSTEM SET shared_buffers = 50;  -- ok
ALTER SYSTEM RESET shared_buffers;  -- ok
SET shared_memory_type = 'mmap';  -- fail, requires restart
RESET shared_memory_type;  -- fail, requires restart
ALTER SYSTEM SET shared_memory_type = 'mmap';  -- ok
ALTER SYSTEM RESET shared_memory_type;  -- ok
-- PGC_POSTMASTER / STATS_COLLECTOR
SET track_activity_query_size = 524338;  -- fail, requires restart
RESET track_activity_query_size;  -- fail, requires restart
ALTER SYSTEM SET track_activity_query_size = 524338;  -- ok
ALTER SYSTEM RESET track_activity_query_size;  -- ok
-- PGC_POSTMASTER / WAL_ARCHIVING
SET archive_mode = 'off';  -- fail, requires restart
RESET archive_mode;  -- fail, requires restart
ALTER SYSTEM SET archive_mode = 'off';  -- ok
ALTER SYSTEM RESET archive_mode;  -- ok
-- PGC_POSTMASTER / WAL_RECOVERY_TARGET
SET recovery_target_action = 'pause';  -- fail, requires restart
RESET recovery_target_action;  -- fail, requires restart
ALTER SYSTEM SET recovery_target_action = 'pause';  -- ok
ALTER SYSTEM RESET recovery_target_action;  -- ok
SET recovery_target_inclusive = OFF;  -- fail, requires restart
RESET recovery_target_inclusive;  -- fail, requires restart
ALTER SYSTEM SET recovery_target_inclusive = OFF;  -- ok
ALTER SYSTEM RESET recovery_target_inclusive;  -- ok
SET recovery_target_lsn = '16/B374D848';  -- fail, requires restart
RESET recovery_target_lsn;  -- fail, requires restart
ALTER SYSTEM SET recovery_target_lsn = '16/B374D848';  -- ok
ALTER SYSTEM RESET recovery_target_lsn;  -- ok
SET recovery_target_name = 'BonPoint';  -- fail, requires restart
RESET recovery_target_name;  -- fail, requires restart
ALTER SYSTEM SET recovery_target_name = 'BonPoint';  -- ok
ALTER SYSTEM RESET recovery_target_name;  -- ok
SET recovery_target_time = '2001-02-03 04:05:06.789 Europe/Helsinki';  -- fail, requires restart
RESET recovery_target_time;  -- fail, requires restart
ALTER SYSTEM SET recovery_target_time = '2001-02-03 04:05:06.789 Europe/Helsinki';  -- ok
ALTER SYSTEM RESET recovery_target_time;  -- ok
SET recovery_target_timeline = 'latest';  -- fail, requires restart
RESET recovery_target_timeline;  -- fail, requires restart
ALTER SYSTEM SET recovery_target_timeline = 'latest';  -- ok
ALTER SYSTEM RESET recovery_target_timeline;  -- ok
SET recovery_target_xid = '12345678';  -- fail, requires restart
RESET recovery_target_xid;  -- fail, requires restart
ALTER SYSTEM SET recovery_target_xid = '12345678';  -- ok
ALTER SYSTEM RESET recovery_target_xid;  -- ok
-- PGC_POSTMASTER / WAL_SETTINGS
SET wal_buffers = 50;  -- fail, requires restart
RESET wal_buffers;  -- fail, requires restart
ALTER SYSTEM SET wal_buffers = 50;  -- ok
ALTER SYSTEM RESET wal_buffers;  -- ok
SET wal_level = 'replica';  -- fail, requires restart
RESET wal_level;  -- fail, requires restart
ALTER SYSTEM SET wal_level = 'replica';  -- ok
ALTER SYSTEM RESET wal_level;  -- ok
SET wal_log_hints = OFF;  -- fail, requires restart
RESET wal_log_hints;  -- fail, requires restart
ALTER SYSTEM SET wal_log_hints = OFF;  -- ok
ALTER SYSTEM RESET wal_log_hints;  -- ok
-- PGC_SIGHUP / AUTOVACUUM
SET autovacuum = OFF;  -- fail, requires reload
RESET autovacuum;  -- fail, requires reload
ALTER SYSTEM SET autovacuum = OFF;  -- ok
ALTER SYSTEM RESET autovacuum;  -- ok
SET autovacuum_analyze_scale_factor = 50;  -- fail, requires reload
RESET autovacuum_analyze_scale_factor;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_analyze_scale_factor = 50;  -- ok
ALTER SYSTEM RESET autovacuum_analyze_scale_factor;  -- ok
SET autovacuum_analyze_threshold = 50;  -- fail, requires reload
RESET autovacuum_analyze_threshold;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_analyze_threshold = 50;  -- ok
ALTER SYSTEM RESET autovacuum_analyze_threshold;  -- ok
SET autovacuum_naptime = 50;  -- fail, requires reload
RESET autovacuum_naptime;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_naptime = 50;  -- ok
ALTER SYSTEM RESET autovacuum_naptime;  -- ok
SET autovacuum_vacuum_cost_delay = 50;  -- fail, requires reload
RESET autovacuum_vacuum_cost_delay;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_vacuum_cost_delay = 50;  -- ok
ALTER SYSTEM RESET autovacuum_vacuum_cost_delay;  -- ok
SET autovacuum_vacuum_cost_limit = 50;  -- fail, requires reload
RESET autovacuum_vacuum_cost_limit;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_vacuum_cost_limit = 50;  -- ok
ALTER SYSTEM RESET autovacuum_vacuum_cost_limit;  -- ok
SET autovacuum_vacuum_insert_scale_factor = 50;  -- fail, requires reload
RESET autovacuum_vacuum_insert_scale_factor;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_vacuum_insert_scale_factor = 50;  -- ok
ALTER SYSTEM RESET autovacuum_vacuum_insert_scale_factor;  -- ok
SET autovacuum_vacuum_insert_threshold = 50;  -- fail, requires reload
RESET autovacuum_vacuum_insert_threshold;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_vacuum_insert_threshold = 50;  -- ok
ALTER SYSTEM RESET autovacuum_vacuum_insert_threshold;  -- ok
SET autovacuum_vacuum_scale_factor = 50;  -- fail, requires reload
RESET autovacuum_vacuum_scale_factor;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 50;  -- ok
ALTER SYSTEM RESET autovacuum_vacuum_scale_factor;  -- ok
SET autovacuum_vacuum_threshold = 50;  -- fail, requires reload
RESET autovacuum_vacuum_threshold;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_vacuum_threshold = 50;  -- ok
ALTER SYSTEM RESET autovacuum_vacuum_threshold;  -- ok
-- PGC_SIGHUP / CONN_AUTH_AUTH
SET authentication_timeout = 50;  -- fail, requires reload
RESET authentication_timeout;  -- fail, requires reload
ALTER SYSTEM SET authentication_timeout = 50;  -- ok
ALTER SYSTEM RESET authentication_timeout;  -- ok
SET db_user_namespace = OFF;  -- fail, requires reload
RESET db_user_namespace;  -- fail, requires reload
ALTER SYSTEM SET db_user_namespace = OFF;  -- ok
ALTER SYSTEM RESET db_user_namespace;  -- ok
SET krb_caseins_users = OFF;  -- fail, requires reload
RESET krb_caseins_users;  -- fail, requires reload
ALTER SYSTEM SET krb_caseins_users = OFF;  -- ok
ALTER SYSTEM RESET krb_caseins_users;  -- ok
SET krb_server_keyfile = 'krb/server.key';  -- fail, requires reload
RESET krb_server_keyfile;  -- fail, requires reload
ALTER SYSTEM SET krb_server_keyfile = 'krb/server.key';  -- ok
ALTER SYSTEM RESET krb_server_keyfile;  -- ok
-- PGC_SIGHUP / CONN_AUTH_SSL
SET ssl_ca_file = 'ca/server.ca';  -- fail, requires reload
RESET ssl_ca_file;  -- fail, requires reload
ALTER SYSTEM SET ssl_ca_file = 'ca/server.ca';  -- ok
ALTER SYSTEM RESET ssl_ca_file;  -- ok
SET ssl_cert_file = 'crt/server.crt';  -- fail, requires reload
RESET ssl_cert_file;  -- fail, requires reload
ALTER SYSTEM SET ssl_cert_file = 'crt/server.crt';  -- ok
ALTER SYSTEM RESET ssl_cert_file;  -- ok
SET ssl_ciphers = 'none';  -- fail, requires reload
RESET ssl_ciphers;  -- fail, requires reload
ALTER SYSTEM SET ssl_ciphers = 'none';  -- ok
ALTER SYSTEM RESET ssl_ciphers;  -- ok
SET ssl_crl_dir = 'crl/';  -- fail, requires reload
RESET ssl_crl_dir;  -- fail, requires reload
ALTER SYSTEM SET ssl_crl_dir = 'crl/';  -- ok
ALTER SYSTEM RESET ssl_crl_dir;  -- ok
SET ssl_crl_file = 'crl/server.crl';  -- fail, requires reload
RESET ssl_crl_file;  -- fail, requires reload
ALTER SYSTEM SET ssl_crl_file = 'crl/server.crl';  -- ok
ALTER SYSTEM RESET ssl_crl_file;  -- ok
SET ssl_dh_params_file = 'ssl/params';  -- fail, requires reload
RESET ssl_dh_params_file;  -- fail, requires reload
ALTER SYSTEM SET ssl_dh_params_file = 'ssl/params';  -- ok
ALTER SYSTEM RESET ssl_dh_params_file;  -- ok
SET ssl_ecdh_curve = 'none';  -- fail, requires reload
RESET ssl_ecdh_curve;  -- fail, requires reload
ALTER SYSTEM SET ssl_ecdh_curve = 'none';  -- ok
ALTER SYSTEM RESET ssl_ecdh_curve;  -- ok
SET ssl_key_file = 'crl/server.key';  -- fail, requires reload
RESET ssl_key_file;  -- fail, requires reload
ALTER SYSTEM SET ssl_key_file = 'crl/server.key';  -- ok
ALTER SYSTEM RESET ssl_key_file;  -- ok
SET ssl_min_protocol_version = 'TLSv1.2';  -- fail, requires reload
RESET ssl_min_protocol_version;  -- fail, requires reload
ALTER SYSTEM SET ssl_min_protocol_version = 'TLSv1.2';  -- ok
ALTER SYSTEM RESET ssl_min_protocol_version;  -- ok
SET ssl_passphrase_command = '/bin/passphrase';  -- fail, requires reload
RESET ssl_passphrase_command;  -- fail, requires reload
ALTER SYSTEM SET ssl_passphrase_command = '/bin/passphrase';  -- ok
ALTER SYSTEM RESET ssl_passphrase_command;  -- ok
SET ssl_passphrase_command_supports_reload = OFF;  -- fail, requires reload
RESET ssl_passphrase_command_supports_reload;  -- fail, requires reload
ALTER SYSTEM SET ssl_passphrase_command_supports_reload = OFF;  -- ok
ALTER SYSTEM RESET ssl_passphrase_command_supports_reload;  -- ok
SET ssl_prefer_server_ciphers = OFF;  -- fail, requires reload
RESET ssl_prefer_server_ciphers;  -- fail, requires reload
ALTER SYSTEM SET ssl_prefer_server_ciphers = OFF;  -- ok
ALTER SYSTEM RESET ssl_prefer_server_ciphers;  -- ok
-- PGC_SIGHUP / DEVELOPER_OPTIONS
SET pre_auth_delay = 50;  -- fail, requires reload
RESET pre_auth_delay;  -- fail, requires reload
ALTER SYSTEM SET pre_auth_delay = 50;  -- ok
ALTER SYSTEM RESET pre_auth_delay;  -- ok
SET remove_temp_files_after_crash = OFF;  -- fail, requires reload
RESET remove_temp_files_after_crash;  -- fail, requires reload
ALTER SYSTEM SET remove_temp_files_after_crash = OFF;  -- ok
ALTER SYSTEM RESET remove_temp_files_after_crash;  -- ok
SET trace_recovery_messages = 'log';  -- fail, requires reload
RESET trace_recovery_messages;  -- fail, requires reload
ALTER SYSTEM SET trace_recovery_messages = 'log';  -- ok
ALTER SYSTEM RESET trace_recovery_messages;  -- ok
-- PGC_SIGHUP / ERROR_HANDLING_OPTIONS
SET recovery_init_sync_method = 'fsync';  -- fail, requires reload
RESET recovery_init_sync_method;  -- fail, requires reload
ALTER SYSTEM SET recovery_init_sync_method = 'fsync';  -- ok
ALTER SYSTEM RESET recovery_init_sync_method;  -- ok
SET restart_after_crash = OFF;  -- fail, requires reload
RESET restart_after_crash;  -- fail, requires reload
ALTER SYSTEM SET restart_after_crash = OFF;  -- ok
ALTER SYSTEM RESET restart_after_crash;  -- ok
-- PGC_SIGHUP / LOCK_MANAGEMENT
SET max_pred_locks_per_page = 50;  -- fail, requires reload
RESET max_pred_locks_per_page;  -- fail, requires reload
ALTER SYSTEM SET max_pred_locks_per_page = 50;  -- ok
ALTER SYSTEM RESET max_pred_locks_per_page;  -- ok
SET max_pred_locks_per_relation = 50;  -- fail, requires reload
RESET max_pred_locks_per_relation;  -- fail, requires reload
ALTER SYSTEM SET max_pred_locks_per_relation = 50;  -- ok
ALTER SYSTEM RESET max_pred_locks_per_relation;  -- ok
-- PGC_SIGHUP / LOGGING_WHAT
SET log_autovacuum_min_duration = 50;  -- fail, requires reload
RESET log_autovacuum_min_duration;  -- fail, requires reload
ALTER SYSTEM SET log_autovacuum_min_duration = 50;  -- ok
ALTER SYSTEM RESET log_autovacuum_min_duration;  -- ok
SET log_checkpoints = OFF;  -- fail, requires reload
RESET log_checkpoints;  -- fail, requires reload
ALTER SYSTEM SET log_checkpoints = OFF;  -- ok
ALTER SYSTEM RESET log_checkpoints;  -- ok
SET log_hostname = OFF;  -- fail, requires reload
RESET log_hostname;  -- fail, requires reload
ALTER SYSTEM SET log_hostname = OFF;  -- ok
ALTER SYSTEM RESET log_hostname;  -- ok
SET log_line_prefix = '%m [%p] ';  -- fail, requires reload
RESET log_line_prefix;  -- fail, requires reload
ALTER SYSTEM SET log_line_prefix = '%m [%p] ';  -- ok
ALTER SYSTEM RESET log_line_prefix;  -- ok
SET log_recovery_conflict_waits = OFF;  -- fail, requires reload
RESET log_recovery_conflict_waits;  -- fail, requires reload
ALTER SYSTEM SET log_recovery_conflict_waits = OFF;  -- ok
ALTER SYSTEM RESET log_recovery_conflict_waits;  -- ok
SET log_timezone = 'Europe/Helsinki';  -- fail, requires reload
RESET log_timezone;  -- fail, requires reload
ALTER SYSTEM SET log_timezone = 'Europe/Helsinki';  -- ok
ALTER SYSTEM RESET log_timezone;  -- ok
-- PGC_SIGHUP / LOGGING_WHERE
SET log_directory = 'log';  -- fail, requires reload
RESET log_directory;  -- fail, requires reload
ALTER SYSTEM SET log_directory = 'log';  -- ok
ALTER SYSTEM RESET log_directory;  -- ok
SET log_file_mode = 50;  -- fail, requires reload
RESET log_file_mode;  -- fail, requires reload
ALTER SYSTEM SET log_file_mode = 50;  -- ok
ALTER SYSTEM RESET log_file_mode;  -- ok
SET log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log';  -- fail, requires reload
RESET log_filename;  -- fail, requires reload
ALTER SYSTEM SET log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log';  -- ok
ALTER SYSTEM RESET log_filename;  -- ok
SET log_rotation_age = 50;  -- fail, requires reload
RESET log_rotation_age;  -- fail, requires reload
ALTER SYSTEM SET log_rotation_age = 50;  -- ok
ALTER SYSTEM RESET log_rotation_age;  -- ok
SET log_rotation_size = 50;  -- fail, requires reload
RESET log_rotation_size;  -- fail, requires reload
ALTER SYSTEM SET log_rotation_size = 50;  -- ok
ALTER SYSTEM RESET log_rotation_size;  -- ok
SET log_truncate_on_rotation = OFF;  -- fail, requires reload
RESET log_truncate_on_rotation;  -- fail, requires reload
ALTER SYSTEM SET log_truncate_on_rotation = OFF;  -- ok
ALTER SYSTEM RESET log_truncate_on_rotation;  -- ok
SET syslog_ident = 'postgres';  -- fail, requires reload
RESET syslog_ident;  -- fail, requires reload
ALTER SYSTEM SET syslog_ident = 'postgres';  -- ok
ALTER SYSTEM RESET syslog_ident;  -- ok
SET syslog_sequence_numbers = OFF;  -- fail, requires reload
RESET syslog_sequence_numbers;  -- fail, requires reload
ALTER SYSTEM SET syslog_sequence_numbers = OFF;  -- ok
ALTER SYSTEM RESET syslog_sequence_numbers;  -- ok
SET syslog_split_messages = OFF;  -- fail, requires reload
RESET syslog_split_messages;  -- fail, requires reload
ALTER SYSTEM SET syslog_split_messages = OFF;  -- ok
ALTER SYSTEM RESET syslog_split_messages;  -- ok
-- PGC_SIGHUP / REPLICATION_PRIMARY
SET synchronous_standby_names = 'fee, fi, fo, fum';  -- fail, requires reload
RESET synchronous_standby_names;  -- fail, requires reload
ALTER SYSTEM SET synchronous_standby_names = 'fee, fi, fo, fum';  -- ok
ALTER SYSTEM RESET synchronous_standby_names;  -- ok
SET vacuum_defer_cleanup_age = 50;  -- fail, requires reload
RESET vacuum_defer_cleanup_age;  -- fail, requires reload
ALTER SYSTEM SET vacuum_defer_cleanup_age = 50;  -- ok
ALTER SYSTEM RESET vacuum_defer_cleanup_age;  -- ok
-- PGC_SIGHUP / REPLICATION_SENDING
SET max_slot_wal_keep_size = 50;  -- fail, requires reload
RESET max_slot_wal_keep_size;  -- fail, requires reload
ALTER SYSTEM SET max_slot_wal_keep_size = 50;  -- ok
ALTER SYSTEM RESET max_slot_wal_keep_size;  -- ok
SET wal_keep_size = 50;  -- fail, requires reload
RESET wal_keep_size;  -- fail, requires reload
ALTER SYSTEM SET wal_keep_size = 50;  -- ok
ALTER SYSTEM RESET wal_keep_size;  -- ok
-- PGC_SIGHUP / REPLICATION_STANDBY
SET hot_standby_feedback = OFF;  -- fail, requires reload
RESET hot_standby_feedback;  -- fail, requires reload
ALTER SYSTEM SET hot_standby_feedback = OFF;  -- ok
ALTER SYSTEM RESET hot_standby_feedback;  -- ok
SET max_standby_archive_delay = 50;  -- fail, requires reload
RESET max_standby_archive_delay;  -- fail, requires reload
ALTER SYSTEM SET max_standby_archive_delay = 50;  -- ok
ALTER SYSTEM RESET max_standby_archive_delay;  -- ok
SET max_standby_streaming_delay = 50;  -- fail, requires reload
RESET max_standby_streaming_delay;  -- fail, requires reload
ALTER SYSTEM SET max_standby_streaming_delay = 50;  -- ok
ALTER SYSTEM RESET max_standby_streaming_delay;  -- ok
SET primary_conninfo = 'postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp';  -- fail, requires reload
RESET primary_conninfo;  -- fail, requires reload
ALTER SYSTEM SET primary_conninfo = 'postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp';  -- ok
ALTER SYSTEM RESET primary_conninfo;  -- ok
SET primary_slot_name = 'bonne_fente';  -- fail, requires reload
RESET primary_slot_name;  -- fail, requires reload
ALTER SYSTEM SET primary_slot_name = 'bonne_fente';  -- ok
ALTER SYSTEM RESET primary_slot_name;  -- ok
SET promote_trigger_file = 'promote.trigger';  -- fail, requires reload
RESET promote_trigger_file;  -- fail, requires reload
ALTER SYSTEM SET promote_trigger_file = 'promote.trigger';  -- ok
ALTER SYSTEM RESET promote_trigger_file;  -- ok
SET recovery_min_apply_delay = 50;  -- fail, requires reload
RESET recovery_min_apply_delay;  -- fail, requires reload
ALTER SYSTEM SET recovery_min_apply_delay = 50;  -- ok
ALTER SYSTEM RESET recovery_min_apply_delay;  -- ok
SET wal_receiver_create_temp_slot = OFF;  -- fail, requires reload
RESET wal_receiver_create_temp_slot;  -- fail, requires reload
ALTER SYSTEM SET wal_receiver_create_temp_slot = OFF;  -- ok
ALTER SYSTEM RESET wal_receiver_create_temp_slot;  -- ok
SET wal_receiver_status_interval = 50;  -- fail, requires reload
RESET wal_receiver_status_interval;  -- fail, requires reload
ALTER SYSTEM SET wal_receiver_status_interval = 50;  -- ok
ALTER SYSTEM RESET wal_receiver_status_interval;  -- ok
SET wal_receiver_timeout = 50;  -- fail, requires reload
RESET wal_receiver_timeout;  -- fail, requires reload
ALTER SYSTEM SET wal_receiver_timeout = 50;  -- ok
ALTER SYSTEM RESET wal_receiver_timeout;  -- ok
SET wal_retrieve_retry_interval = 50;  -- fail, requires reload
RESET wal_retrieve_retry_interval;  -- fail, requires reload
ALTER SYSTEM SET wal_retrieve_retry_interval = 50;  -- ok
ALTER SYSTEM RESET wal_retrieve_retry_interval;  -- ok
-- PGC_SIGHUP / REPLICATION_SUBSCRIBERS
SET max_sync_workers_per_subscription = 50;  -- fail, requires reload
RESET max_sync_workers_per_subscription;  -- fail, requires reload
ALTER SYSTEM SET max_sync_workers_per_subscription = 50;  -- ok
ALTER SYSTEM RESET max_sync_workers_per_subscription;  -- ok
-- PGC_SIGHUP / RESOURCES_BGWRITER
SET bgwriter_delay = 50;  -- fail, requires reload
RESET bgwriter_delay;  -- fail, requires reload
ALTER SYSTEM SET bgwriter_delay = 50;  -- ok
ALTER SYSTEM RESET bgwriter_delay;  -- ok
SET bgwriter_flush_after = 50;  -- fail, requires reload
RESET bgwriter_flush_after;  -- fail, requires reload
ALTER SYSTEM SET bgwriter_flush_after = 50;  -- ok
ALTER SYSTEM RESET bgwriter_flush_after;  -- ok
SET bgwriter_lru_maxpages = 50;  -- fail, requires reload
RESET bgwriter_lru_maxpages;  -- fail, requires reload
ALTER SYSTEM SET bgwriter_lru_maxpages = 50;  -- ok
ALTER SYSTEM RESET bgwriter_lru_maxpages;  -- ok
SET bgwriter_lru_multiplier = 5;  -- fail, requires reload
RESET bgwriter_lru_multiplier;  -- fail, requires reload
ALTER SYSTEM SET bgwriter_lru_multiplier = 5;  -- ok
ALTER SYSTEM RESET bgwriter_lru_multiplier;  -- ok
-- PGC_SIGHUP / RESOURCES_MEM
SET autovacuum_work_mem = 50;  -- fail, requires reload
RESET autovacuum_work_mem;  -- fail, requires reload
ALTER SYSTEM SET autovacuum_work_mem = 50;  -- ok
ALTER SYSTEM RESET autovacuum_work_mem;  -- ok
-- PGC_SIGHUP / STATS_COLLECTOR
SET stats_temp_directory = 'pg_stat_tmp';  -- fail, requires reload
RESET stats_temp_directory;  -- fail, requires reload
ALTER SYSTEM SET stats_temp_directory = 'pg_stat_tmp';  -- ok
ALTER SYSTEM RESET stats_temp_directory;  -- ok
-- PGC_SIGHUP / WAL_ARCHIVE_RECOVERY
SET archive_cleanup_command = '/bin/cleanup my stuff';  -- fail, requires reload
RESET archive_cleanup_command;  -- fail, requires reload
ALTER SYSTEM SET archive_cleanup_command = '/bin/cleanup my stuff';  -- ok
ALTER SYSTEM RESET archive_cleanup_command;  -- ok
SET recovery_end_command = '/bin/recover my stuff';  -- fail, requires reload
RESET recovery_end_command;  -- fail, requires reload
ALTER SYSTEM SET recovery_end_command = '/bin/recover my stuff';  -- ok
ALTER SYSTEM RESET recovery_end_command;  -- ok
SET restore_command = '/bin/restore my stuff';  -- fail, requires reload
RESET restore_command;  -- fail, requires reload
ALTER SYSTEM SET restore_command = '/bin/restore my stuff';  -- ok
ALTER SYSTEM RESET restore_command;  -- ok
-- PGC_SIGHUP / WAL_ARCHIVING
SET archive_command = '/bin/archive my stuff';  -- fail, requires reload
RESET archive_command;  -- fail, requires reload
ALTER SYSTEM SET archive_command = '/bin/archive my stuff';  -- ok
ALTER SYSTEM RESET archive_command;  -- ok
SET archive_timeout = 50;  -- fail, requires reload
RESET archive_timeout;  -- fail, requires reload
ALTER SYSTEM SET archive_timeout = 50;  -- ok
ALTER SYSTEM RESET archive_timeout;  -- ok
-- PGC_SIGHUP / WAL_CHECKPOINTS
SET checkpoint_completion_target = 0;  -- fail, requires reload
RESET checkpoint_completion_target;  -- fail, requires reload
ALTER SYSTEM SET checkpoint_completion_target = 0;  -- ok
ALTER SYSTEM RESET checkpoint_completion_target;  -- ok
SET checkpoint_flush_after = 50;  -- fail, requires reload
RESET checkpoint_flush_after;  -- fail, requires reload
ALTER SYSTEM SET checkpoint_flush_after = 50;  -- ok
ALTER SYSTEM RESET checkpoint_flush_after;  -- ok
SET checkpoint_timeout = 50;  -- fail, requires reload
RESET checkpoint_timeout;  -- fail, requires reload
ALTER SYSTEM SET checkpoint_timeout = 50;  -- ok
ALTER SYSTEM RESET checkpoint_timeout;  -- ok
SET checkpoint_warning = 50;  -- fail, requires reload
RESET checkpoint_warning;  -- fail, requires reload
ALTER SYSTEM SET checkpoint_warning = 50;  -- ok
ALTER SYSTEM RESET checkpoint_warning;  -- ok
SET max_wal_size = 50;  -- fail, requires reload
RESET max_wal_size;  -- fail, requires reload
ALTER SYSTEM SET max_wal_size = 50;  -- ok
ALTER SYSTEM RESET max_wal_size;  -- ok
SET min_wal_size = 50;  -- fail, requires reload
RESET min_wal_size;  -- fail, requires reload
ALTER SYSTEM SET min_wal_size = 50;  -- ok
ALTER SYSTEM RESET min_wal_size;  -- ok
-- PGC_SIGHUP / WAL_SETTINGS
SET fsync = OFF;  -- fail, requires reload
RESET fsync;  -- fail, requires reload
ALTER SYSTEM SET fsync = OFF;  -- ok
ALTER SYSTEM RESET fsync;  -- ok
SET full_page_writes = OFF;  -- fail, requires reload
RESET full_page_writes;  -- fail, requires reload
ALTER SYSTEM SET full_page_writes = OFF;  -- ok
ALTER SYSTEM RESET full_page_writes;  -- ok
SET wal_sync_method = 'open_datasync';  -- fail, requires reload
RESET wal_sync_method;  -- fail, requires reload
ALTER SYSTEM SET wal_sync_method = 'open_datasync';  -- ok
ALTER SYSTEM RESET wal_sync_method;  -- ok
SET wal_writer_delay = 50;  -- fail, requires reload
RESET wal_writer_delay;  -- fail, requires reload
ALTER SYSTEM SET wal_writer_delay = 50;  -- ok
ALTER SYSTEM RESET wal_writer_delay;  -- ok
SET wal_writer_flush_after = 50;  -- fail, requires reload
RESET wal_writer_flush_after;  -- fail, requires reload
ALTER SYSTEM SET wal_writer_flush_after = 50;  -- ok
ALTER SYSTEM RESET wal_writer_flush_after;  -- ok
-- PGC_SUSET / CLIENT_CONN_LOCALE
SET lc_messages = 'en_US.UTF-8';  -- ok
RESET lc_messages;  -- ok
ALTER SYSTEM SET lc_messages = 'en_US.UTF-8';  -- ok
ALTER SYSTEM RESET lc_messages;  -- ok
-- PGC_SUSET / CLIENT_CONN_OTHER
SET dynamic_library_path = '$libdir';  -- ok
RESET dynamic_library_path;  -- ok
ALTER SYSTEM SET dynamic_library_path = '$libdir';  -- ok
ALTER SYSTEM RESET dynamic_library_path;  -- ok
-- PGC_SUSET / CLIENT_CONN_PRELOAD
SET session_preload_libraries = 'gssapi_krb5';  -- ok
RESET session_preload_libraries;  -- ok
ALTER SYSTEM SET session_preload_libraries = 'gssapi_krb5';  -- ok
ALTER SYSTEM RESET session_preload_libraries;  -- ok
-- PGC_SUSET / CLIENT_CONN_STATEMENT
SET session_replication_role = 'origin';  -- ok
RESET session_replication_role;  -- ok
ALTER SYSTEM SET session_replication_role = 'origin';  -- ok
ALTER SYSTEM RESET session_replication_role;  -- ok
-- PGC_SUSET / COMPAT_OPTIONS_PREVIOUS
SET lo_compat_privileges = OFF;  -- ok
RESET lo_compat_privileges;  -- ok
ALTER SYSTEM SET lo_compat_privileges = OFF;  -- ok
ALTER SYSTEM RESET lo_compat_privileges;  -- ok
-- PGC_SUSET / DEVELOPER_OPTIONS
SET allow_system_table_mods = OFF;  -- ok
RESET allow_system_table_mods;  -- ok
ALTER SYSTEM SET allow_system_table_mods = OFF;  -- ok
ALTER SYSTEM RESET allow_system_table_mods;  -- ok
SET backtrace_functions = 'partition_list_bsearch,partition_range_datum_bsearch,partition_hash_bsearch';  -- ok
RESET backtrace_functions;  -- ok
ALTER SYSTEM SET backtrace_functions = 'partition_list_bsearch,partition_range_datum_bsearch,partition_hash_bsearch';  -- ok
ALTER SYSTEM RESET backtrace_functions;  -- ok
SET debug_discard_caches = 2;  -- ok
RESET debug_discard_caches;  -- ok
ALTER SYSTEM SET debug_discard_caches = 2;  -- ok
ALTER SYSTEM RESET debug_discard_caches;  -- ok
SET ignore_checksum_failure = OFF;  -- ok
RESET ignore_checksum_failure;  -- ok
ALTER SYSTEM SET ignore_checksum_failure = OFF;  -- ok
ALTER SYSTEM RESET ignore_checksum_failure;  -- ok
SET jit_dump_bitcode = OFF;  -- ok
RESET jit_dump_bitcode;  -- ok
ALTER SYSTEM SET jit_dump_bitcode = OFF;  -- ok
ALTER SYSTEM RESET jit_dump_bitcode;  -- ok
SET wal_consistency_checking = 'heap, heap2, btree, hash, gin, gist, sequence, spgist, brin, generic';  -- ok
RESET wal_consistency_checking;  -- ok
ALTER SYSTEM SET wal_consistency_checking = 'heap, heap2, btree, hash, gin, gist, sequence, spgist, brin, generic';  -- ok
ALTER SYSTEM RESET wal_consistency_checking;  -- ok
SET zero_damaged_pages = OFF;  -- ok
RESET zero_damaged_pages;  -- ok
ALTER SYSTEM SET zero_damaged_pages = OFF;  -- ok
ALTER SYSTEM RESET zero_damaged_pages;  -- ok
-- PGC_SUSET / LOCK_MANAGEMENT
SET deadlock_timeout = 1073741824;  -- ok
RESET deadlock_timeout;  -- ok
ALTER SYSTEM SET deadlock_timeout = 1073741824;  -- ok
ALTER SYSTEM RESET deadlock_timeout;  -- ok
-- PGC_SUSET / LOGGING_WHAT
SET log_duration = OFF;  -- ok
RESET log_duration;  -- ok
ALTER SYSTEM SET log_duration = OFF;  -- ok
ALTER SYSTEM RESET log_duration;  -- ok
SET log_error_verbosity = 'default';  -- ok
RESET log_error_verbosity;  -- ok
ALTER SYSTEM SET log_error_verbosity = 'default';  -- ok
ALTER SYSTEM RESET log_error_verbosity;  -- ok
SET log_lock_waits = OFF;  -- ok
RESET log_lock_waits;  -- ok
ALTER SYSTEM SET log_lock_waits = OFF;  -- ok
ALTER SYSTEM RESET log_lock_waits;  -- ok
SET log_parameter_max_length = 50;  -- ok
RESET log_parameter_max_length;  -- ok
ALTER SYSTEM SET log_parameter_max_length = 50;  -- ok
ALTER SYSTEM RESET log_parameter_max_length;  -- ok
SET log_replication_commands = OFF;  -- ok
RESET log_replication_commands;  -- ok
ALTER SYSTEM SET log_replication_commands = OFF;  -- ok
ALTER SYSTEM RESET log_replication_commands;  -- ok
SET log_statement = 'none';  -- ok
RESET log_statement;  -- ok
ALTER SYSTEM SET log_statement = 'none';  -- ok
ALTER SYSTEM RESET log_statement;  -- ok
SET log_temp_files = 50;  -- ok
RESET log_temp_files;  -- ok
ALTER SYSTEM SET log_temp_files = 50;  -- ok
ALTER SYSTEM RESET log_temp_files;  -- ok
-- PGC_SUSET / LOGGING_WHEN
SET log_min_duration_sample = 50;  -- ok
RESET log_min_duration_sample;  -- ok
ALTER SYSTEM SET log_min_duration_sample = 50;  -- ok
ALTER SYSTEM RESET log_min_duration_sample;  -- ok
SET log_min_duration_statement = 50;  -- ok
RESET log_min_duration_statement;  -- ok
ALTER SYSTEM SET log_min_duration_statement = 50;  -- ok
ALTER SYSTEM RESET log_min_duration_statement;  -- ok
SET log_min_error_statement = 'error';  -- ok
RESET log_min_error_statement;  -- ok
ALTER SYSTEM SET log_min_error_statement = 'error';  -- ok
ALTER SYSTEM RESET log_min_error_statement;  -- ok
SET log_min_messages = 'warning';  -- ok
RESET log_min_messages;  -- ok
ALTER SYSTEM SET log_min_messages = 'warning';  -- ok
ALTER SYSTEM RESET log_min_messages;  -- ok
SET log_statement_sample_rate = 0;  -- ok
RESET log_statement_sample_rate;  -- ok
ALTER SYSTEM SET log_statement_sample_rate = 0;  -- ok
ALTER SYSTEM RESET log_statement_sample_rate;  -- ok
SET log_transaction_sample_rate = 0;  -- ok
RESET log_transaction_sample_rate;  -- ok
ALTER SYSTEM SET log_transaction_sample_rate = 0;  -- ok
ALTER SYSTEM RESET log_transaction_sample_rate;  -- ok
-- PGC_SUSET / PROCESS_TITLE
SET update_process_title = OFF;  -- ok
RESET update_process_title;  -- ok
ALTER SYSTEM SET update_process_title = OFF;  -- ok
ALTER SYSTEM RESET update_process_title;  -- ok
-- PGC_SUSET / RESOURCES_DISK
SET temp_file_limit = 50;  -- ok
RESET temp_file_limit;  -- ok
ALTER SYSTEM SET temp_file_limit = 50;  -- ok
ALTER SYSTEM RESET temp_file_limit;  -- ok
-- PGC_SUSET / RESOURCES_MEM
SET max_stack_depth = 3890;  -- ok
RESET max_stack_depth;  -- ok
ALTER SYSTEM SET max_stack_depth = 3890;  -- ok
ALTER SYSTEM RESET max_stack_depth;  -- ok
-- PGC_SUSET / STATS_COLLECTOR
SET track_activities = OFF;  -- ok
RESET track_activities;  -- ok
ALTER SYSTEM SET track_activities = OFF;  -- ok
ALTER SYSTEM RESET track_activities;  -- ok
SET track_counts = OFF;  -- ok
RESET track_counts;  -- ok
ALTER SYSTEM SET track_counts = OFF;  -- ok
ALTER SYSTEM RESET track_counts;  -- ok
SET track_functions = 'none';  -- ok
RESET track_functions;  -- ok
ALTER SYSTEM SET track_functions = 'none';  -- ok
ALTER SYSTEM RESET track_functions;  -- ok
SET track_io_timing = OFF;  -- ok
RESET track_io_timing;  -- ok
ALTER SYSTEM SET track_io_timing = OFF;  -- ok
ALTER SYSTEM RESET track_io_timing;  -- ok
SET track_wal_io_timing = OFF;  -- ok
RESET track_wal_io_timing;  -- ok
ALTER SYSTEM SET track_wal_io_timing = OFF;  -- ok
ALTER SYSTEM RESET track_wal_io_timing;  -- ok
-- PGC_SUSET / STATS_MONITORING
SET compute_query_id = 'auto';  -- ok
RESET compute_query_id;  -- ok
ALTER SYSTEM SET compute_query_id = 'auto';  -- ok
ALTER SYSTEM RESET compute_query_id;  -- ok
SET log_executor_stats = OFF;  -- ok
RESET log_executor_stats;  -- ok
ALTER SYSTEM SET log_executor_stats = OFF;  -- ok
ALTER SYSTEM RESET log_executor_stats;  -- ok
SET log_parser_stats = OFF;  -- ok
RESET log_parser_stats;  -- ok
ALTER SYSTEM SET log_parser_stats = OFF;  -- ok
ALTER SYSTEM RESET log_parser_stats;  -- ok
SET log_planner_stats = OFF;  -- ok
RESET log_planner_stats;  -- ok
ALTER SYSTEM SET log_planner_stats = OFF;  -- ok
ALTER SYSTEM RESET log_planner_stats;  -- ok
SET log_statement_stats = OFF;  -- ok
RESET log_statement_stats;  -- ok
ALTER SYSTEM SET log_statement_stats = OFF;  -- ok
ALTER SYSTEM RESET log_statement_stats;  -- ok
-- PGC_SUSET / WAL_SETTINGS
SET commit_delay = 50;  -- ok
RESET commit_delay;  -- ok
ALTER SYSTEM SET commit_delay = 50;  -- ok
ALTER SYSTEM RESET commit_delay;  -- ok
SET wal_compression = 'pglz';  -- ok
RESET wal_compression;  -- ok
ALTER SYSTEM SET wal_compression = 'pglz';  -- ok
ALTER SYSTEM RESET wal_compression;  -- ok
SET wal_init_zero = OFF;  -- ok
RESET wal_init_zero;  -- ok
ALTER SYSTEM SET wal_init_zero = OFF;  -- ok
ALTER SYSTEM RESET wal_init_zero;  -- ok
SET wal_recycle = OFF;  -- ok
RESET wal_recycle;  -- ok
ALTER SYSTEM SET wal_recycle = OFF;  -- ok
ALTER SYSTEM RESET wal_recycle;  -- ok
-- PGC_SU_BACKEND / DEVELOPER_OPTIONS
SET jit_debugging_support = OFF;  -- fail, cannot be set after connection start
RESET jit_debugging_support;  -- fail, cannot be set after connection start
ALTER SYSTEM SET jit_debugging_support = OFF;  -- ok
ALTER SYSTEM RESET jit_debugging_support;  -- ok
SET jit_profiling_support = OFF;  -- fail, cannot be set after connection start
RESET jit_profiling_support;  -- fail, cannot be set after connection start
ALTER SYSTEM SET jit_profiling_support = OFF;  -- ok
ALTER SYSTEM RESET jit_profiling_support;  -- ok
-- PGC_SU_BACKEND / LOGGING_WHAT
SET log_connections = OFF;  -- fail, cannot be set after connection start
RESET log_connections;  -- fail, cannot be set after connection start
ALTER SYSTEM SET log_connections = OFF;  -- ok
ALTER SYSTEM RESET log_connections;  -- ok
SET log_disconnections = OFF;  -- fail, cannot be set after connection start
RESET log_disconnections;  -- fail, cannot be set after connection start
ALTER SYSTEM SET log_disconnections = OFF;  -- ok
ALTER SYSTEM RESET log_disconnections;  -- ok
-- PGC_USERSET / CLIENT_CONN_LOCALE
SET DateStyle = 'ISO, MDY';  -- ok
RESET DateStyle;  -- ok
ALTER SYSTEM SET DateStyle = 'ISO, MDY';  -- ok
ALTER SYSTEM RESET DateStyle;  -- ok
SET IntervalStyle = 'postgres';  -- ok
RESET IntervalStyle;  -- ok
ALTER SYSTEM SET IntervalStyle = 'postgres';  -- ok
ALTER SYSTEM RESET IntervalStyle;  -- ok
SET TimeZone = 'Europe/Helsinki';  -- ok
RESET TimeZone;  -- ok
ALTER SYSTEM SET TimeZone = 'Europe/Helsinki';  -- ok
ALTER SYSTEM RESET TimeZone;  -- ok
SET client_encoding = 'UTF8';  -- ok
RESET client_encoding;  -- ok
ALTER SYSTEM SET client_encoding = 'UTF8';  -- ok
ALTER SYSTEM RESET client_encoding;  -- ok
SET default_text_search_config = 'pg_catalog.english';  -- ok
RESET default_text_search_config;  -- ok
ALTER SYSTEM SET default_text_search_config = 'pg_catalog.english';  -- ok
ALTER SYSTEM RESET default_text_search_config;  -- ok
SET extra_float_digits = -6;  -- ok
RESET extra_float_digits;  -- ok
ALTER SYSTEM SET extra_float_digits = -6;  -- ok
ALTER SYSTEM RESET extra_float_digits;  -- ok
SET lc_monetary = 'en_US.UTF-8';  -- ok
RESET lc_monetary;  -- ok
ALTER SYSTEM SET lc_monetary = 'en_US.UTF-8';  -- ok
ALTER SYSTEM RESET lc_monetary;  -- ok
SET lc_numeric = 'en_US.UTF-8';  -- ok
RESET lc_numeric;  -- ok
ALTER SYSTEM SET lc_numeric = 'en_US.UTF-8';  -- ok
ALTER SYSTEM RESET lc_numeric;  -- ok
SET lc_time = 'en_US.UTF-8';  -- ok
RESET lc_time;  -- ok
ALTER SYSTEM SET lc_time = 'en_US.UTF-8';  -- ok
ALTER SYSTEM RESET lc_time;  -- ok
SET timezone_abbreviations = 'Default';  -- ok
RESET timezone_abbreviations;  -- ok
ALTER SYSTEM SET timezone_abbreviations = 'Default';  -- ok
ALTER SYSTEM RESET timezone_abbreviations;  -- ok
-- PGC_USERSET / CLIENT_CONN_OTHER
SET gin_fuzzy_search_limit = 50;  -- ok
RESET gin_fuzzy_search_limit;  -- ok
ALTER SYSTEM SET gin_fuzzy_search_limit = 50;  -- ok
ALTER SYSTEM RESET gin_fuzzy_search_limit;  -- ok
-- PGC_USERSET / CLIENT_CONN_PRELOAD
SET local_preload_libraries = 'gssapi_krb5';  -- ok
RESET local_preload_libraries;  -- ok
ALTER SYSTEM SET local_preload_libraries = 'gssapi_krb5';  -- ok
ALTER SYSTEM RESET local_preload_libraries;  -- ok
-- PGC_USERSET / CLIENT_CONN_STATEMENT
SET bytea_output = 'hex';  -- ok
RESET bytea_output;  -- ok
ALTER SYSTEM SET bytea_output = 'hex';  -- ok
ALTER SYSTEM RESET bytea_output;  -- ok
SET check_function_bodies = OFF;  -- ok
RESET check_function_bodies;  -- ok
ALTER SYSTEM SET check_function_bodies = OFF;  -- ok
ALTER SYSTEM RESET check_function_bodies;  -- ok
SET client_min_messages = 'notice';  -- ok
RESET client_min_messages;  -- ok
ALTER SYSTEM SET client_min_messages = 'notice';  -- ok
ALTER SYSTEM RESET client_min_messages;  -- ok
SET default_table_access_method = 'heap';  -- ok
RESET default_table_access_method;  -- ok
ALTER SYSTEM SET default_table_access_method = 'heap';  -- ok
ALTER SYSTEM RESET default_table_access_method;  -- ok
SET default_toast_compression = 'pglz';  -- ok
RESET default_toast_compression;  -- ok
ALTER SYSTEM SET default_toast_compression = 'pglz';  -- ok
ALTER SYSTEM RESET default_toast_compression;  -- ok
SET default_transaction_deferrable = OFF;  -- ok
RESET default_transaction_deferrable;  -- ok
ALTER SYSTEM SET default_transaction_deferrable = OFF;  -- ok
ALTER SYSTEM RESET default_transaction_deferrable;  -- ok
SET default_transaction_isolation = 'read committed';  -- ok
RESET default_transaction_isolation;  -- ok
ALTER SYSTEM SET default_transaction_isolation = 'read committed';  -- ok
ALTER SYSTEM RESET default_transaction_isolation;  -- ok
SET default_transaction_read_only = OFF;  -- ok
RESET default_transaction_read_only;  -- ok
ALTER SYSTEM SET default_transaction_read_only = OFF;  -- ok
ALTER SYSTEM RESET default_transaction_read_only;  -- ok
SET gin_pending_list_limit = 1073741855;  -- ok
RESET gin_pending_list_limit;  -- ok
ALTER SYSTEM SET gin_pending_list_limit = 1073741855;  -- ok
ALTER SYSTEM RESET gin_pending_list_limit;  -- ok
SET idle_in_transaction_session_timeout = 50;  -- ok
RESET idle_in_transaction_session_timeout;  -- ok
ALTER SYSTEM SET idle_in_transaction_session_timeout = 50;  -- ok
ALTER SYSTEM RESET idle_in_transaction_session_timeout;  -- ok
SET idle_session_timeout = 50;  -- ok
RESET idle_session_timeout;  -- ok
ALTER SYSTEM SET idle_session_timeout = 50;  -- ok
ALTER SYSTEM RESET idle_session_timeout;  -- ok
SET lock_timeout = 50;  -- ok
RESET lock_timeout;  -- ok
ALTER SYSTEM SET lock_timeout = 50;  -- ok
ALTER SYSTEM RESET lock_timeout;  -- ok
SET row_security = OFF;  -- ok
RESET row_security;  -- ok
ALTER SYSTEM SET row_security = OFF;  -- ok
ALTER SYSTEM RESET row_security;  -- ok
SET search_path = '"$user", public';  -- ok
RESET search_path;  -- ok
ALTER SYSTEM SET search_path = '"$user", public';  -- ok
ALTER SYSTEM RESET search_path;  -- ok
SET statement_timeout = 5250;  -- ok
RESET statement_timeout;  -- ok
ALTER SYSTEM SET statement_timeout = 5250;  -- ok
ALTER SYSTEM RESET statement_timeout;  -- ok
SET transaction_deferrable = OFF;  -- ok
RESET transaction_deferrable;  -- ok
ALTER SYSTEM SET transaction_deferrable = OFF;  -- fail, cannot be changed
ALTER SYSTEM RESET transaction_deferrable;  -- fail, cannot be changed
SET transaction_read_only = OFF;  -- ok
RESET transaction_read_only;  -- ok
ALTER SYSTEM SET transaction_read_only = OFF;  -- fail, cannot be changed
ALTER SYSTEM RESET transaction_read_only;  -- fail, cannot be changed
SET vacuum_failsafe_age = 50;  -- ok
RESET vacuum_failsafe_age;  -- ok
ALTER SYSTEM SET vacuum_failsafe_age = 50;  -- ok
ALTER SYSTEM RESET vacuum_failsafe_age;  -- ok
SET vacuum_freeze_min_age = 50;  -- ok
RESET vacuum_freeze_min_age;  -- ok
ALTER SYSTEM SET vacuum_freeze_min_age = 50;  -- ok
ALTER SYSTEM RESET vacuum_freeze_min_age;  -- ok
SET vacuum_freeze_table_age = 50;  -- ok
RESET vacuum_freeze_table_age;  -- ok
ALTER SYSTEM SET vacuum_freeze_table_age = 50;  -- ok
ALTER SYSTEM RESET vacuum_freeze_table_age;  -- ok
SET vacuum_multixact_failsafe_age = 50;  -- ok
RESET vacuum_multixact_failsafe_age;  -- ok
ALTER SYSTEM SET vacuum_multixact_failsafe_age = 50;  -- ok
ALTER SYSTEM RESET vacuum_multixact_failsafe_age;  -- ok
SET vacuum_multixact_freeze_min_age = 50;  -- ok
RESET vacuum_multixact_freeze_min_age;  -- ok
ALTER SYSTEM SET vacuum_multixact_freeze_min_age = 50;  -- ok
ALTER SYSTEM RESET vacuum_multixact_freeze_min_age;  -- ok
SET vacuum_multixact_freeze_table_age = 50;  -- ok
RESET vacuum_multixact_freeze_table_age;  -- ok
ALTER SYSTEM SET vacuum_multixact_freeze_table_age = 50;  -- ok
ALTER SYSTEM RESET vacuum_multixact_freeze_table_age;  -- ok
SET xmlbinary = 'base64';  -- ok
RESET xmlbinary;  -- ok
ALTER SYSTEM SET xmlbinary = 'base64';  -- ok
ALTER SYSTEM RESET xmlbinary;  -- ok
SET xmloption = 'content';  -- ok
RESET xmloption;  -- ok
ALTER SYSTEM SET xmloption = 'content';  -- ok
ALTER SYSTEM RESET xmloption;  -- ok
-- PGC_USERSET / COMPAT_OPTIONS_PREVIOUS
SET array_nulls = OFF;  -- ok
RESET array_nulls;  -- ok
ALTER SYSTEM SET array_nulls = OFF;  -- ok
ALTER SYSTEM RESET array_nulls;  -- ok
SET escape_string_warning = OFF;  -- ok
RESET escape_string_warning;  -- ok
ALTER SYSTEM SET escape_string_warning = OFF;  -- ok
ALTER SYSTEM RESET escape_string_warning;  -- ok
SET quote_all_identifiers = OFF;  -- ok
RESET quote_all_identifiers;  -- ok
ALTER SYSTEM SET quote_all_identifiers = OFF;  -- ok
ALTER SYSTEM RESET quote_all_identifiers;  -- ok
SET standard_conforming_strings = OFF;  -- ok
RESET standard_conforming_strings;  -- ok
ALTER SYSTEM SET standard_conforming_strings = OFF;  -- ok
ALTER SYSTEM RESET standard_conforming_strings;  -- ok
SET synchronize_seqscans = OFF;  -- ok
RESET synchronize_seqscans;  -- ok
ALTER SYSTEM SET synchronize_seqscans = OFF;  -- ok
ALTER SYSTEM RESET synchronize_seqscans;  -- ok
-- PGC_USERSET / CONN_AUTH_AUTH
SET password_encryption = 'scram-sha-256';  -- ok
RESET password_encryption;  -- ok
ALTER SYSTEM SET password_encryption = 'scram-sha-256';  -- ok
ALTER SYSTEM RESET password_encryption;  -- ok
-- PGC_USERSET / CONN_AUTH_SETTINGS
SET client_connection_check_interval = 0;  -- ok
RESET client_connection_check_interval;  -- ok
ALTER SYSTEM SET client_connection_check_interval = 0;  -- ok
ALTER SYSTEM RESET client_connection_check_interval;  -- ok
SET tcp_keepalives_count = 50;  -- ok
RESET tcp_keepalives_count;  -- ok
ALTER SYSTEM SET tcp_keepalives_count = 50;  -- ok
ALTER SYSTEM RESET tcp_keepalives_count;  -- ok
SET tcp_keepalives_idle = 50;  -- ok
RESET tcp_keepalives_idle;  -- ok
ALTER SYSTEM SET tcp_keepalives_idle = 50;  -- ok
ALTER SYSTEM RESET tcp_keepalives_idle;  -- ok
SET tcp_keepalives_interval = 50;  -- ok
RESET tcp_keepalives_interval;  -- ok
ALTER SYSTEM SET tcp_keepalives_interval = 50;  -- ok
ALTER SYSTEM RESET tcp_keepalives_interval;  -- ok
SET tcp_user_timeout = 50;  -- ok
RESET tcp_user_timeout;  -- ok
ALTER SYSTEM SET tcp_user_timeout = 50;  -- ok
ALTER SYSTEM RESET tcp_user_timeout;  -- ok
-- PGC_USERSET / CONN_AUTH_SSL
SET ssl_renegotiation_limit = 0;  -- ok
RESET ssl_renegotiation_limit;  -- ok
ALTER SYSTEM SET ssl_renegotiation_limit = 0;  -- fail, cannot be changed
ALTER SYSTEM RESET ssl_renegotiation_limit;  -- fail, cannot be changed
-- PGC_USERSET / DEVELOPER_OPTIONS
SET force_parallel_mode = 'off';  -- ok
RESET force_parallel_mode;  -- ok
ALTER SYSTEM SET force_parallel_mode = 'off';  -- ok
ALTER SYSTEM RESET force_parallel_mode;  -- ok
SET jit_expressions = OFF;  -- ok
RESET jit_expressions;  -- ok
ALTER SYSTEM SET jit_expressions = OFF;  -- ok
ALTER SYSTEM RESET jit_expressions;  -- ok
SET jit_tuple_deforming = OFF;  -- ok
RESET jit_tuple_deforming;  -- ok
ALTER SYSTEM SET jit_tuple_deforming = OFF;  -- ok
ALTER SYSTEM RESET jit_tuple_deforming;  -- ok
SET trace_notify = OFF;  -- ok
RESET trace_notify;  -- ok
ALTER SYSTEM SET trace_notify = OFF;  -- ok
ALTER SYSTEM RESET trace_notify;  -- ok
SET trace_sort = OFF;  -- ok
RESET trace_sort;  -- ok
ALTER SYSTEM SET trace_sort = OFF;  -- ok
ALTER SYSTEM RESET trace_sort;  -- ok
-- PGC_USERSET / ERROR_HANDLING_OPTIONS
SET exit_on_error = OFF;  -- ok
RESET exit_on_error;  -- ok
ALTER SYSTEM SET exit_on_error = OFF;  -- ok
ALTER SYSTEM RESET exit_on_error;  -- ok
-- PGC_USERSET / LOGGING_WHAT
SET application_name = 'psql';  -- ok
RESET application_name;  -- ok
ALTER SYSTEM SET application_name = 'psql';  -- ok
ALTER SYSTEM RESET application_name;  -- ok
SET debug_pretty_print = OFF;  -- ok
RESET debug_pretty_print;  -- ok
ALTER SYSTEM SET debug_pretty_print = OFF;  -- ok
ALTER SYSTEM RESET debug_pretty_print;  -- ok
SET debug_print_parse = OFF;  -- ok
RESET debug_print_parse;  -- ok
ALTER SYSTEM SET debug_print_parse = OFF;  -- ok
ALTER SYSTEM RESET debug_print_parse;  -- ok
SET debug_print_plan = OFF;  -- ok
RESET debug_print_plan;  -- ok
ALTER SYSTEM SET debug_print_plan = OFF;  -- ok
ALTER SYSTEM RESET debug_print_plan;  -- ok
SET debug_print_rewritten = OFF;  -- ok
RESET debug_print_rewritten;  -- ok
ALTER SYSTEM SET debug_print_rewritten = OFF;  -- ok
ALTER SYSTEM RESET debug_print_rewritten;  -- ok
SET log_parameter_max_length_on_error = 50;  -- ok
RESET log_parameter_max_length_on_error;  -- ok
ALTER SYSTEM SET log_parameter_max_length_on_error = 50;  -- ok
ALTER SYSTEM RESET log_parameter_max_length_on_error;  -- ok
-- PGC_USERSET / QUERY_TUNING_COST
SET cpu_index_tuple_cost = 50;  -- ok
RESET cpu_index_tuple_cost;  -- ok
ALTER SYSTEM SET cpu_index_tuple_cost = 50;  -- ok
ALTER SYSTEM RESET cpu_index_tuple_cost;  -- ok
SET cpu_operator_cost = 50;  -- ok
RESET cpu_operator_cost;  -- ok
ALTER SYSTEM SET cpu_operator_cost = 50;  -- ok
ALTER SYSTEM RESET cpu_operator_cost;  -- ok
SET cpu_tuple_cost = 50;  -- ok
RESET cpu_tuple_cost;  -- ok
ALTER SYSTEM SET cpu_tuple_cost = 50;  -- ok
ALTER SYSTEM RESET cpu_tuple_cost;  -- ok
SET effective_cache_size = 1073741824;  -- ok
RESET effective_cache_size;  -- ok
ALTER SYSTEM SET effective_cache_size = 1073741824;  -- ok
ALTER SYSTEM RESET effective_cache_size;  -- ok
SET jit_above_cost = 50;  -- ok
RESET jit_above_cost;  -- ok
ALTER SYSTEM SET jit_above_cost = 50;  -- ok
ALTER SYSTEM RESET jit_above_cost;  -- ok
SET jit_inline_above_cost = 50;  -- ok
RESET jit_inline_above_cost;  -- ok
ALTER SYSTEM SET jit_inline_above_cost = 50;  -- ok
ALTER SYSTEM RESET jit_inline_above_cost;  -- ok
SET jit_optimize_above_cost = 50;  -- ok
RESET jit_optimize_above_cost;  -- ok
ALTER SYSTEM SET jit_optimize_above_cost = 50;  -- ok
ALTER SYSTEM RESET jit_optimize_above_cost;  -- ok
SET min_parallel_index_scan_size = 50;  -- ok
RESET min_parallel_index_scan_size;  -- ok
ALTER SYSTEM SET min_parallel_index_scan_size = 50;  -- ok
ALTER SYSTEM RESET min_parallel_index_scan_size;  -- ok
SET min_parallel_table_scan_size = 50;  -- ok
RESET min_parallel_table_scan_size;  -- ok
ALTER SYSTEM SET min_parallel_table_scan_size = 50;  -- ok
ALTER SYSTEM RESET min_parallel_table_scan_size;  -- ok
SET parallel_setup_cost = 50;  -- ok
RESET parallel_setup_cost;  -- ok
ALTER SYSTEM SET parallel_setup_cost = 50;  -- ok
ALTER SYSTEM RESET parallel_setup_cost;  -- ok
SET parallel_tuple_cost = 50;  -- ok
RESET parallel_tuple_cost;  -- ok
ALTER SYSTEM SET parallel_tuple_cost = 50;  -- ok
ALTER SYSTEM RESET parallel_tuple_cost;  -- ok
SET random_page_cost = 50;  -- ok
RESET random_page_cost;  -- ok
ALTER SYSTEM SET random_page_cost = 50;  -- ok
ALTER SYSTEM RESET random_page_cost;  -- ok
SET seq_page_cost = 50;  -- ok
RESET seq_page_cost;  -- ok
ALTER SYSTEM SET seq_page_cost = 50;  -- ok
ALTER SYSTEM RESET seq_page_cost;  -- ok
-- PGC_USERSET / QUERY_TUNING_GEQO
SET geqo = OFF;  -- ok
RESET geqo;  -- ok
ALTER SYSTEM SET geqo = OFF;  -- ok
ALTER SYSTEM RESET geqo;  -- ok
SET geqo_effort = 5;  -- ok
RESET geqo_effort;  -- ok
ALTER SYSTEM SET geqo_effort = 5;  -- ok
ALTER SYSTEM RESET geqo_effort;  -- ok
SET geqo_generations = 50;  -- ok
RESET geqo_generations;  -- ok
ALTER SYSTEM SET geqo_generations = 50;  -- ok
ALTER SYSTEM RESET geqo_generations;  -- ok
SET geqo_pool_size = 50;  -- ok
RESET geqo_pool_size;  -- ok
ALTER SYSTEM SET geqo_pool_size = 50;  -- ok
ALTER SYSTEM RESET geqo_pool_size;  -- ok
SET geqo_seed = 0;  -- ok
RESET geqo_seed;  -- ok
ALTER SYSTEM SET geqo_seed = 0;  -- ok
ALTER SYSTEM RESET geqo_seed;  -- ok
SET geqo_selection_bias = 2;  -- ok
RESET geqo_selection_bias;  -- ok
ALTER SYSTEM SET geqo_selection_bias = 2;  -- ok
ALTER SYSTEM RESET geqo_selection_bias;  -- ok
SET geqo_threshold = 1073741824;  -- ok
RESET geqo_threshold;  -- ok
ALTER SYSTEM SET geqo_threshold = 1073741824;  -- ok
ALTER SYSTEM RESET geqo_threshold;  -- ok
-- PGC_USERSET / QUERY_TUNING_METHOD
SET enable_async_append = OFF;  -- ok
RESET enable_async_append;  -- ok
ALTER SYSTEM SET enable_async_append = OFF;  -- ok
ALTER SYSTEM RESET enable_async_append;  -- ok
SET enable_bitmapscan = OFF;  -- ok
RESET enable_bitmapscan;  -- ok
ALTER SYSTEM SET enable_bitmapscan = OFF;  -- ok
ALTER SYSTEM RESET enable_bitmapscan;  -- ok
SET enable_gathermerge = OFF;  -- ok
RESET enable_gathermerge;  -- ok
ALTER SYSTEM SET enable_gathermerge = OFF;  -- ok
ALTER SYSTEM RESET enable_gathermerge;  -- ok
SET enable_hashagg = OFF;  -- ok
RESET enable_hashagg;  -- ok
ALTER SYSTEM SET enable_hashagg = OFF;  -- ok
ALTER SYSTEM RESET enable_hashagg;  -- ok
SET enable_hashjoin = OFF;  -- ok
RESET enable_hashjoin;  -- ok
ALTER SYSTEM SET enable_hashjoin = OFF;  -- ok
ALTER SYSTEM RESET enable_hashjoin;  -- ok
SET enable_incremental_sort = OFF;  -- ok
RESET enable_incremental_sort;  -- ok
ALTER SYSTEM SET enable_incremental_sort = OFF;  -- ok
ALTER SYSTEM RESET enable_incremental_sort;  -- ok
SET enable_indexonlyscan = OFF;  -- ok
RESET enable_indexonlyscan;  -- ok
ALTER SYSTEM SET enable_indexonlyscan = OFF;  -- ok
ALTER SYSTEM RESET enable_indexonlyscan;  -- ok
SET enable_indexscan = OFF;  -- ok
RESET enable_indexscan;  -- ok
ALTER SYSTEM SET enable_indexscan = OFF;  -- ok
ALTER SYSTEM RESET enable_indexscan;  -- ok
SET enable_material = OFF;  -- ok
RESET enable_material;  -- ok
ALTER SYSTEM SET enable_material = OFF;  -- ok
ALTER SYSTEM RESET enable_material;  -- ok
SET enable_memoize = OFF;  -- ok
RESET enable_memoize;  -- ok
ALTER SYSTEM SET enable_memoize = OFF;  -- ok
ALTER SYSTEM RESET enable_memoize;  -- ok
SET enable_mergejoin = OFF;  -- ok
RESET enable_mergejoin;  -- ok
ALTER SYSTEM SET enable_mergejoin = OFF;  -- ok
ALTER SYSTEM RESET enable_mergejoin;  -- ok
SET enable_nestloop = OFF;  -- ok
RESET enable_nestloop;  -- ok
ALTER SYSTEM SET enable_nestloop = OFF;  -- ok
ALTER SYSTEM RESET enable_nestloop;  -- ok
SET enable_parallel_append = OFF;  -- ok
RESET enable_parallel_append;  -- ok
ALTER SYSTEM SET enable_parallel_append = OFF;  -- ok
ALTER SYSTEM RESET enable_parallel_append;  -- ok
SET enable_parallel_hash = OFF;  -- ok
RESET enable_parallel_hash;  -- ok
ALTER SYSTEM SET enable_parallel_hash = OFF;  -- ok
ALTER SYSTEM RESET enable_parallel_hash;  -- ok
SET enable_partition_pruning = OFF;  -- ok
RESET enable_partition_pruning;  -- ok
ALTER SYSTEM SET enable_partition_pruning = OFF;  -- ok
ALTER SYSTEM RESET enable_partition_pruning;  -- ok
SET enable_partitionwise_aggregate = OFF;  -- ok
RESET enable_partitionwise_aggregate;  -- ok
ALTER SYSTEM SET enable_partitionwise_aggregate = OFF;  -- ok
ALTER SYSTEM RESET enable_partitionwise_aggregate;  -- ok
SET enable_partitionwise_join = OFF;  -- ok
RESET enable_partitionwise_join;  -- ok
ALTER SYSTEM SET enable_partitionwise_join = OFF;  -- ok
ALTER SYSTEM RESET enable_partitionwise_join;  -- ok
SET enable_seqscan = OFF;  -- ok
RESET enable_seqscan;  -- ok
ALTER SYSTEM SET enable_seqscan = OFF;  -- ok
ALTER SYSTEM RESET enable_seqscan;  -- ok
SET enable_sort = OFF;  -- ok
RESET enable_sort;  -- ok
ALTER SYSTEM SET enable_sort = OFF;  -- ok
ALTER SYSTEM RESET enable_sort;  -- ok
SET enable_tidscan = OFF;  -- ok
RESET enable_tidscan;  -- ok
ALTER SYSTEM SET enable_tidscan = OFF;  -- ok
ALTER SYSTEM RESET enable_tidscan;  -- ok
-- PGC_USERSET / QUERY_TUNING_OTHER
SET constraint_exclusion = 'partition';  -- ok
RESET constraint_exclusion;  -- ok
ALTER SYSTEM SET constraint_exclusion = 'partition';  -- ok
ALTER SYSTEM RESET constraint_exclusion;  -- ok
SET cursor_tuple_fraction = 0;  -- ok
RESET cursor_tuple_fraction;  -- ok
ALTER SYSTEM SET cursor_tuple_fraction = 0;  -- ok
ALTER SYSTEM RESET cursor_tuple_fraction;  -- ok
SET default_statistics_target = 5000;  -- ok
RESET default_statistics_target;  -- ok
ALTER SYSTEM SET default_statistics_target = 5000;  -- ok
ALTER SYSTEM RESET default_statistics_target;  -- ok
SET from_collapse_limit = 1073741824;  -- ok
RESET from_collapse_limit;  -- ok
ALTER SYSTEM SET from_collapse_limit = 1073741824;  -- ok
ALTER SYSTEM RESET from_collapse_limit;  -- ok
SET jit = OFF;  -- ok
RESET jit;  -- ok
ALTER SYSTEM SET jit = OFF;  -- ok
ALTER SYSTEM RESET jit;  -- ok
SET join_collapse_limit = 1073741824;  -- ok
RESET join_collapse_limit;  -- ok
ALTER SYSTEM SET join_collapse_limit = 1073741824;  -- ok
ALTER SYSTEM RESET join_collapse_limit;  -- ok
SET plan_cache_mode = 'force_generic_plan';  -- ok
RESET plan_cache_mode;  -- ok
ALTER SYSTEM SET plan_cache_mode = 'force_generic_plan';  -- ok
ALTER SYSTEM RESET plan_cache_mode;  -- ok
-- PGC_USERSET / REPLICATION_SENDING
SET wal_sender_timeout = 50;  -- ok
RESET wal_sender_timeout;  -- ok
ALTER SYSTEM SET wal_sender_timeout = 50;  -- ok
ALTER SYSTEM RESET wal_sender_timeout;  -- ok
-- PGC_USERSET / RESOURCES_ASYNCHRONOUS
SET backend_flush_after = 128;  -- ok
RESET backend_flush_after;  -- ok
ALTER SYSTEM SET backend_flush_after = 128;  -- ok
ALTER SYSTEM RESET backend_flush_after;  -- ok
SET effective_io_concurrency = 0;  -- ok
RESET effective_io_concurrency;  -- ok
ALTER SYSTEM SET effective_io_concurrency = 0;  -- ok
ALTER SYSTEM RESET effective_io_concurrency;  -- ok
SET maintenance_io_concurrency = 0;  -- ok
RESET maintenance_io_concurrency;  -- ok
ALTER SYSTEM SET maintenance_io_concurrency = 0;  -- ok
ALTER SYSTEM RESET maintenance_io_concurrency;  -- ok
SET max_parallel_maintenance_workers = 50;  -- ok
RESET max_parallel_maintenance_workers;  -- ok
ALTER SYSTEM SET max_parallel_maintenance_workers = 50;  -- ok
ALTER SYSTEM RESET max_parallel_maintenance_workers;  -- ok
SET max_parallel_workers = 50;  -- ok
RESET max_parallel_workers;  -- ok
ALTER SYSTEM SET max_parallel_workers = 50;  -- ok
ALTER SYSTEM RESET max_parallel_workers;  -- ok
SET max_parallel_workers_per_gather = 50;  -- ok
RESET max_parallel_workers_per_gather;  -- ok
ALTER SYSTEM SET max_parallel_workers_per_gather = 50;  -- ok
ALTER SYSTEM RESET max_parallel_workers_per_gather;  -- ok
SET parallel_leader_participation = OFF;  -- ok
RESET parallel_leader_participation;  -- ok
ALTER SYSTEM SET parallel_leader_participation = OFF;  -- ok
ALTER SYSTEM RESET parallel_leader_participation;  -- ok
-- PGC_USERSET / RESOURCES_MEM
SET hash_mem_multiplier = 500;  -- ok
RESET hash_mem_multiplier;  -- ok
ALTER SYSTEM SET hash_mem_multiplier = 500;  -- ok
ALTER SYSTEM RESET hash_mem_multiplier;  -- ok
SET logical_decoding_work_mem = 1073741855;  -- ok
RESET logical_decoding_work_mem;  -- ok
ALTER SYSTEM SET logical_decoding_work_mem = 1073741855;  -- ok
ALTER SYSTEM RESET logical_decoding_work_mem;  -- ok
SET maintenance_work_mem = 1073742335;  -- ok
RESET maintenance_work_mem;  -- ok
ALTER SYSTEM SET maintenance_work_mem = 1073742335;  -- ok
ALTER SYSTEM RESET maintenance_work_mem;  -- ok
SET temp_buffers = 536870961;  -- ok
RESET temp_buffers;  -- ok
ALTER SYSTEM SET temp_buffers = 536870961;  -- ok
ALTER SYSTEM RESET temp_buffers;  -- ok
SET work_mem = 1073741855;  -- ok
RESET work_mem;  -- ok
ALTER SYSTEM SET work_mem = 1073741855;  -- ok
ALTER SYSTEM RESET work_mem;  -- ok
-- PGC_USERSET / RESOURCES_VACUUM_DELAY
SET vacuum_cost_delay = 50;  -- ok
RESET vacuum_cost_delay;  -- ok
ALTER SYSTEM SET vacuum_cost_delay = 50;  -- ok
ALTER SYSTEM RESET vacuum_cost_delay;  -- ok
SET vacuum_cost_limit = 5000;  -- ok
RESET vacuum_cost_limit;  -- ok
ALTER SYSTEM SET vacuum_cost_limit = 5000;  -- ok
ALTER SYSTEM RESET vacuum_cost_limit;  -- ok
SET vacuum_cost_page_dirty = 50;  -- ok
RESET vacuum_cost_page_dirty;  -- ok
ALTER SYSTEM SET vacuum_cost_page_dirty = 50;  -- ok
ALTER SYSTEM RESET vacuum_cost_page_dirty;  -- ok
SET vacuum_cost_page_hit = 50;  -- ok
RESET vacuum_cost_page_hit;  -- ok
ALTER SYSTEM SET vacuum_cost_page_hit = 50;  -- ok
ALTER SYSTEM RESET vacuum_cost_page_hit;  -- ok
SET vacuum_cost_page_miss = 50;  -- ok
RESET vacuum_cost_page_miss;  -- ok
ALTER SYSTEM SET vacuum_cost_page_miss = 50;  -- ok
ALTER SYSTEM RESET vacuum_cost_page_miss;  -- ok
-- PGC_USERSET / UNGROUPED
SET seed = 0;  -- ok
RESET seed;  -- ok
ALTER SYSTEM SET seed = 0;  -- fail, cannot be changed
ALTER SYSTEM RESET seed;  -- fail, cannot be changed
-- PGC_USERSET / WAL_SETTINGS
SET commit_siblings = 50;  -- ok
RESET commit_siblings;  -- ok
ALTER SYSTEM SET commit_siblings = 50;  -- ok
ALTER SYSTEM RESET commit_siblings;  -- ok
SET synchronous_commit = 'remote_write';  -- ok
RESET synchronous_commit;  -- ok
ALTER SYSTEM SET synchronous_commit = 'remote_write';  -- ok
ALTER SYSTEM RESET synchronous_commit;  -- ok
SET wal_skip_threshold = 50;  -- ok
RESET wal_skip_threshold;  -- ok
ALTER SYSTEM SET wal_skip_threshold = 50;  -- ok
ALTER SYSTEM RESET wal_skip_threshold;  -- ok
RESET statement_timeout;
RESET SESSION AUTHORIZATION;
DROP ROLE regress_admin;
