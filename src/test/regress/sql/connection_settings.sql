-- Non-superuser with privileges to configure connections and authentication
CREATE ROLE regress_connection_admin NOSUPERUSER;
GRANT pg_manage_connection_settings TO regress_connection_admin;
-- Perform all operations as user 'regress_connection_admin' --
SET SESSION AUTHORIZATION regress_connection_admin;
-- PGC_BACKEND / DEVELOPER_OPTIONS
ALTER SYSTEM SET ignore_system_indexes = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET ignore_system_indexes;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET post_auth_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET post_auth_delay;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_INTERNAL / PRESET_OPTIONS
-- PGC_INTERNAL / UNGROUPED
-- PGC_POSTMASTER / AUTOVACUUM
ALTER SYSTEM SET autovacuum_freeze_max_age = 1000050000;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_freeze_max_age;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_max_workers = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_max_workers;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_multixact_freeze_max_age = 1000005000;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_multixact_freeze_max_age;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / CLIENT_CONN_PRELOAD
ALTER SYSTEM SET jit_provider = 'llvmjit';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_provider;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET shared_preload_libraries = 'iconv, pcre';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET shared_preload_libraries;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / CONN_AUTH_SETTINGS
ALTER SYSTEM SET bonjour = OFF;  -- ok
ALTER SYSTEM RESET bonjour;  -- ok
ALTER SYSTEM SET bonjour_name = 'BonneNuit';  -- ok
ALTER SYSTEM RESET bonjour_name;  -- ok
ALTER SYSTEM SET listen_addresses = 'localhost';  -- ok
ALTER SYSTEM RESET listen_addresses;  -- ok
ALTER SYSTEM SET max_connections = 50;  -- ok
ALTER SYSTEM RESET max_connections;  -- ok
ALTER SYSTEM SET port = 50;  -- ok
ALTER SYSTEM RESET port;  -- ok
ALTER SYSTEM SET superuser_reserved_connections = 50;  -- ok
ALTER SYSTEM RESET superuser_reserved_connections;  -- ok
ALTER SYSTEM SET unix_socket_directories = '/tmp';  -- ok
ALTER SYSTEM RESET unix_socket_directories;  -- ok
ALTER SYSTEM SET unix_socket_group = 'tenant';  -- ok
ALTER SYSTEM RESET unix_socket_group;  -- ok
ALTER SYSTEM SET unix_socket_permissions = 50;  -- ok
ALTER SYSTEM RESET unix_socket_permissions;  -- ok
-- PGC_POSTMASTER / DEVELOPER_OPTIONS
ALTER SYSTEM SET ignore_invalid_pages = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET ignore_invalid_pages;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / ERROR_HANDLING_OPTIONS
ALTER SYSTEM SET data_sync_retry = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET data_sync_retry;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / FILE_LOCATIONS
ALTER SYSTEM SET external_pid_file = '/var/postgres/master.pid';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET external_pid_file;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / LOCK_MANAGEMENT
ALTER SYSTEM SET max_locks_per_transaction = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_locks_per_transaction;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_pred_locks_per_transaction = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_pred_locks_per_transaction;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / LOGGING_WHERE
ALTER SYSTEM SET event_source = 'PostgreSQL';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET event_source;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET logging_collector = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET logging_collector;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / PROCESS_TITLE
ALTER SYSTEM SET cluster_name = 'BonCluster';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET cluster_name;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / REPLICATION_SENDING
ALTER SYSTEM SET max_replication_slots = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_replication_slots;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_wal_senders = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_wal_senders;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET track_commit_timestamp = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET track_commit_timestamp;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / REPLICATION_STANDBY
ALTER SYSTEM SET hot_standby = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET hot_standby;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / REPLICATION_SUBSCRIBERS
ALTER SYSTEM SET max_logical_replication_workers = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_logical_replication_workers;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / RESOURCES_ASYNCHRONOUS
ALTER SYSTEM SET max_worker_processes = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_worker_processes;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET old_snapshot_threshold = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET old_snapshot_threshold;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / RESOURCES_KERNEL
ALTER SYSTEM SET max_files_per_process = 1073741855;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_files_per_process;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / RESOURCES_MEM
ALTER SYSTEM SET dynamic_shared_memory_type = 'posix';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET dynamic_shared_memory_type;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET huge_pages = 'try';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET huge_pages;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_prepared_transactions = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_prepared_transactions;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET min_dynamic_shared_memory = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET min_dynamic_shared_memory;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET shared_buffers = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET shared_buffers;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET shared_memory_type = 'mmap';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET shared_memory_type;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / STATS_COLLECTOR
ALTER SYSTEM SET track_activity_query_size = 524338;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET track_activity_query_size;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / WAL_ARCHIVING
ALTER SYSTEM SET archive_mode = 'off';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET archive_mode;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / WAL_RECOVERY_TARGET
ALTER SYSTEM SET recovery_target_action = 'pause';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_target_action;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_target_inclusive = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_target_inclusive;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_target_lsn = '16/B374D848';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_target_lsn;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_target_name = 'BonPoint';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_target_name;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_target_time = '2001-02-03 04:05:06.789 Europe/Helsinki';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_target_time;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_target_timeline = 'latest';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_target_timeline;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_target_xid = '12345678';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_target_xid;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_POSTMASTER / WAL_SETTINGS
ALTER SYSTEM SET wal_buffers = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_buffers;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_level = 'replica';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_level;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_log_hints = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_log_hints;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / AUTOVACUUM
ALTER SYSTEM SET autovacuum = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_analyze_scale_factor = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_analyze_scale_factor;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_analyze_threshold = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_analyze_threshold;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_naptime = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_naptime;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_vacuum_cost_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_vacuum_cost_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_vacuum_cost_limit = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_vacuum_cost_limit;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_vacuum_insert_scale_factor = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_vacuum_insert_scale_factor;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_vacuum_insert_threshold = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_vacuum_insert_threshold;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_vacuum_scale_factor;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET autovacuum_vacuum_threshold = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_vacuum_threshold;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / CONN_AUTH_AUTH
ALTER SYSTEM SET authentication_timeout = 50;  -- ok
ALTER SYSTEM RESET authentication_timeout;  -- ok
ALTER SYSTEM SET db_user_namespace = OFF;  -- ok
ALTER SYSTEM RESET db_user_namespace;  -- ok
ALTER SYSTEM SET krb_caseins_users = OFF;  -- ok
ALTER SYSTEM RESET krb_caseins_users;  -- ok
ALTER SYSTEM SET krb_server_keyfile = 'krb/server.key';  -- ok
ALTER SYSTEM RESET krb_server_keyfile;  -- ok
-- PGC_SIGHUP / CONN_AUTH_SSL
ALTER SYSTEM SET ssl_ca_file = 'ca/server.ca';  -- ok
ALTER SYSTEM RESET ssl_ca_file;  -- ok
ALTER SYSTEM SET ssl_cert_file = 'crt/server.crt';  -- ok
ALTER SYSTEM RESET ssl_cert_file;  -- ok
ALTER SYSTEM SET ssl_ciphers = 'none';  -- ok
ALTER SYSTEM RESET ssl_ciphers;  -- ok
ALTER SYSTEM SET ssl_crl_dir = 'crl/';  -- ok
ALTER SYSTEM RESET ssl_crl_dir;  -- ok
ALTER SYSTEM SET ssl_crl_file = 'crl/server.crl';  -- ok
ALTER SYSTEM RESET ssl_crl_file;  -- ok
ALTER SYSTEM SET ssl_dh_params_file = 'ssl/params';  -- ok
ALTER SYSTEM RESET ssl_dh_params_file;  -- ok
ALTER SYSTEM SET ssl_ecdh_curve = 'none';  -- ok
ALTER SYSTEM RESET ssl_ecdh_curve;  -- ok
ALTER SYSTEM SET ssl_key_file = 'crl/server.key';  -- ok
ALTER SYSTEM RESET ssl_key_file;  -- ok
ALTER SYSTEM SET ssl_min_protocol_version = 'TLSv1.2';  -- ok
ALTER SYSTEM RESET ssl_min_protocol_version;  -- ok
ALTER SYSTEM SET ssl_passphrase_command = '/bin/passphrase';  -- ok
ALTER SYSTEM RESET ssl_passphrase_command;  -- ok
ALTER SYSTEM SET ssl_passphrase_command_supports_reload = OFF;  -- ok
ALTER SYSTEM RESET ssl_passphrase_command_supports_reload;  -- ok
ALTER SYSTEM SET ssl_prefer_server_ciphers = OFF;  -- ok
ALTER SYSTEM RESET ssl_prefer_server_ciphers;  -- ok
-- PGC_SIGHUP / DEVELOPER_OPTIONS
ALTER SYSTEM SET pre_auth_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET pre_auth_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET remove_temp_files_after_crash = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET remove_temp_files_after_crash;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET trace_recovery_messages = 'log';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET trace_recovery_messages;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / ERROR_HANDLING_OPTIONS
ALTER SYSTEM SET recovery_init_sync_method = 'fsync';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_init_sync_method;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET restart_after_crash = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET restart_after_crash;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / LOCK_MANAGEMENT
ALTER SYSTEM SET max_pred_locks_per_page = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_pred_locks_per_page;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_pred_locks_per_relation = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_pred_locks_per_relation;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / LOGGING_WHAT
ALTER SYSTEM SET log_autovacuum_min_duration = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_autovacuum_min_duration;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_checkpoints = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_checkpoints;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_hostname = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_hostname;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_line_prefix = '%m [%p] ';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_line_prefix;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_recovery_conflict_waits = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_recovery_conflict_waits;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_timezone = 'Europe/Helsinki';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_timezone;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / LOGGING_WHERE
ALTER SYSTEM SET log_directory = 'log';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_directory;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_file_mode = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_file_mode;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_filename;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_rotation_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_rotation_age;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_rotation_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_rotation_size;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_truncate_on_rotation = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_truncate_on_rotation;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET syslog_ident = 'postgres';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET syslog_ident;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET syslog_sequence_numbers = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET syslog_sequence_numbers;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET syslog_split_messages = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET syslog_split_messages;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / REPLICATION_PRIMARY
ALTER SYSTEM SET synchronous_standby_names = 'fee, fi, fo, fum';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET synchronous_standby_names;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET vacuum_defer_cleanup_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_defer_cleanup_age;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / REPLICATION_SENDING
ALTER SYSTEM SET max_slot_wal_keep_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_slot_wal_keep_size;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_keep_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_keep_size;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / REPLICATION_STANDBY
ALTER SYSTEM SET hot_standby_feedback = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET hot_standby_feedback;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_standby_archive_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_standby_archive_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_standby_streaming_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_standby_streaming_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET primary_conninfo = 'postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET primary_conninfo;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET primary_slot_name = 'bonne_fente';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET primary_slot_name;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET promote_trigger_file = 'promote.trigger';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET promote_trigger_file;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_min_apply_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_min_apply_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_receiver_create_temp_slot = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_receiver_create_temp_slot;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_receiver_status_interval = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_receiver_status_interval;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_receiver_timeout = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_receiver_timeout;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_retrieve_retry_interval = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_retrieve_retry_interval;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / REPLICATION_SUBSCRIBERS
ALTER SYSTEM SET max_sync_workers_per_subscription = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_sync_workers_per_subscription;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / RESOURCES_BGWRITER
ALTER SYSTEM SET bgwriter_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET bgwriter_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET bgwriter_flush_after = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET bgwriter_flush_after;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET bgwriter_lru_maxpages = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET bgwriter_lru_maxpages;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET bgwriter_lru_multiplier = 5;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET bgwriter_lru_multiplier;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / RESOURCES_MEM
ALTER SYSTEM SET autovacuum_work_mem = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET autovacuum_work_mem;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / STATS_COLLECTOR
ALTER SYSTEM SET stats_temp_directory = 'pg_stat_tmp';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET stats_temp_directory;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / WAL_ARCHIVE_RECOVERY
ALTER SYSTEM SET archive_cleanup_command = '/bin/cleanup my stuff';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET archive_cleanup_command;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET recovery_end_command = '/bin/recover my stuff';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET recovery_end_command;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET restore_command = '/bin/restore my stuff';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET restore_command;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / WAL_ARCHIVING
ALTER SYSTEM SET archive_command = '/bin/archive my stuff';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET archive_command;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET archive_timeout = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET archive_timeout;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / WAL_CHECKPOINTS
ALTER SYSTEM SET checkpoint_warning = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET checkpoint_warning;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET checkpoint_completion_target = 0;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET checkpoint_completion_target;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET checkpoint_flush_after = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET checkpoint_flush_after;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET checkpoint_timeout = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET checkpoint_timeout;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_wal_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_wal_size;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET min_wal_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET min_wal_size;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SIGHUP / WAL_SETTINGS
ALTER SYSTEM SET fsync = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET fsync;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET full_page_writes = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET full_page_writes;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_sync_method = 'open_datasync';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_sync_method;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_writer_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_writer_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_writer_flush_after = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_writer_flush_after;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / CLIENT_CONN_LOCALE
SET lc_messages = 'en_US.UTF-8';  -- fail, regress_connection_admin has insufficient privileges
RESET lc_messages;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET lc_messages = 'en_US.UTF-8';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET lc_messages;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / CLIENT_CONN_OTHER
SET dynamic_library_path = '$libdir';  -- fail, regress_connection_admin has insufficient privileges
RESET dynamic_library_path;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET dynamic_library_path = '$libdir';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET dynamic_library_path;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / CLIENT_CONN_PRELOAD
SET session_preload_libraries = 'gssapi_krb5';  -- fail, regress_connection_admin has insufficient privileges
RESET session_preload_libraries;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET session_preload_libraries = 'gssapi_krb5';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET session_preload_libraries;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / CLIENT_CONN_STATEMENT
SET session_replication_role = 'origin';  -- fail, regress_connection_admin has insufficient privileges
RESET session_replication_role;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET session_replication_role = 'origin';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET session_replication_role;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / COMPAT_OPTIONS_PREVIOUS
SET lo_compat_privileges = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET lo_compat_privileges;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET lo_compat_privileges = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET lo_compat_privileges;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / DEVELOPER_OPTIONS
SET allow_system_table_mods = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET allow_system_table_mods;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET allow_system_table_mods = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET allow_system_table_mods;  -- fail, regress_connection_admin has insufficient privileges
SET backtrace_functions = 'partition_list_bsearch,partition_range_datum_bsearch,partition_hash_bsearch';  -- fail, regress_connection_admin has insufficient privileges
RESET backtrace_functions;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET backtrace_functions = 'partition_list_bsearch,partition_range_datum_bsearch,partition_hash_bsearch';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET backtrace_functions;  -- fail, regress_connection_admin has insufficient privileges
SET debug_discard_caches = 2;  -- fail, regress_connection_admin has insufficient privileges
RESET debug_discard_caches;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET debug_discard_caches = 2;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET debug_discard_caches;  -- fail, regress_connection_admin has insufficient privileges
SET ignore_checksum_failure = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET ignore_checksum_failure;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET ignore_checksum_failure = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET ignore_checksum_failure;  -- fail, regress_connection_admin has insufficient privileges
SET jit_dump_bitcode = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET jit_dump_bitcode;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET jit_dump_bitcode = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_dump_bitcode;  -- fail, regress_connection_admin has insufficient privileges
SET wal_consistency_checking = 'heap, heap2, btree, hash, gin, gist, sequence, spgist, brin, generic';  -- fail, regress_connection_admin has insufficient privileges
RESET wal_consistency_checking;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_consistency_checking = 'heap, heap2, btree, hash, gin, gist, sequence, spgist, brin, generic';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_consistency_checking;  -- fail, regress_connection_admin has insufficient privileges
SET zero_damaged_pages = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET zero_damaged_pages;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET zero_damaged_pages = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET zero_damaged_pages;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / LOCK_MANAGEMENT
SET deadlock_timeout = 1073741824;  -- fail, regress_connection_admin has insufficient privileges
RESET deadlock_timeout;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET deadlock_timeout = 1073741824;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET deadlock_timeout;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / LOGGING_WHAT
SET log_duration = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET log_duration;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_duration = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_duration;  -- fail, regress_connection_admin has insufficient privileges
SET log_error_verbosity = 'default';  -- fail, regress_connection_admin has insufficient privileges
RESET log_error_verbosity;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_error_verbosity = 'default';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_error_verbosity;  -- fail, regress_connection_admin has insufficient privileges
SET log_lock_waits = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET log_lock_waits;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_lock_waits = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_lock_waits;  -- fail, regress_connection_admin has insufficient privileges
SET log_parameter_max_length = 50;  -- fail, regress_connection_admin has insufficient privileges
RESET log_parameter_max_length;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_parameter_max_length = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_parameter_max_length;  -- fail, regress_connection_admin has insufficient privileges
SET log_replication_commands = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET log_replication_commands;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_replication_commands = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_replication_commands;  -- fail, regress_connection_admin has insufficient privileges
SET log_statement = 'none';  -- fail, regress_connection_admin has insufficient privileges
RESET log_statement;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_statement = 'none';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_statement;  -- fail, regress_connection_admin has insufficient privileges
SET log_temp_files = 50;  -- fail, regress_connection_admin has insufficient privileges
RESET log_temp_files;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_temp_files = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_temp_files;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / LOGGING_WHEN
SET log_min_duration_sample = 50;  -- fail, regress_connection_admin has insufficient privileges
RESET log_min_duration_sample;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_min_duration_sample = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_min_duration_sample;  -- fail, regress_connection_admin has insufficient privileges
SET log_min_duration_statement = 50;  -- fail, regress_connection_admin has insufficient privileges
RESET log_min_duration_statement;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_min_duration_statement = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_min_duration_statement;  -- fail, regress_connection_admin has insufficient privileges
SET log_min_error_statement = 'error';  -- fail, regress_connection_admin has insufficient privileges
RESET log_min_error_statement;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_min_error_statement = 'error';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_min_error_statement;  -- fail, regress_connection_admin has insufficient privileges
SET log_min_messages = 'warning';  -- fail, regress_connection_admin has insufficient privileges
RESET log_min_messages;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_min_messages = 'warning';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_min_messages;  -- fail, regress_connection_admin has insufficient privileges
SET log_statement_sample_rate = 0;  -- fail, regress_connection_admin has insufficient privileges
RESET log_statement_sample_rate;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_statement_sample_rate = 0;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_statement_sample_rate;  -- fail, regress_connection_admin has insufficient privileges
SET log_transaction_sample_rate = 0;  -- fail, regress_connection_admin has insufficient privileges
RESET log_transaction_sample_rate;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_transaction_sample_rate = 0;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_transaction_sample_rate;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / PROCESS_TITLE
SET update_process_title = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET update_process_title;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET update_process_title = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET update_process_title;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / RESOURCES_DISK
SET temp_file_limit = 50;  -- fail, regress_connection_admin has insufficient privileges
RESET temp_file_limit;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET temp_file_limit = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET temp_file_limit;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / RESOURCES_MEM
SET max_stack_depth = 3890;  -- fail, regress_connection_admin has insufficient privileges
RESET max_stack_depth;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET max_stack_depth = 3890;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_stack_depth;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / STATS_COLLECTOR
SET track_activities = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET track_activities;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET track_activities = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET track_activities;  -- fail, regress_connection_admin has insufficient privileges
SET track_counts = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET track_counts;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET track_counts = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET track_counts;  -- fail, regress_connection_admin has insufficient privileges
SET track_functions = 'none';  -- fail, regress_connection_admin has insufficient privileges
RESET track_functions;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET track_functions = 'none';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET track_functions;  -- fail, regress_connection_admin has insufficient privileges
SET track_io_timing = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET track_io_timing;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET track_io_timing = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET track_io_timing;  -- fail, regress_connection_admin has insufficient privileges
SET track_wal_io_timing = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET track_wal_io_timing;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET track_wal_io_timing = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET track_wal_io_timing;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / STATS_MONITORING
SET log_executor_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET log_executor_stats;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_executor_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_executor_stats;  -- fail, regress_connection_admin has insufficient privileges
SET log_parser_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET log_parser_stats;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_parser_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_parser_stats;  -- fail, regress_connection_admin has insufficient privileges
SET log_planner_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET log_planner_stats;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_planner_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_planner_stats;  -- fail, regress_connection_admin has insufficient privileges
SET log_statement_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET log_statement_stats;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_statement_stats = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_statement_stats;  -- fail, regress_connection_admin has insufficient privileges
SET compute_query_id = 'auto';  -- fail, regress_connection_admin has insufficient privileges
RESET compute_query_id;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET compute_query_id = 'auto';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET compute_query_id;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SUSET / WAL_SETTINGS
SET commit_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
RESET commit_delay;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET commit_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET commit_delay;  -- fail, regress_connection_admin has insufficient privileges
SET wal_compression = 'pglz';  -- fail, regress_connection_admin has insufficient privileges
RESET wal_compression;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_compression = 'pglz';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_compression;  -- fail, regress_connection_admin has insufficient privileges
SET wal_init_zero = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET wal_init_zero;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_init_zero = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_init_zero;  -- fail, regress_connection_admin has insufficient privileges
SET wal_recycle = OFF;  -- fail, regress_connection_admin has insufficient privileges
RESET wal_recycle;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET wal_recycle = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_recycle;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SU_BACKEND / DEVELOPER_OPTIONS
ALTER SYSTEM SET jit_debugging_support = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_debugging_support;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET jit_profiling_support = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_profiling_support;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_SU_BACKEND / LOGGING_WHAT
ALTER SYSTEM SET log_connections = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_connections;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM SET log_disconnections = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_disconnections;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / CLIENT_CONN_LOCALE
SET DateStyle = 'ISO, MDY';  -- ok
RESET DateStyle;  -- ok
ALTER SYSTEM SET DateStyle = 'ISO, MDY';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET DateStyle;  -- fail, regress_connection_admin has insufficient privileges
SET IntervalStyle = 'postgres';  -- ok
RESET IntervalStyle;  -- ok
ALTER SYSTEM SET IntervalStyle = 'postgres';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET IntervalStyle;  -- fail, regress_connection_admin has insufficient privileges
SET TimeZone = 'Europe/Helsinki';  -- ok
RESET TimeZone;  -- ok
ALTER SYSTEM SET TimeZone = 'Europe/Helsinki';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET TimeZone;  -- fail, regress_connection_admin has insufficient privileges
SET client_encoding = 'UTF8';  -- ok
RESET client_encoding;  -- ok
ALTER SYSTEM SET client_encoding = 'UTF8';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET client_encoding;  -- fail, regress_connection_admin has insufficient privileges
SET default_text_search_config = 'pg_catalog.english';  -- ok
RESET default_text_search_config;  -- ok
ALTER SYSTEM SET default_text_search_config = 'pg_catalog.english';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET default_text_search_config;  -- fail, regress_connection_admin has insufficient privileges
SET extra_float_digits = -6;  -- ok
RESET extra_float_digits;  -- ok
ALTER SYSTEM SET extra_float_digits = -6;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET extra_float_digits;  -- fail, regress_connection_admin has insufficient privileges
SET lc_monetary = 'en_US.UTF-8';  -- ok
RESET lc_monetary;  -- ok
ALTER SYSTEM SET lc_monetary = 'en_US.UTF-8';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET lc_monetary;  -- fail, regress_connection_admin has insufficient privileges
SET lc_numeric = 'en_US.UTF-8';  -- ok
RESET lc_numeric;  -- ok
ALTER SYSTEM SET lc_numeric = 'en_US.UTF-8';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET lc_numeric;  -- fail, regress_connection_admin has insufficient privileges
SET lc_time = 'en_US.UTF-8';  -- ok
RESET lc_time;  -- ok
ALTER SYSTEM SET lc_time = 'en_US.UTF-8';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET lc_time;  -- fail, regress_connection_admin has insufficient privileges
SET timezone_abbreviations = 'Default';  -- ok
RESET timezone_abbreviations;  -- ok
ALTER SYSTEM SET timezone_abbreviations = 'Default';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET timezone_abbreviations;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / CLIENT_CONN_OTHER
SET gin_fuzzy_search_limit = 50;  -- ok
RESET gin_fuzzy_search_limit;  -- ok
ALTER SYSTEM SET gin_fuzzy_search_limit = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET gin_fuzzy_search_limit;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / CLIENT_CONN_PRELOAD
SET local_preload_libraries = 'gssapi_krb5';  -- ok
RESET local_preload_libraries;  -- ok
ALTER SYSTEM SET local_preload_libraries = 'gssapi_krb5';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET local_preload_libraries;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / CLIENT_CONN_STATEMENT
SET bytea_output = 'hex';  -- ok
RESET bytea_output;  -- ok
ALTER SYSTEM SET bytea_output = 'hex';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET bytea_output;  -- fail, regress_connection_admin has insufficient privileges
SET check_function_bodies = OFF;  -- ok
RESET check_function_bodies;  -- ok
ALTER SYSTEM SET check_function_bodies = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET check_function_bodies;  -- fail, regress_connection_admin has insufficient privileges
SET client_min_messages = 'notice';  -- ok
RESET client_min_messages;  -- ok
ALTER SYSTEM SET client_min_messages = 'notice';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET client_min_messages;  -- fail, regress_connection_admin has insufficient privileges
SET default_table_access_method = 'heap';  -- ok
RESET default_table_access_method;  -- ok
ALTER SYSTEM SET default_table_access_method = 'heap';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET default_table_access_method;  -- fail, regress_connection_admin has insufficient privileges
SET default_toast_compression = 'pglz';  -- ok
RESET default_toast_compression;  -- ok
ALTER SYSTEM SET default_toast_compression = 'pglz';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET default_toast_compression;  -- fail, regress_connection_admin has insufficient privileges
SET default_transaction_deferrable = OFF;  -- ok
RESET default_transaction_deferrable;  -- ok
ALTER SYSTEM SET default_transaction_deferrable = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET default_transaction_deferrable;  -- fail, regress_connection_admin has insufficient privileges
SET default_transaction_isolation = 'read committed';  -- ok
RESET default_transaction_isolation;  -- ok
ALTER SYSTEM SET default_transaction_isolation = 'read committed';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET default_transaction_isolation;  -- fail, regress_connection_admin has insufficient privileges
SET default_transaction_read_only = OFF;  -- ok
RESET default_transaction_read_only;  -- ok
ALTER SYSTEM SET default_transaction_read_only = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET default_transaction_read_only;  -- fail, regress_connection_admin has insufficient privileges
SET gin_pending_list_limit = 1073741855;  -- ok
RESET gin_pending_list_limit;  -- ok
ALTER SYSTEM SET gin_pending_list_limit = 1073741855;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET gin_pending_list_limit;  -- fail, regress_connection_admin has insufficient privileges
SET idle_in_transaction_session_timeout = 50;  -- ok
RESET idle_in_transaction_session_timeout;  -- ok
ALTER SYSTEM SET idle_in_transaction_session_timeout = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET idle_in_transaction_session_timeout;  -- fail, regress_connection_admin has insufficient privileges
SET idle_session_timeout = 50;  -- ok
RESET idle_session_timeout;  -- ok
ALTER SYSTEM SET idle_session_timeout = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET idle_session_timeout;  -- fail, regress_connection_admin has insufficient privileges
SET lock_timeout = 50;  -- ok
RESET lock_timeout;  -- ok
ALTER SYSTEM SET lock_timeout = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET lock_timeout;  -- fail, regress_connection_admin has insufficient privileges
SET row_security = OFF;  -- ok
RESET row_security;  -- ok
ALTER SYSTEM SET row_security = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET row_security;  -- fail, regress_connection_admin has insufficient privileges
SET search_path = '"$user", public';  -- ok
RESET search_path;  -- ok
ALTER SYSTEM SET search_path = '"$user", public';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET search_path;  -- fail, regress_connection_admin has insufficient privileges
SET statement_timeout = 5250;  -- ok
RESET statement_timeout;  -- ok
ALTER SYSTEM SET statement_timeout = 5250;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET statement_timeout;  -- fail, regress_connection_admin has insufficient privileges
SET transaction_deferrable = OFF;  -- ok
RESET transaction_deferrable;  -- ok
SET transaction_read_only = OFF;  -- ok
RESET transaction_read_only;  -- ok
SET vacuum_failsafe_age = 50;  -- ok
RESET vacuum_failsafe_age;  -- ok
ALTER SYSTEM SET vacuum_failsafe_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_failsafe_age;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_freeze_min_age = 50;  -- ok
RESET vacuum_freeze_min_age;  -- ok
ALTER SYSTEM SET vacuum_freeze_min_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_freeze_min_age;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_freeze_table_age = 50;  -- ok
RESET vacuum_freeze_table_age;  -- ok
ALTER SYSTEM SET vacuum_freeze_table_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_freeze_table_age;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_multixact_failsafe_age = 50;  -- ok
RESET vacuum_multixact_failsafe_age;  -- ok
ALTER SYSTEM SET vacuum_multixact_failsafe_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_multixact_failsafe_age;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_multixact_freeze_min_age = 50;  -- ok
RESET vacuum_multixact_freeze_min_age;  -- ok
ALTER SYSTEM SET vacuum_multixact_freeze_min_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_multixact_freeze_min_age;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_multixact_freeze_table_age = 50;  -- ok
RESET vacuum_multixact_freeze_table_age;  -- ok
ALTER SYSTEM SET vacuum_multixact_freeze_table_age = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_multixact_freeze_table_age;  -- fail, regress_connection_admin has insufficient privileges
SET xmlbinary = 'base64';  -- ok
RESET xmlbinary;  -- ok
ALTER SYSTEM SET xmlbinary = 'base64';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET xmlbinary;  -- fail, regress_connection_admin has insufficient privileges
SET xmloption = 'content';  -- ok
RESET xmloption;  -- ok
ALTER SYSTEM SET xmloption = 'content';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET xmloption;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / COMPAT_OPTIONS_PREVIOUS
SET escape_string_warning = OFF;  -- ok
RESET escape_string_warning;  -- ok
ALTER SYSTEM SET escape_string_warning = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET escape_string_warning;  -- fail, regress_connection_admin has insufficient privileges
SET array_nulls = OFF;  -- ok
RESET array_nulls;  -- ok
ALTER SYSTEM SET array_nulls = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET array_nulls;  -- fail, regress_connection_admin has insufficient privileges
SET quote_all_identifiers = OFF;  -- ok
RESET quote_all_identifiers;  -- ok
ALTER SYSTEM SET quote_all_identifiers = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET quote_all_identifiers;  -- fail, regress_connection_admin has insufficient privileges
SET standard_conforming_strings = OFF;  -- ok
RESET standard_conforming_strings;  -- ok
ALTER SYSTEM SET standard_conforming_strings = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET standard_conforming_strings;  -- fail, regress_connection_admin has insufficient privileges
SET synchronize_seqscans = OFF;  -- ok
RESET synchronize_seqscans;  -- ok
ALTER SYSTEM SET synchronize_seqscans = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET synchronize_seqscans;  -- fail, regress_connection_admin has insufficient privileges
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
-- PGC_USERSET / DEVELOPER_OPTIONS
SET force_parallel_mode = 'off';  -- ok
RESET force_parallel_mode;  -- ok
ALTER SYSTEM SET force_parallel_mode = 'off';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET force_parallel_mode;  -- fail, regress_connection_admin has insufficient privileges
SET jit_expressions = OFF;  -- ok
RESET jit_expressions;  -- ok
ALTER SYSTEM SET jit_expressions = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_expressions;  -- fail, regress_connection_admin has insufficient privileges
SET jit_tuple_deforming = OFF;  -- ok
RESET jit_tuple_deforming;  -- ok
ALTER SYSTEM SET jit_tuple_deforming = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_tuple_deforming;  -- fail, regress_connection_admin has insufficient privileges
SET trace_notify = OFF;  -- ok
RESET trace_notify;  -- ok
ALTER SYSTEM SET trace_notify = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET trace_notify;  -- fail, regress_connection_admin has insufficient privileges
SET trace_sort = OFF;  -- ok
RESET trace_sort;  -- ok
ALTER SYSTEM SET trace_sort = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET trace_sort;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / ERROR_HANDLING_OPTIONS
SET exit_on_error = OFF;  -- ok
RESET exit_on_error;  -- ok
ALTER SYSTEM SET exit_on_error = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET exit_on_error;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / LOGGING_WHAT
SET application_name = 'psql';  -- ok
RESET application_name;  -- ok
ALTER SYSTEM SET application_name = 'psql';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET application_name;  -- fail, regress_connection_admin has insufficient privileges
SET debug_pretty_print = OFF;  -- ok
RESET debug_pretty_print;  -- ok
ALTER SYSTEM SET debug_pretty_print = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET debug_pretty_print;  -- fail, regress_connection_admin has insufficient privileges
SET debug_print_parse = OFF;  -- ok
RESET debug_print_parse;  -- ok
ALTER SYSTEM SET debug_print_parse = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET debug_print_parse;  -- fail, regress_connection_admin has insufficient privileges
SET debug_print_plan = OFF;  -- ok
RESET debug_print_plan;  -- ok
ALTER SYSTEM SET debug_print_plan = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET debug_print_plan;  -- fail, regress_connection_admin has insufficient privileges
SET debug_print_rewritten = OFF;  -- ok
RESET debug_print_rewritten;  -- ok
ALTER SYSTEM SET debug_print_rewritten = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET debug_print_rewritten;  -- fail, regress_connection_admin has insufficient privileges
SET log_parameter_max_length_on_error = 50;  -- ok
RESET log_parameter_max_length_on_error;  -- ok
ALTER SYSTEM SET log_parameter_max_length_on_error = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET log_parameter_max_length_on_error;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / QUERY_TUNING_COST
SET cpu_index_tuple_cost = 50;  -- ok
RESET cpu_index_tuple_cost;  -- ok
ALTER SYSTEM SET cpu_index_tuple_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET cpu_index_tuple_cost;  -- fail, regress_connection_admin has insufficient privileges
SET cpu_operator_cost = 50;  -- ok
RESET cpu_operator_cost;  -- ok
ALTER SYSTEM SET cpu_operator_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET cpu_operator_cost;  -- fail, regress_connection_admin has insufficient privileges
SET cpu_tuple_cost = 50;  -- ok
RESET cpu_tuple_cost;  -- ok
ALTER SYSTEM SET cpu_tuple_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET cpu_tuple_cost;  -- fail, regress_connection_admin has insufficient privileges
SET effective_cache_size = 1073741824;  -- ok
RESET effective_cache_size;  -- ok
ALTER SYSTEM SET effective_cache_size = 1073741824;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET effective_cache_size;  -- fail, regress_connection_admin has insufficient privileges
SET jit_above_cost = 50;  -- ok
RESET jit_above_cost;  -- ok
ALTER SYSTEM SET jit_above_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_above_cost;  -- fail, regress_connection_admin has insufficient privileges
SET jit_inline_above_cost = 50;  -- ok
RESET jit_inline_above_cost;  -- ok
ALTER SYSTEM SET jit_inline_above_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_inline_above_cost;  -- fail, regress_connection_admin has insufficient privileges
SET jit_optimize_above_cost = 50;  -- ok
RESET jit_optimize_above_cost;  -- ok
ALTER SYSTEM SET jit_optimize_above_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit_optimize_above_cost;  -- fail, regress_connection_admin has insufficient privileges
SET min_parallel_index_scan_size = 50;  -- ok
RESET min_parallel_index_scan_size;  -- ok
ALTER SYSTEM SET min_parallel_index_scan_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET min_parallel_index_scan_size;  -- fail, regress_connection_admin has insufficient privileges
SET min_parallel_table_scan_size = 50;  -- ok
RESET min_parallel_table_scan_size;  -- ok
ALTER SYSTEM SET min_parallel_table_scan_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET min_parallel_table_scan_size;  -- fail, regress_connection_admin has insufficient privileges
SET parallel_setup_cost = 50;  -- ok
RESET parallel_setup_cost;  -- ok
ALTER SYSTEM SET parallel_setup_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET parallel_setup_cost;  -- fail, regress_connection_admin has insufficient privileges
SET parallel_tuple_cost = 50;  -- ok
RESET parallel_tuple_cost;  -- ok
ALTER SYSTEM SET parallel_tuple_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET parallel_tuple_cost;  -- fail, regress_connection_admin has insufficient privileges
SET random_page_cost = 50;  -- ok
RESET random_page_cost;  -- ok
ALTER SYSTEM SET random_page_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET random_page_cost;  -- fail, regress_connection_admin has insufficient privileges
SET seq_page_cost = 50;  -- ok
RESET seq_page_cost;  -- ok
ALTER SYSTEM SET seq_page_cost = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET seq_page_cost;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / QUERY_TUNING_GEQO
SET geqo = OFF;  -- ok
RESET geqo;  -- ok
ALTER SYSTEM SET geqo = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET geqo;  -- fail, regress_connection_admin has insufficient privileges
SET geqo_effort = 5;  -- ok
RESET geqo_effort;  -- ok
ALTER SYSTEM SET geqo_effort = 5;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET geqo_effort;  -- fail, regress_connection_admin has insufficient privileges
SET geqo_generations = 50;  -- ok
RESET geqo_generations;  -- ok
ALTER SYSTEM SET geqo_generations = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET geqo_generations;  -- fail, regress_connection_admin has insufficient privileges
SET geqo_pool_size = 50;  -- ok
RESET geqo_pool_size;  -- ok
ALTER SYSTEM SET geqo_pool_size = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET geqo_pool_size;  -- fail, regress_connection_admin has insufficient privileges
SET geqo_seed = 0;  -- ok
RESET geqo_seed;  -- ok
ALTER SYSTEM SET geqo_seed = 0;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET geqo_seed;  -- fail, regress_connection_admin has insufficient privileges
SET geqo_selection_bias = 2;  -- ok
RESET geqo_selection_bias;  -- ok
ALTER SYSTEM SET geqo_selection_bias = 2;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET geqo_selection_bias;  -- fail, regress_connection_admin has insufficient privileges
SET geqo_threshold = 1073741824;  -- ok
RESET geqo_threshold;  -- ok
ALTER SYSTEM SET geqo_threshold = 1073741824;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET geqo_threshold;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / QUERY_TUNING_METHOD
SET enable_async_append = OFF;  -- ok
RESET enable_async_append;  -- ok
ALTER SYSTEM SET enable_async_append = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_async_append;  -- fail, regress_connection_admin has insufficient privileges
SET enable_bitmapscan = OFF;  -- ok
RESET enable_bitmapscan;  -- ok
ALTER SYSTEM SET enable_bitmapscan = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_bitmapscan;  -- fail, regress_connection_admin has insufficient privileges
SET enable_gathermerge = OFF;  -- ok
RESET enable_gathermerge;  -- ok
ALTER SYSTEM SET enable_gathermerge = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_gathermerge;  -- fail, regress_connection_admin has insufficient privileges
SET enable_hashagg = OFF;  -- ok
RESET enable_hashagg;  -- ok
ALTER SYSTEM SET enable_hashagg = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_hashagg;  -- fail, regress_connection_admin has insufficient privileges
SET enable_hashjoin = OFF;  -- ok
RESET enable_hashjoin;  -- ok
ALTER SYSTEM SET enable_hashjoin = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_hashjoin;  -- fail, regress_connection_admin has insufficient privileges
SET enable_incremental_sort = OFF;  -- ok
RESET enable_incremental_sort;  -- ok
ALTER SYSTEM SET enable_incremental_sort = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_incremental_sort;  -- fail, regress_connection_admin has insufficient privileges
SET enable_indexonlyscan = OFF;  -- ok
RESET enable_indexonlyscan;  -- ok
ALTER SYSTEM SET enable_indexonlyscan = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_indexonlyscan;  -- fail, regress_connection_admin has insufficient privileges
SET enable_indexscan = OFF;  -- ok
RESET enable_indexscan;  -- ok
ALTER SYSTEM SET enable_indexscan = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_indexscan;  -- fail, regress_connection_admin has insufficient privileges
SET enable_material = OFF;  -- ok
RESET enable_material;  -- ok
ALTER SYSTEM SET enable_material = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_material;  -- fail, regress_connection_admin has insufficient privileges
SET enable_memoize = OFF;  -- ok
RESET enable_memoize;  -- ok
ALTER SYSTEM SET enable_memoize = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_memoize;  -- fail, regress_connection_admin has insufficient privileges
SET enable_mergejoin = OFF;  -- ok
RESET enable_mergejoin;  -- ok
ALTER SYSTEM SET enable_mergejoin = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_mergejoin;  -- fail, regress_connection_admin has insufficient privileges
SET enable_nestloop = OFF;  -- ok
RESET enable_nestloop;  -- ok
ALTER SYSTEM SET enable_nestloop = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_nestloop;  -- fail, regress_connection_admin has insufficient privileges
SET enable_parallel_append = OFF;  -- ok
RESET enable_parallel_append;  -- ok
ALTER SYSTEM SET enable_parallel_append = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_parallel_append;  -- fail, regress_connection_admin has insufficient privileges
SET enable_parallel_hash = OFF;  -- ok
RESET enable_parallel_hash;  -- ok
ALTER SYSTEM SET enable_parallel_hash = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_parallel_hash;  -- fail, regress_connection_admin has insufficient privileges
SET enable_partition_pruning = OFF;  -- ok
RESET enable_partition_pruning;  -- ok
ALTER SYSTEM SET enable_partition_pruning = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_partition_pruning;  -- fail, regress_connection_admin has insufficient privileges
SET enable_partitionwise_aggregate = OFF;  -- ok
RESET enable_partitionwise_aggregate;  -- ok
ALTER SYSTEM SET enable_partitionwise_aggregate = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_partitionwise_aggregate;  -- fail, regress_connection_admin has insufficient privileges
SET enable_partitionwise_join = OFF;  -- ok
RESET enable_partitionwise_join;  -- ok
ALTER SYSTEM SET enable_partitionwise_join = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_partitionwise_join;  -- fail, regress_connection_admin has insufficient privileges
SET enable_seqscan = OFF;  -- ok
RESET enable_seqscan;  -- ok
ALTER SYSTEM SET enable_seqscan = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_seqscan;  -- fail, regress_connection_admin has insufficient privileges
SET enable_sort = OFF;  -- ok
RESET enable_sort;  -- ok
ALTER SYSTEM SET enable_sort = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_sort;  -- fail, regress_connection_admin has insufficient privileges
SET enable_tidscan = OFF;  -- ok
RESET enable_tidscan;  -- ok
ALTER SYSTEM SET enable_tidscan = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET enable_tidscan;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / QUERY_TUNING_OTHER
SET constraint_exclusion = 'partition';  -- ok
RESET constraint_exclusion;  -- ok
ALTER SYSTEM SET constraint_exclusion = 'partition';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET constraint_exclusion;  -- fail, regress_connection_admin has insufficient privileges
SET cursor_tuple_fraction = 0;  -- ok
RESET cursor_tuple_fraction;  -- ok
ALTER SYSTEM SET cursor_tuple_fraction = 0;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET cursor_tuple_fraction;  -- fail, regress_connection_admin has insufficient privileges
SET default_statistics_target = 5000;  -- ok
RESET default_statistics_target;  -- ok
ALTER SYSTEM SET default_statistics_target = 5000;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET default_statistics_target;  -- fail, regress_connection_admin has insufficient privileges
SET from_collapse_limit = 1073741824;  -- ok
RESET from_collapse_limit;  -- ok
ALTER SYSTEM SET from_collapse_limit = 1073741824;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET from_collapse_limit;  -- fail, regress_connection_admin has insufficient privileges
SET jit = OFF;  -- ok
RESET jit;  -- ok
ALTER SYSTEM SET jit = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET jit;  -- fail, regress_connection_admin has insufficient privileges
SET join_collapse_limit = 1073741824;  -- ok
RESET join_collapse_limit;  -- ok
ALTER SYSTEM SET join_collapse_limit = 1073741824;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET join_collapse_limit;  -- fail, regress_connection_admin has insufficient privileges
SET plan_cache_mode = 'force_generic_plan';  -- ok
RESET plan_cache_mode;  -- ok
ALTER SYSTEM SET plan_cache_mode = 'force_generic_plan';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET plan_cache_mode;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / REPLICATION_SENDING
SET wal_sender_timeout = 50;  -- ok
RESET wal_sender_timeout;  -- ok
ALTER SYSTEM SET wal_sender_timeout = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_sender_timeout;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / RESOURCES_ASYNCHRONOUS
SET backend_flush_after = 128;  -- ok
RESET backend_flush_after;  -- ok
ALTER SYSTEM SET backend_flush_after = 128;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET backend_flush_after;  -- fail, regress_connection_admin has insufficient privileges
SET effective_io_concurrency = 0;  -- ok
RESET effective_io_concurrency;  -- ok
ALTER SYSTEM SET effective_io_concurrency = 0;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET effective_io_concurrency;  -- fail, regress_connection_admin has insufficient privileges
SET maintenance_io_concurrency = 0;  -- ok
RESET maintenance_io_concurrency;  -- ok
ALTER SYSTEM SET maintenance_io_concurrency = 0;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET maintenance_io_concurrency;  -- fail, regress_connection_admin has insufficient privileges
SET max_parallel_maintenance_workers = 50;  -- ok
RESET max_parallel_maintenance_workers;  -- ok
ALTER SYSTEM SET max_parallel_maintenance_workers = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_parallel_maintenance_workers;  -- fail, regress_connection_admin has insufficient privileges
SET max_parallel_workers = 50;  -- ok
RESET max_parallel_workers;  -- ok
ALTER SYSTEM SET max_parallel_workers = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_parallel_workers;  -- fail, regress_connection_admin has insufficient privileges
SET max_parallel_workers_per_gather = 50;  -- ok
RESET max_parallel_workers_per_gather;  -- ok
ALTER SYSTEM SET max_parallel_workers_per_gather = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET max_parallel_workers_per_gather;  -- fail, regress_connection_admin has insufficient privileges
SET parallel_leader_participation = OFF;  -- ok
RESET parallel_leader_participation;  -- ok
ALTER SYSTEM SET parallel_leader_participation = OFF;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET parallel_leader_participation;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / RESOURCES_MEM
SET hash_mem_multiplier = 500;  -- ok
RESET hash_mem_multiplier;  -- ok
ALTER SYSTEM SET hash_mem_multiplier = 500;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET hash_mem_multiplier;  -- fail, regress_connection_admin has insufficient privileges
SET logical_decoding_work_mem = 1073741855;  -- ok
RESET logical_decoding_work_mem;  -- ok
ALTER SYSTEM SET logical_decoding_work_mem = 1073741855;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET logical_decoding_work_mem;  -- fail, regress_connection_admin has insufficient privileges
SET maintenance_work_mem = 1073742335;  -- ok
RESET maintenance_work_mem;  -- ok
ALTER SYSTEM SET maintenance_work_mem = 1073742335;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET maintenance_work_mem;  -- fail, regress_connection_admin has insufficient privileges
SET temp_buffers = 536870961;  -- ok
RESET temp_buffers;  -- ok
ALTER SYSTEM SET temp_buffers = 536870961;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET temp_buffers;  -- fail, regress_connection_admin has insufficient privileges
SET work_mem = 1073741855;  -- ok
RESET work_mem;  -- ok
ALTER SYSTEM SET work_mem = 1073741855;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET work_mem;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / RESOURCES_VACUUM_DELAY
SET vacuum_cost_delay = 50;  -- ok
RESET vacuum_cost_delay;  -- ok
ALTER SYSTEM SET vacuum_cost_delay = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_cost_delay;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_cost_limit = 5000;  -- ok
RESET vacuum_cost_limit;  -- ok
ALTER SYSTEM SET vacuum_cost_limit = 5000;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_cost_limit;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_cost_page_dirty = 50;  -- ok
RESET vacuum_cost_page_dirty;  -- ok
ALTER SYSTEM SET vacuum_cost_page_dirty = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_cost_page_dirty;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_cost_page_hit = 50;  -- ok
RESET vacuum_cost_page_hit;  -- ok
ALTER SYSTEM SET vacuum_cost_page_hit = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_cost_page_hit;  -- fail, regress_connection_admin has insufficient privileges
SET vacuum_cost_page_miss = 50;  -- ok
RESET vacuum_cost_page_miss;  -- ok
ALTER SYSTEM SET vacuum_cost_page_miss = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET vacuum_cost_page_miss;  -- fail, regress_connection_admin has insufficient privileges
-- PGC_USERSET / UNGROUPED
SET seed = 0;  -- ok
RESET seed;  -- ok
-- PGC_USERSET / WAL_SETTINGS
SET commit_siblings = 50;  -- ok
RESET commit_siblings;  -- ok
ALTER SYSTEM SET commit_siblings = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET commit_siblings;  -- fail, regress_connection_admin has insufficient privileges
SET synchronous_commit = 'remote_write';  -- ok
RESET synchronous_commit;  -- ok
ALTER SYSTEM SET synchronous_commit = 'remote_write';  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET synchronous_commit;  -- fail, regress_connection_admin has insufficient privileges
SET wal_skip_threshold = 50;  -- ok
RESET wal_skip_threshold;  -- ok
ALTER SYSTEM SET wal_skip_threshold = 50;  -- fail, regress_connection_admin has insufficient privileges
ALTER SYSTEM RESET wal_skip_threshold;  -- fail, regress_connection_admin has insufficient privileges
RESET statement_timeout;
RESET SESSION AUTHORIZATION;
DROP ROLE regress_connection_admin;
