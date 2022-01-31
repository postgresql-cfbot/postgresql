/*
 * PostgreSQL User SET variables
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/backend/catalog/setting_privileges.sql
 *
 * Note: this file is read in single-user -j mode, which means that the
 * command terminator is semicolon-newline-newline; whenever the backend
 * sees that, it stops and executes what it's got.  If you write a lot of
 * statements without empty lines between, they'll all get quoted to you
 * in any error message about one of them, so don't do that.  Also, you
 * cannot write a semicolon immediately followed by an empty line in a
 * string literal (including a function body!) or a multiline comment.
 */

GRANT SET VALUE ON
	enable_seqscan, enable_indexscan, enable_indexonlyscan, enable_bitmapscan,
	enable_tidscan, enable_sort, enable_incremental_sort, enable_hashagg,
	enable_material, enable_memoize, enable_nestloop, enable_mergejoin,
	enable_hashjoin, enable_gathermerge, enable_partitionwise_join,
	enable_partitionwise_aggregate, enable_parallel_append,
	enable_parallel_hash, enable_partition_pruning, enable_async_append, geqo,
	exit_on_error, debug_print_parse, debug_print_rewritten, debug_print_plan,
	debug_pretty_print, trace_notify, transform_null_equals,
	default_transaction_read_only, transaction_read_only,
	default_transaction_deferrable, transaction_deferrable, row_security,
	check_function_bodies, array_nulls, default_with_oids, trace_sort,
	trace_syncscan, optimize_bounded_sort, escape_string_warning,
	standard_conforming_strings, synchronize_seqscans, quote_all_identifiers,
	parallel_leader_participation, jit, jit_expressions, jit_tuple_deforming,
	default_statistics_target, from_collapse_limit, join_collapse_limit,
	geqo_threshold, geqo_effort, geqo_pool_size, geqo_generations,
	temp_buffers, work_mem, maintenance_work_mem, logical_decoding_work_mem,
	vacuum_cost_page_hit, vacuum_cost_page_miss, vacuum_cost_page_dirty,
	vacuum_cost_limit, statement_timeout, lock_timeout,
	idle_in_transaction_session_timeout, idle_session_timeout,
	vacuum_freeze_min_age, vacuum_freeze_table_age,
	vacuum_multixact_freeze_min_age, vacuum_multixact_freeze_table_age,
	vacuum_failsafe_age, vacuum_multixact_failsafe_age, wal_skip_threshold,
	wal_sender_timeout, commit_siblings, extra_float_digits,
	log_parameter_max_length_on_error, effective_io_concurrency,
	maintenance_io_concurrency, backend_flush_after,
	max_parallel_maintenance_workers, max_parallel_workers_per_gather,
	max_parallel_workers, tcp_keepalives_idle, tcp_keepalives_interval,
	ssl_renegotiation_limit, tcp_keepalives_count, gin_fuzzy_search_limit,
	effective_cache_size, min_parallel_table_scan_size,
	min_parallel_index_scan_size, gin_pending_list_limit, tcp_user_timeout,
	client_connection_check_interval, seq_page_cost, random_page_cost,
	cpu_tuple_cost, cpu_index_tuple_cost, cpu_operator_cost,
	parallel_tuple_cost, parallel_setup_cost, jit_above_cost,
	jit_optimize_above_cost, jit_inline_above_cost, cursor_tuple_fraction,
	geqo_selection_bias, geqo_seed, hash_mem_multiplier, seed,
	vacuum_cost_delay, DateStyle, default_table_access_method,
	default_tablespace, temp_tablespaces, lc_monetary, lc_numeric, lc_time,
	local_preload_libraries, search_path, role, TimeZone,
	timezone_abbreviations, default_text_search_config, application_name,
	backslash_quote, bytea_output, client_min_messages, constraint_exclusion,
	default_toast_compression, default_transaction_isolation,
	transaction_isolation, IntervalStyle, synchronous_commit, xmlbinary,
	xmloption, force_parallel_mode, password_encryption, plan_cache_mode
	TO public;
