select pg_cancel_backend(NULL);
select case
	when pg_cancel_backend(pg_backend_pid())
	then pg_sleep(60)
end;
select pg_cancel_backend(NULL, NULL);
select pg_cancel_backend(NULL, 'suicide is painless');
select case
	when pg_cancel_backend(pg_backend_pid(), NULL)
	then pg_sleep(60)
end;
