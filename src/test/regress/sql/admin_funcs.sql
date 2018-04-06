select pg_cancel_backend();
select pg_cancel_backend(NULL);
select pg_cancel_backend(pg_backend_pid());
select pg_cancel_backend(NULL, NULL);
select pg_cancel_backend(NULL, 'suicide is painless');
select pg_cancel_backend(pg_backend_pid(), 'it brings on many changes');
select pg_cancel_backend(pg_backend_pid(), NULL);
