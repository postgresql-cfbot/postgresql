select pg_cancel_backend(NULL);
select pg_cancel_backend(pg_backend_pid());
select pg_cancel_backend(NULL, NULL);
select pg_cancel_backend(NULL, 'suicide is painless');
select case
	when pg_cancel_backend(pg_backend_pid(), 'ソケットをブロッキングモードに設定できませんでした')
	then pg_sleep(60)
end;
select case
	when pg_cancel_backend(pg_backend_pid(), NULL)
	then pg_sleep(60)
end;
