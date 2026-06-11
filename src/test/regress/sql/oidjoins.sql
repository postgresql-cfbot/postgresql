--
-- Verify system catalog foreign key relationships
--
DO $doblock$
declare
  fk record;
  nkeys integer;
  cmd text;
  err record;
begin
  for fk in select * from pg_get_catalog_foreign_keys()
  loop
    raise notice 'checking % % => % %',
      fk.fktable, fk.fkcols, fk.pktable, fk.pkcols;
    nkeys := array_length(fk.fkcols, 1);
    cmd := 'SELECT ctid';
    for i in 1 .. nkeys loop
      cmd := cmd || ', ' || quote_ident(fk.fkcols[i]);
    end loop;
    if fk.is_array then
      cmd := cmd || ' FROM (SELECT ctid';
      for i in 1 .. nkeys-1 loop
        cmd := cmd || ', ' || quote_ident(fk.fkcols[i]);
      end loop;
      cmd := cmd || ', unnest(' || quote_ident(fk.fkcols[nkeys]);
      cmd := cmd || ') as ' || quote_ident(fk.fkcols[nkeys]);
      -- For pg_statistic_ext, include stxkind for join stats filtering
      if fk.fktable = 'pg_statistic_ext'::regclass and
         fk.fkcols = ARRAY['stxrelid', 'stxkeys'] then
        cmd := cmd || ', stxkind, stxjoinrels';
      end if;
      cmd := cmd || ' FROM ' || fk.fktable::text || ') fk WHERE ';
    else
      cmd := cmd || ' FROM ' || fk.fktable::text || ' fk WHERE ';
    end if;
    if fk.is_opt then
      for i in 1 .. nkeys loop
        cmd := cmd || quote_ident(fk.fkcols[i]) || ' != 0 AND ';
      end loop;
    end if;
    -- Special case: For join statistics, stxkeys references attributes from
    -- the other table (via stxkeyrefs), not from stxrelid.  Skip the FK
    -- check for join stats where stxjoinrels is not null.
    if fk.fktable = 'pg_statistic_ext'::regclass and
       fk.fkcols = ARRAY['stxrelid', 'stxkeys'] then
      cmd := cmd || 'stxjoinrels IS NULL AND ';
    end if;
    cmd := cmd || 'NOT EXISTS(SELECT 1 FROM ' || fk.pktable::text || ' pk WHERE ';
    for i in 1 .. nkeys loop
      if i > 1 then cmd := cmd || ' AND '; end if;
      cmd := cmd || 'pk.' || quote_ident(fk.pkcols[i]);
      cmd := cmd || ' = fk.' || quote_ident(fk.fkcols[i]);
    end loop;
    cmd := cmd || ')';
    -- raise notice 'cmd = %', cmd;
    for err in execute cmd loop
      raise warning 'FK VIOLATION IN %(%): %', fk.fktable, fk.fkcols, err;
    end loop;
  end loop;
end
$doblock$;
