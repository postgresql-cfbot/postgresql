select count(distinct a), sum(hashtext(a::text)) from t_175_35 where a > b;
select count(distinct a), sum(hashtext(a::text)) from t_400_80 where a > b;
select count(distinct a), sum(hashtext(a::text)) from t_1000_200 where a > b;

select sum(hashtext((a+b)::text)),
    sum(hashtext((a-b)::text)),
    sum(hashtext((a*b)::text)),
    sum(hashtext((a/nullif(b, 0))::text)),
    sum(hashtext((a%nullif(b, 0))::text)),
    sum(hashtext((a*b+b/nullif(a, 0))::text)),
    sum(hashtext((a-b/nullif((b-a), 0))::text)),
    sum(hashtext((a > b)::text)),
    sum(hashtext((a < b)::text)),
    sum(hashtext((a = b)::text)),
    sum(hashtext(abs(a)::text)),
    sum(hashtext(round(a)::text)),
    sum(hashtext(trunc(a)::text))
from t_175_35;
select sum(hashtext((a+b)::text)),
    sum(hashtext((a-b)::text)),
    sum(hashtext((a*b)::text)),
    sum(hashtext((a/nullif(b, 0))::text)),
    sum(hashtext((a%nullif(b, 0))::text)),
    sum(hashtext((a*b+b/nullif(a, 0))::text)),
    sum(hashtext((a-b/nullif((b-a), 0))::text)),
    sum(hashtext((a > b)::text)),
    sum(hashtext((a < b)::text)),
    sum(hashtext((a = b)::text)),
    sum(hashtext(abs(a)::text)),
    sum(hashtext(round(a)::text)),
    sum(hashtext(trunc(a)::text))
from t_400_80;
select sum(hashtext((a+b)::text)),
    sum(hashtext((a-b)::text)),
    sum(hashtext((a*b)::text)),
    sum(hashtext((a/nullif(b, 0))::text)),
    sum(hashtext((a%nullif(b, 0))::text)),
    sum(hashtext((a*b+b/nullif(a, 0))::text)),
    sum(hashtext((a-b/nullif((b-a), 0))::text)),
    sum(hashtext((a > b)::text)),
    sum(hashtext((a < b)::text)),
    sum(hashtext((a = b)::text)),
    sum(hashtext(abs(a)::text)),
    sum(hashtext(round(a)::text)),
    sum(hashtext(trunc(a)::text))
from t_1000_200;

select sum(hashtext((rk)::text)), sum(hashtext((rn)::text)), sum(hashtext((s)::text)) from (
    select rank() over(partition by id2 order by a+b, a, b, id) as rk,
        row_number() over(partition by id2 order by b, a, id) as rn,
        sum(a) over(
            partition by id2
            order by a, b, id
            rows between unbounded preceding and current row
        ) as s
    from t_175_35
) t1;
select sum(hashtext((rk)::text)), sum(hashtext((rn)::text)), sum(hashtext((s)::text)) from (
    select rank() over(partition by id2 order by a+b, a, b, id) as rk,
        row_number() over(partition by id2 order by b, a, id) as rn,
        sum(a) over(
            partition by id2
            order by a, b, id
            rows between unbounded preceding and current row
        ) as s
    from t_400_80
) t1;
select sum(hashtext((rk)::text)), sum(hashtext((rn)::text)), sum(hashtext((s)::text)) from (
    select rank() over(partition by id2 order by a+b, a, b, id) as rk,
        row_number() over(partition by id2 order by b, a, id) as rn,
        sum(a) over(
            partition by id2
            order by a, b, id
            rows between unbounded preceding and current row
        ) as s
    from t_1000_200
) t1;

select a/0 from t_175_35;
select a/0 from t_400_80;
select a/0 from t_1000_200;

select a%0 from t_175_35;
select a%0 from t_400_80;
select a%0 from t_1000_200;

select (a-a)/(a-a) from t_175_35;
select (a-a)/(a-a) from t_400_80;
select (a-a)/(a-a) from t_1000_200;

set enable_hashjoin = on;
set enable_nestloop = off;
set enable_mergejoin = off;

select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_175_35 t1 join t_175_35 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_400_80 t1 join t_400_80 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_1000_200 t1 join t_1000_200 t2 on t1.a = t2.a;

reset enable_hashjoin;
reset enable_nestloop;
reset enable_mergejoin;

set enable_hashjoin = off;
set enable_nestloop = on;
set enable_mergejoin = off;

select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_175_35 t1 join t_175_35 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_400_80 t1 join t_400_80 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_1000_200 t1 join t_1000_200 t2 on t1.a = t2.a;

reset enable_hashjoin;
reset enable_nestloop;
reset enable_mergejoin;

set enable_hashjoin = off;
set enable_nestloop = off;
set enable_mergejoin = on;

select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_175_35 t1 join t_175_35 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_400_80 t1 join t_400_80 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_1000_200 t1 join t_1000_200 t2 on t1.a = t2.a;

reset enable_hashjoin;
reset enable_nestloop;
reset enable_mergejoin;

set enable_seqscan = off;
set enable_bitmapscan = off;
explain (costs off) select * from t_175_35 where a > 1000 order by a;
explain (costs off) select a from t_175_35 where a > 1000 order by a;
explain (costs off) select * from t_400_80 where a > 1000 order by a;
explain (costs off) select a from t_400_80 where a > 1000 order by a;
explain (costs off) select * from t_1000_200 where a > 1000 order by a;
explain (costs off) select a from t_1000_200 where a > 1000 order by a;
reset enable_seqscan;
reset enable_bitmapscan;
update t_175_35 set a = a + 1;
update t_400_80 set a = a + 1;
update t_1000_200 set a = a + 1;

select count(distinct a), sum(hashtext(a::text)) from t_175_35 where a > b;
select count(distinct a), sum(hashtext(a::text)) from t_400_80 where a > b;
select count(distinct a), sum(hashtext(a::text)) from t_1000_200 where a > b;

select sum(hashtext((a+b)::text)),
    sum(hashtext((a-b)::text)),
    sum(hashtext((a*b)::text)),
    sum(hashtext((a/nullif(b, 0))::text)),
    sum(hashtext((a%nullif(b, 0))::text)),
    sum(hashtext((a*b+b/nullif(a, 0))::text)),
    sum(hashtext((a-b/nullif((b-a), 0))::text)),
    sum(hashtext((a > b)::text)),
    sum(hashtext((a < b)::text)),
    sum(hashtext((a = b)::text)),
    sum(hashtext(abs(a)::text)),
    sum(hashtext(round(a)::text)),
    sum(hashtext(trunc(a)::text))
from t_175_35;
select sum(hashtext((a+b)::text)),
    sum(hashtext((a-b)::text)),
    sum(hashtext((a*b)::text)),
    sum(hashtext((a/nullif(b, 0))::text)),
    sum(hashtext((a%nullif(b, 0))::text)),
    sum(hashtext((a*b+b/nullif(a, 0))::text)),
    sum(hashtext((a-b/nullif((b-a), 0))::text)),
    sum(hashtext((a > b)::text)),
    sum(hashtext((a < b)::text)),
    sum(hashtext((a = b)::text)),
    sum(hashtext(abs(a)::text)),
    sum(hashtext(round(a)::text)),
    sum(hashtext(trunc(a)::text))
from t_400_80;
select sum(hashtext((a+b)::text)),
    sum(hashtext((a-b)::text)),
    sum(hashtext((a*b)::text)),
    sum(hashtext((a/nullif(b, 0))::text)),
    sum(hashtext((a%nullif(b, 0))::text)),
    sum(hashtext((a*b+b/nullif(a, 0))::text)),
    sum(hashtext((a-b/nullif((b-a), 0))::text)),
    sum(hashtext((a > b)::text)),
    sum(hashtext((a < b)::text)),
    sum(hashtext((a = b)::text)),
    sum(hashtext(abs(a)::text)),
    sum(hashtext(round(a)::text)),
    sum(hashtext(trunc(a)::text))
from t_1000_200;

select sum(hashtext((rk)::text)), sum(hashtext((rn)::text)), sum(hashtext((s)::text)) from (
    select rank() over(partition by id2 order by a+b, a, b, id) as rk,
        row_number() over(partition by id2 order by b, a, id) as rn,
        sum(a) over(
            partition by id2
            order by a, b, id
            rows between unbounded preceding and current row
        ) as s
    from t_175_35
) t1;
select sum(hashtext((rk)::text)), sum(hashtext((rn)::text)), sum(hashtext((s)::text)) from (
    select rank() over(partition by id2 order by a+b, a, b, id) as rk,
        row_number() over(partition by id2 order by b, a, id) as rn,
        sum(a) over(
            partition by id2
            order by a, b, id
            rows between unbounded preceding and current row
        ) as s
    from t_400_80
) t1;
select sum(hashtext((rk)::text)), sum(hashtext((rn)::text)), sum(hashtext((s)::text)) from (
    select rank() over(partition by id2 order by a+b, a, b, id) as rk,
        row_number() over(partition by id2 order by b, a, id) as rn,
        sum(a) over(
            partition by id2
            order by a, b, id
            rows between unbounded preceding and current row
        ) as s
    from t_1000_200
) t1;

select a/0 from t_175_35;
select a/0 from t_400_80;
select a/0 from t_1000_200;

select a%0 from t_175_35;
select a%0 from t_400_80;
select a%0 from t_1000_200;

select (a-a)/(a-a) from t_175_35;
select (a-a)/(a-a) from t_400_80;
select (a-a)/(a-a) from t_1000_200;

set enable_hashjoin = on;
set enable_nestloop = off;
set enable_mergejoin = off;

select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_175_35 t1 join t_175_35 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_400_80 t1 join t_400_80 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_1000_200 t1 join t_1000_200 t2 on t1.a = t2.a;

reset enable_hashjoin;
reset enable_nestloop;
reset enable_mergejoin;

set enable_hashjoin = off;
set enable_nestloop = on;
set enable_mergejoin = off;

select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_175_35 t1 join t_175_35 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_400_80 t1 join t_400_80 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_1000_200 t1 join t_1000_200 t2 on t1.a = t2.a;

reset enable_hashjoin;
reset enable_nestloop;
reset enable_mergejoin;

set enable_hashjoin = off;
set enable_nestloop = off;
set enable_mergejoin = on;

select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_175_35 t1 join t_175_35 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_400_80 t1 join t_400_80 t2 on t1.a = t2.a;
select md5(sum(t1.a + t2.b)::text), md5(avg(t1.a * t2.b)::text), md5(stddev(t1.a - t2.b)::text) from t_1000_200 t1 join t_1000_200 t2 on t1.a = t2.a;

reset enable_hashjoin;
reset enable_nestloop;
reset enable_mergejoin;

select sum(hashtext((a*rn)::text)) from (select a, row_number() over(order by a, id) as rn from t_175_35 where a > 100) t1;
select sum(hashtext((a*rn)::text)) from (select a, row_number() over(order by a, id) as rn from t_400_80 where a > 100) t1;
select sum(hashtext((a*rn)::text)) from (select a, row_number() over(order by a, id) as rn from t_1000_200 where a > 100) t1;
