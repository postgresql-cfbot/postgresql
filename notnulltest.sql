create table t1(a int, b int not null, c int, d int);
create table t2(a int, b int not null, c int, d int);

-- single rel
select * from t1;
select * from t1 where a > 1;
select * from t2 where a > 1 or c > 1;

-- partitioned relation.
create table p (a int, b int, c int not null) partition by range(a);
create table p_1 partition of p for values from (0) to (10000) partition by list(b);
create table p_1_1(b int,  c int not null, a int);
alter table p_1 attach partition p_1_1 for values in (1);


select * from p;
select * from p where a > 1;


-- test join:
select * from t1, t2 where t1.a = t2.c;
select t1.a, t2.b, t2.c from t1 left join t2 on t1.a = t2.c;
select * from t1 full join t2 on t1.a = t2.c;

