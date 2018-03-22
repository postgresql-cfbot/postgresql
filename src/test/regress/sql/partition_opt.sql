create table bt (k integer, v integer) partition by range (k);
create table dt1 partition of bt for values from (1) to (10001);
create table dt2 partition of bt for values from (10001) to (20001);
create index dti1 on dt1(v);
create index dti2 on dt2(v);
insert into bt values (generate_series(1,20000), generate_series(1,20000));
analyze bt;
explain (costs off) select * from bt where k between 1 and 20000 and v = 100;

CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) PARTITION BY RANGE (logdate);


CREATE TABLE measurement_y2006m03 PARTITION OF measurement
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');

explain (costs off) select * from measurement where logdate between '2006-03-01' AND '2006-03-31';

drop table bt;
drop table measurement;
