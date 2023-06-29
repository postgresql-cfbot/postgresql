
create operator \{hello_named_operators} (function = box_add, leftarg = box, rightarg = point);

select '((0,0), (1,1))'::box \{hello_named_operators} '(1,1)'::point;

drop operator \{hello_named_operators}(box, point);

create operator \{add_point} (function = box_add, leftarg = box, rightarg = point);

create table test(a box);

insert into test values('((0,0),(1,1))'), ('((0,0),(2,1))');

select a as original, a \{add_point} '(1,1)' as modified from test;

CREATE OPERATOR \{equal_1} (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = \{equal_1}
);

CREATE OPERATOR \{equal_2} (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = equal_2 -- The absence of delimiters causes an expected failure
);


\! pg_dump -U postgres --schema public

drop operator \{add_point}(box, point);
drop operator \{equal_1}(int, int);
drop operator \{equal_2}(int, int);

drop table test;

