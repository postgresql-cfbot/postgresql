with clauses(expr) as (
  values
    ('x'),
    ('not x'),
    ('strictf(x, y)'),
    ('not strictf(x, y)'),
    ('x is null'),
    ('x is not null'),
    ('x is true'),
    ('x is not true'),
    ('x is false'),
    ('x is not false'),
    ('x is unknown'),
    ('x is not unknown'),
    ('x = true'),
    ('x = false')
)
select p.expr predicate, c.expr clause, t.*
from clauses p, clauses c
join lateral (
  select *
  from test_predtest(
    'select ' || p.expr || ', ' || c.expr ||
    ' from booleans'
  )
) t on true;
