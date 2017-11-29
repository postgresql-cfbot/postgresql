-- Positive mixed-case tests --

SELECT to_number('mMm', 'rn');
SELECT to_number('Vi', 'RN');
SELECT to_number('CvIiI', 'rn');
SELECT to_number('MMMXIII', 'RN');
SELECT to_number('CvIiI', 'FMrn9');

-- Positive tests from to_char --

SELECT i, to_char(i, 'rn'), to_number(to_char(i, 'rn'), 'rn') FROM generate_series(1,3999) i;

-- Negative tests --

SELECT to_number('MDH', 'RN');
SELECT to_number('VVI', 'RN');
SELECT to_number('MCLL', 'RN');
SELECT to_number('MDD', 'RN');
SELECT to_number('MMMM', 'RN');
SELECT to_number('MIIX', 'RN');
SELECT to_number('CXXC', 'RN');
SELECT to_number('MXCXC', 'RN');
SELECT to_number('MxcXC', 'rn');
SELECT to_number('', 'rn');
SELECT to_number('   ', 'rn');
select to_number('vii7', 'rn9');
select to_number('7vii', '9rn');
