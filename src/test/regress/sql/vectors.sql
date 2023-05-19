SET extra_float_digits = -1;

-- float8_array_is_vector
SELECT float8_array_is_vector(ARRAY[]::float8[]);
SELECT float8_array_is_vector('{{1.0},{1.0}}');
SELECT float8_array_is_vector(ARRAY[NULL]::float8[]);
SELECT float8_array_is_vector('{1,2,3}');

-- euclidean_norm
SELECT euclidean_norm('{1,2,3}');

-- normalize_vector
SELECT normalize_vector('{0,0,0}');
SELECT normalize_vector('{1,2,3}');

-- dot_product
SELECT dot_product('{1,2,3}', '{4,5,6}');

-- squared_euclidean_distance
SELECT squared_euclidean_distance('{1,2}', '{4,5,6}');
SELECT squared_euclidean_distance('{1,2,3}', '{4,5,6}');

-- euclidean_distance
SELECT euclidean_distance('{1,2}', '{4,5,6}');
SELECT euclidean_distance('{1,2,3}', '{4,5,6}');

-- cosine_distance
SELECT cosine_distance('{1,2}', '{4,5,6}');
SELECT cosine_distance('{1,2,3}', '{4,5,6}');

-- taxicab_distance
SELECT taxicab_distance('{1,2}', '{4,5,6}');
SELECT taxicab_distance('{1,2,3}', '{4,5,6}');

-- chebyshev_distance
SELECT chebyshev_distance('{1,2}', '{4,5,6}');
SELECT chebyshev_distance('{1,2,3}', '{4,5,6}');

-- standard_unit_vector
SELECT standard_unit_vector(0, 0);
SELECT standard_unit_vector(1, 0);
SELECT standard_unit_vector(1, 2);
SELECT standard_unit_vector(3, 1);
SELECT standard_unit_vector(3, 3);

-- kmeans
SELECT * FROM kmeans(0, '{{1,2,3},{4,5,6},{7,8,9},{10,11,12}}', 1);
SELECT * FROM kmeans(5, '{{1,2,3},{4,5,6},{7,8,9},{10,11,12}}', 1);
SELECT * FROM kmeans(2, '{{1,2,3},{4,5,6},{7,8,9},{10,11,NULL}}', 1);
SELECT * FROM kmeans(2, ARRAY[[],[]]::float8[][], 1);
SELECT * FROM kmeans(2, '{{1,2,3},{4,5,6},{7,8,9},{10,11,12}}', -1);
SELECT * FROM kmeans(2, '{{1,2,3},{4,5,6},{7,8,9},{10,11,12}}', 1) ORDER BY 1;

-- closest_vector
SELECT closest_vector('{{1,2,3},{4,5,6},{7,8,9}}', ARRAY[]::float8[]);
SELECT closest_vector(ARRAY[]::float8[], '{1,2,3}');
SELECT closest_vector('{{1,2,3},{4,5,6},{7,8,NULL}}', '{1,2,3}');
SELECT closest_vector('{{1,2,3},{4,5,6},{7,8,9}}', '{1,2}');
SELECT closest_vector('{1,2,3}', '{1,2}');
SELECT closest_vector('{{1,2,3},{4,5,6},{7,8,9}}', '{10,11,12}');
SELECT closest_vector('{1,2,3}', '{2,3,4}');
