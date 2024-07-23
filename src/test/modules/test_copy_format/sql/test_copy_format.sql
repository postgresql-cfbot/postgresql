CREATE EXTENSION test_copy_format;
CREATE TABLE public.test (a smallint, b integer, c bigint);
INSERT INTO public.test VALUES (1, 2, 3), (12, 34, 56), (123, 456, 789);
COPY public.test FROM stdin WITH (format 'test_copy_format');
\.
COPY public.test TO stdout WITH (format 'test_copy_format');
