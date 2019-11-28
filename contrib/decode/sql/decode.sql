--
--  Test decode function
--

CREATE EXTENSION decode;

SELECT decode(1, 1, 5, 0);
SELECT decode(2, 1, 5, 0);
SELECT decode(1, 1, 5, 0.5);
SELECT decode(2, 1, 5, 0.5);

SELECT decode(1, 1, 'Ahoj', 'Nazdar');
SELECT decode(2, 1, 'Ahoj', 'Nazdar');
