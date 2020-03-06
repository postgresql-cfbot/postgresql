CREATE EXTENSION bool_plperlu CASCADE;

 --- test transforming from perl
CREATE FUNCTION perl2int(int) RETURNS bool
LANGUAGE plperlu
TRANSFORM FOR TYPE bool
AS $$
return shift;
$$;

CREATE FUNCTION perl2text(text) RETURNS bool
LANGUAGE plperlu
TRANSFORM FOR TYPE bool
AS $$
return shift;
$$;

CREATE FUNCTION perl2undef() RETURNS bool
LANGUAGE plperlu
TRANSFORM FOR TYPE bool
AS $$
return undef;
$$;

SELECT perl2int(1);
SELECT perl2int(0);
SELECT perl2text('foo');
SELECT perl2text('');
SELECT perl2undef() IS NULL AS p;

 --- test transforming to perl 
CREATE FUNCTION bool2perl(bool, bool, bool) RETURNS void
LANGUAGE plperlu
TRANSFORM FOR TYPE bool
AS $$
my ($x, $y, $z) = @_;
if(defined($z)) {
die("NULL mistransformed");
}
if(!defined($x)) {
die("TRUE mistransformed to UNDEF");
}
if(!defined($y)) {
die("FALSE mistransformed to UNDEF");
}
if(!$x) {
die("TRUE mistransformed");
}
if($y) {
die("FALSE mistransformed");
}
$$;

SELECT bool2perl (true, false, NULL);

 --- test selecting bool through SPI

CREATE FUNCTION spi_test()  RETURNS void
LANGUAGE plperlu
TRANSFORM FOR TYPE bool
AS $$
my $rv = spi_exec_query('SELECT true t, false f, NULL n')->{rows}->[0];
if(! defined ($rv->{t})) {
die("TRUE mistransformed to UNDEF in SPI");
}
if(! defined ($rv->{f})) {
die("FALSE mistransformed to UNDEF in SPI");
}
if( defined ($rv->{n})) {
die("NULL mistransformed in SPI");
}
if(! $rv->{t} ) {
die("TRUE mistransformed in SPI");
}
if(  $rv->{f} ) {
die("FALSE mistransformed in SPI");
}
$$;

SELECT spi_test();

\set VERBOSITY terse \\ -- suppress cascade details
DROP EXTENSION plperlu CASCADE;
