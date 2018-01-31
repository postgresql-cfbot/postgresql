#define PERL_constant_NOTFOUND	1
#define PERL_constant_NOTDEF	2
#define PERL_constant_ISIV	3
#define PERL_constant_ISNO	4
#define PERL_constant_ISNV	5
#define PERL_constant_ISPV	6
#define PERL_constant_ISPVN	7
#define PERL_constant_ISSV	8
#define PERL_constant_ISUNDEF	9
#define PERL_constant_ISUV	10
#define PERL_constant_ISYES	11

#ifndef NVTYPE
typedef double NV; /* 5.6 and later define NVTYPE, and typedef NV to it.  */
#endif
#ifndef aTHX_
#define aTHX_ /* 5.6 or later define this for threading support.  */
#endif
#ifndef pTHX_
#define pTHX_ /* 5.6 or later define this for threading support.  */
#endif

static int
constant_13 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     CONNECTION_OK PGRES_COPY_IN PQPING_REJECT */
  /* Offset 2 gives the best switch position.  */
  switch (name[2]) {
  case 'N':
    if (memEQ(name, "CONNECTION_OK", 13)) {
    /*                 ^                 */
      *iv_return = CONNECTION_OK;
      return PERL_constant_ISIV;
    }
    break;
  case 'P':
    if (memEQ(name, "PQPING_REJECT", 13)) {
    /*                 ^                 */
      *iv_return = PQPING_REJECT;
      return PERL_constant_ISIV;
    }
    break;
  case 'R':
    if (memEQ(name, "PGRES_COPY_IN", 13)) {
    /*                 ^                 */
      *iv_return = PGRES_COPY_IN;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_14 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     CONNECTION_BAD PGRES_COPY_OUT PQERRORS_TERSE PQTRANS_ACTIVE */
  /* Offset 2 gives the best switch position.  */
  switch (name[2]) {
  case 'E':
    if (memEQ(name, "PQERRORS_TERSE", 14)) {
    /*                 ^                  */
      *iv_return = PQERRORS_TERSE;
      return PERL_constant_ISIV;
    }
    break;
  case 'N':
    if (memEQ(name, "CONNECTION_BAD", 14)) {
    /*                 ^                  */
      *iv_return = CONNECTION_BAD;
      return PERL_constant_ISIV;
    }
    break;
  case 'R':
    if (memEQ(name, "PGRES_COPY_OUT", 14)) {
    /*                 ^                  */
      *iv_return = PGRES_COPY_OUT;
      return PERL_constant_ISIV;
    }
    break;
  case 'T':
    if (memEQ(name, "PQTRANS_ACTIVE", 14)) {
    /*                 ^                  */
      *iv_return = PQTRANS_ACTIVE;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_15 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     CONNECTION_MADE PGRES_COPY_BOTH PGRES_TUPLES_OK PQTRANS_INERROR
     PQTRANS_INTRANS PQTRANS_UNKNOWN */
  /* Offset 14 gives the best switch position.  */
  switch (name[14]) {
  case 'E':
    if (memEQ(name, "CONNECTION_MAD", 14)) {
    /*                             E      */
      *iv_return = CONNECTION_MADE;
      return PERL_constant_ISIV;
    }
    break;
  case 'H':
    if (memEQ(name, "PGRES_COPY_BOT", 14)) {
    /*                             H      */
      *iv_return = PGRES_COPY_BOTH;
      return PERL_constant_ISIV;
    }
    break;
  case 'K':
    if (memEQ(name, "PGRES_TUPLES_O", 14)) {
    /*                             K      */
      *iv_return = PGRES_TUPLES_OK;
      return PERL_constant_ISIV;
    }
    break;
  case 'N':
    if (memEQ(name, "PQTRANS_UNKNOW", 14)) {
    /*                             N      */
      *iv_return = PQTRANS_UNKNOWN;
      return PERL_constant_ISIV;
    }
    break;
  case 'R':
    if (memEQ(name, "PQTRANS_INERRO", 14)) {
    /*                             R      */
      *iv_return = PQTRANS_INERROR;
      return PERL_constant_ISIV;
    }
    break;
  case 'S':
    if (memEQ(name, "PQTRANS_INTRAN", 14)) {
    /*                             S      */
      *iv_return = PQTRANS_INTRANS;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_16 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     PGRES_COMMAND_OK PGRES_POLLING_OK PG_COPYRES_ATTRS PQERRORS_DEFAULT
     PQERRORS_VERBOSE */
  /* Offset 9 gives the best switch position.  */
  switch (name[9]) {
  case 'D':
    if (memEQ(name, "PQERRORS_DEFAULT", 16)) {
    /*                        ^             */
      *iv_return = PQERRORS_DEFAULT;
      return PERL_constant_ISIV;
    }
    break;
  case 'L':
    if (memEQ(name, "PGRES_POLLING_OK", 16)) {
    /*                        ^             */
      *iv_return = PGRES_POLLING_OK;
      return PERL_constant_ISIV;
    }
    break;
  case 'M':
    if (memEQ(name, "PGRES_COMMAND_OK", 16)) {
    /*                        ^             */
      *iv_return = PGRES_COMMAND_OK;
      return PERL_constant_ISIV;
    }
    break;
  case 'S':
    if (memEQ(name, "PG_COPYRES_ATTRS", 16)) {
    /*                        ^             */
#ifdef PG_COPYRES_ATTRS
      *iv_return = PG_COPYRES_ATTRS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'V':
    if (memEQ(name, "PQERRORS_VERBOSE", 16)) {
    /*                        ^             */
      *iv_return = PQERRORS_VERBOSE;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_17 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     CONNECTION_NEEDED CONNECTION_SETENV PGRES_EMPTY_QUERY PGRES_FATAL_ERROR
     PG_COPYRES_EVENTS PG_COPYRES_TUPLES PQPING_NO_ATTEMPT */
  /* Offset 14 gives the best switch position.  */
  switch (name[14]) {
  case 'D':
    if (memEQ(name, "CONNECTION_NEEDED", 17)) {
    /*                             ^         */
      *iv_return = CONNECTION_NEEDED;
      return PERL_constant_ISIV;
    }
    break;
  case 'E':
    if (memEQ(name, "CONNECTION_SETENV", 17)) {
    /*                             ^         */
      *iv_return = CONNECTION_SETENV;
      return PERL_constant_ISIV;
    }
    if (memEQ(name, "PGRES_EMPTY_QUERY", 17)) {
    /*                             ^         */
      *iv_return = PGRES_EMPTY_QUERY;
      return PERL_constant_ISIV;
    }
    break;
  case 'L':
    if (memEQ(name, "PG_COPYRES_TUPLES", 17)) {
    /*                             ^         */
#ifdef PG_COPYRES_TUPLES
      *iv_return = PG_COPYRES_TUPLES;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'M':
    if (memEQ(name, "PQPING_NO_ATTEMPT", 17)) {
    /*                             ^         */
      *iv_return = PQPING_NO_ATTEMPT;
      return PERL_constant_ISIV;
    }
    break;
  case 'N':
    if (memEQ(name, "PG_COPYRES_EVENTS", 17)) {
    /*                             ^         */
#ifdef PG_COPYRES_EVENTS
      *iv_return = PG_COPYRES_EVENTS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'R':
    if (memEQ(name, "PGRES_FATAL_ERROR", 17)) {
    /*                             ^         */
      *iv_return = PGRES_FATAL_ERROR;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_18 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     CONNECTION_AUTH_OK CONNECTION_CONSUME CONNECTION_STARTED
     PGRES_BAD_RESPONSE PGRES_SINGLE_TUPLE PQPING_NO_RESPONSE */
  /* Offset 14 gives the best switch position.  */
  switch (name[14]) {
  case 'H':
    if (memEQ(name, "CONNECTION_AUTH_OK", 18)) {
    /*                             ^          */
      *iv_return = CONNECTION_AUTH_OK;
      return PERL_constant_ISIV;
    }
    break;
  case 'O':
    if (memEQ(name, "PGRES_BAD_RESPONSE", 18)) {
    /*                             ^          */
      *iv_return = PGRES_BAD_RESPONSE;
      return PERL_constant_ISIV;
    }
    if (memEQ(name, "PQPING_NO_RESPONSE", 18)) {
    /*                             ^          */
      *iv_return = PQPING_NO_RESPONSE;
      return PERL_constant_ISIV;
    }
    break;
  case 'R':
    if (memEQ(name, "CONNECTION_STARTED", 18)) {
    /*                             ^          */
      *iv_return = CONNECTION_STARTED;
      return PERL_constant_ISIV;
    }
    break;
  case 'S':
    if (memEQ(name, "CONNECTION_CONSUME", 18)) {
    /*                             ^          */
      *iv_return = CONNECTION_CONSUME;
      return PERL_constant_ISIV;
    }
    break;
  case 'U':
    if (memEQ(name, "PGRES_SINGLE_TUPLE", 18)) {
    /*                             ^          */
      *iv_return = PGRES_SINGLE_TUPLE;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_20 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     PGRES_NONFATAL_ERROR PGRES_POLLING_ACTIVE PGRES_POLLING_FAILED
     PQSHOW_CONTEXT_NEVER */
  /* Offset 15 gives the best switch position.  */
  switch (name[15]) {
  case 'A':
    if (memEQ(name, "PGRES_POLLING_FAILED", 20)) {
    /*                              ^           */
      *iv_return = PGRES_POLLING_FAILED;
      return PERL_constant_ISIV;
    }
    break;
  case 'C':
    if (memEQ(name, "PGRES_POLLING_ACTIVE", 20)) {
    /*                              ^           */
      *iv_return = PGRES_POLLING_ACTIVE;
      return PERL_constant_ISIV;
    }
    break;
  case 'E':
    if (memEQ(name, "PGRES_NONFATAL_ERROR", 20)) {
    /*                              ^           */
      *iv_return = PGRES_NONFATAL_ERROR;
      return PERL_constant_ISIV;
    }
    break;
  case 'N':
    if (memEQ(name, "PQSHOW_CONTEXT_NEVER", 20)) {
    /*                              ^           */
      *iv_return = PQSHOW_CONTEXT_NEVER;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_21 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     PGRES_POLLING_READING PGRES_POLLING_WRITING PQSHOW_CONTEXT_ALWAYS
     PQSHOW_CONTEXT_ERRORS */
  /* Offset 16 gives the best switch position.  */
  switch (name[16]) {
  case 'A':
    if (memEQ(name, "PGRES_POLLING_READING", 21)) {
    /*                               ^           */
      *iv_return = PGRES_POLLING_READING;
      return PERL_constant_ISIV;
    }
    break;
  case 'I':
    if (memEQ(name, "PGRES_POLLING_WRITING", 21)) {
    /*                               ^           */
      *iv_return = PGRES_POLLING_WRITING;
      return PERL_constant_ISIV;
    }
    break;
  case 'L':
    if (memEQ(name, "PQSHOW_CONTEXT_ALWAYS", 21)) {
    /*                               ^           */
      *iv_return = PQSHOW_CONTEXT_ALWAYS;
      return PERL_constant_ISIV;
    }
    break;
  case 'R':
    if (memEQ(name, "PQSHOW_CONTEXT_ERRORS", 21)) {
    /*                               ^           */
      *iv_return = PQSHOW_CONTEXT_ERRORS;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant (pTHX_ const char *name, STRLEN len, IV *iv_return) {
  /* Initially switch on the length of the name.  */
  /* When generated this function returned values for the list of names given
     in this section of perl code.  Rather than manually editing these functions
     to add or remove constants, which would result in this comment and section
     of code becoming inaccurate, we recommend that you edit this section of
     code, and use it to regenerate a new set of constant functions which you
     then use to replace the originals.

     Regenerate these constant functions by feeding this entire source file to
     perl -x

#!/usr/bin/perl -w
use ExtUtils::Constant qw (constant_types C_constant XS_constant);

my $types = {map {($_, 1)} qw(IV)};
my @names = (qw(PG_COPYRES_ATTRS PG_COPYRES_EVENTS PG_COPYRES_NOTICEHOOKS
	       PG_COPYRES_TUPLES),
            {name=>"CONNECTION_AUTH_OK", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_AWAITING_RESPONSE", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_BAD", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_CHECK_WRITABLE", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_CONSUME", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_MADE", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_NEEDED", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_OK", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_SETENV", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_SSL_STARTUP", type=>"IV", macro=>"1"},
            {name=>"CONNECTION_STARTED", type=>"IV", macro=>"1"},
            {name=>"PGRES_BAD_RESPONSE", type=>"IV", macro=>"1"},
            {name=>"PGRES_COMMAND_OK", type=>"IV", macro=>"1"},
            {name=>"PGRES_COPY_BOTH", type=>"IV", macro=>"1"},
            {name=>"PGRES_COPY_IN", type=>"IV", macro=>"1"},
            {name=>"PGRES_COPY_OUT", type=>"IV", macro=>"1"},
            {name=>"PGRES_EMPTY_QUERY", type=>"IV", macro=>"1"},
            {name=>"PGRES_FATAL_ERROR", type=>"IV", macro=>"1"},
            {name=>"PGRES_NONFATAL_ERROR", type=>"IV", macro=>"1"},
            {name=>"PGRES_POLLING_ACTIVE", type=>"IV", macro=>"1"},
            {name=>"PGRES_POLLING_FAILED", type=>"IV", macro=>"1"},
            {name=>"PGRES_POLLING_OK", type=>"IV", macro=>"1"},
            {name=>"PGRES_POLLING_READING", type=>"IV", macro=>"1"},
            {name=>"PGRES_POLLING_WRITING", type=>"IV", macro=>"1"},
            {name=>"PGRES_SINGLE_TUPLE", type=>"IV", macro=>"1"},
            {name=>"PGRES_TUPLES_OK", type=>"IV", macro=>"1"},
            {name=>"PQERRORS_DEFAULT", type=>"IV", macro=>"1"},
            {name=>"PQERRORS_TERSE", type=>"IV", macro=>"1"},
            {name=>"PQERRORS_VERBOSE", type=>"IV", macro=>"1"},
            {name=>"PQPING_NO_ATTEMPT", type=>"IV", macro=>"1"},
            {name=>"PQPING_NO_RESPONSE", type=>"IV", macro=>"1"},
            {name=>"PQPING_OK", type=>"IV", macro=>"1"},
            {name=>"PQPING_REJECT", type=>"IV", macro=>"1"},
            {name=>"PQSHOW_CONTEXT_ALWAYS", type=>"IV", macro=>"1"},
            {name=>"PQSHOW_CONTEXT_ERRORS", type=>"IV", macro=>"1"},
            {name=>"PQSHOW_CONTEXT_NEVER", type=>"IV", macro=>"1"},
            {name=>"PQTRANS_ACTIVE", type=>"IV", macro=>"1"},
            {name=>"PQTRANS_IDLE", type=>"IV", macro=>"1"},
            {name=>"PQTRANS_INERROR", type=>"IV", macro=>"1"},
            {name=>"PQTRANS_INTRANS", type=>"IV", macro=>"1"},
            {name=>"PQTRANS_UNKNOWN", type=>"IV", macro=>"1"});

print constant_types(), "\n"; # macro defs
foreach (C_constant ("PostgresClient", 'constant', 'IV', $types, undef, 3, @names) ) {
    print $_, "\n"; # C constant subs
}
print "\n#### XS Section:\n";
print XS_constant ("PostgresClient", $types);
__END__
   */

  switch (len) {
  case 9:
    if (memEQ(name, "PQPING_OK", 9)) {
      *iv_return = PQPING_OK;
      return PERL_constant_ISIV;
    }
    break;
  case 12:
    if (memEQ(name, "PQTRANS_IDLE", 12)) {
      *iv_return = PQTRANS_IDLE;
      return PERL_constant_ISIV;
    }
    break;
  case 13:
    return constant_13 (aTHX_ name, iv_return);
    break;
  case 14:
    return constant_14 (aTHX_ name, iv_return);
    break;
  case 15:
    return constant_15 (aTHX_ name, iv_return);
    break;
  case 16:
    return constant_16 (aTHX_ name, iv_return);
    break;
  case 17:
    return constant_17 (aTHX_ name, iv_return);
    break;
  case 18:
    return constant_18 (aTHX_ name, iv_return);
    break;
  case 20:
    return constant_20 (aTHX_ name, iv_return);
    break;
  case 21:
    return constant_21 (aTHX_ name, iv_return);
    break;
  case 22:
    /* Names all of length 22.  */
    /* CONNECTION_SSL_STARTUP PG_COPYRES_NOTICEHOOKS */
    /* Offset 21 gives the best switch position.  */
    switch (name[21]) {
    case 'P':
      if (memEQ(name, "CONNECTION_SSL_STARTU", 21)) {
      /*                                    P      */
        *iv_return = CONNECTION_SSL_STARTUP;
        return PERL_constant_ISIV;
      }
      break;
    case 'S':
      if (memEQ(name, "PG_COPYRES_NOTICEHOOK", 21)) {
      /*                                    S      */
#ifdef PG_COPYRES_NOTICEHOOKS
        *iv_return = PG_COPYRES_NOTICEHOOKS;
        return PERL_constant_ISIV;
#else
        return PERL_constant_NOTDEF;
#endif
      }
      break;
    }
    break;
  case 25:
    if (memEQ(name, "CONNECTION_CHECK_WRITABLE", 25)) {
      *iv_return = CONNECTION_CHECK_WRITABLE;
      return PERL_constant_ISIV;
    }
    break;
  case 28:
    if (memEQ(name, "CONNECTION_AWAITING_RESPONSE", 28)) {
      *iv_return = CONNECTION_AWAITING_RESPONSE;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}
