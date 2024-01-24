/* Processed by ecpg (regression mode) */
/* These include files are added by the preprocessor */
#include <ecpglib.h>
#include <ecpgerrno.h>
#include <sqlca.h>
/* End of automatic include section */
#define ECPGdebug(X,Y) ECPGdebug((X)+100,(Y))

#line 1 "sqljson_jsontable.pgc"
#include <stdio.h>


#line 1 "sqlca.h"
#ifndef POSTGRES_SQLCA_H
#define POSTGRES_SQLCA_H

#ifndef PGDLLIMPORT
#if  defined(WIN32) || defined(__CYGWIN__)
#define PGDLLIMPORT __declspec (dllimport)
#else
#define PGDLLIMPORT
#endif							/* __CYGWIN__ */
#endif							/* PGDLLIMPORT */

#define SQLERRMC_LEN	150

#ifdef __cplusplus
extern "C"
{
#endif

struct sqlca_t
{
	char		sqlcaid[8];
	long		sqlabc;
	long		sqlcode;
	struct
	{
		int			sqlerrml;
		char		sqlerrmc[SQLERRMC_LEN];
	}			sqlerrm;
	char		sqlerrp[8];
	long		sqlerrd[6];
	/* Element 0: empty						*/
	/* 1: OID of processed tuple if applicable			*/
	/* 2: number of rows processed				*/
	/* after an INSERT, UPDATE or				*/
	/* DELETE statement					*/
	/* 3: empty						*/
	/* 4: empty						*/
	/* 5: empty						*/
	char		sqlwarn[8];
	/* Element 0: set to 'W' if at least one other is 'W'	*/
	/* 1: if 'W' at least one character string		*/
	/* value was truncated when it was			*/
	/* stored into a host variable.             */

	/*
	 * 2: if 'W' a (hopefully) non-fatal notice occurred
	 */	/* 3: empty */
	/* 4: empty						*/
	/* 5: empty						*/
	/* 6: empty						*/
	/* 7: empty						*/

	char		sqlstate[5];
};

struct sqlca_t *ECPGget_sqlca(void);

#ifndef POSTGRES_ECPG_INTERNAL
#define sqlca (*ECPGget_sqlca())
#endif

#ifdef __cplusplus
}
#endif

#endif

#line 3 "sqljson_jsontable.pgc"


#line 1 "regression.h"






#line 4 "sqljson_jsontable.pgc"


/* exec sql whenever sqlerror  sqlprint ; */
#line 6 "sqljson_jsontable.pgc"


int
main ()
{
  ECPGdebug (1, stderr);

  { ECPGconnect(__LINE__, 0, "ecpg1_regression" , NULL, NULL , NULL, 0); 
#line 13 "sqljson_jsontable.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 13 "sqljson_jsontable.pgc"

  { ECPGsetcommit(__LINE__, "on", NULL);
#line 14 "sqljson_jsontable.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 14 "sqljson_jsontable.pgc"


  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "select * from json_table ( jsonb 'null' , '$[*]' as p0 columns ( nested '$' as p1 columns ( nested path '$' as p11 columns ( foo int ) , nested path '$' as p12 columns ( bar int ) ) , nested path '$' as p2 columns ( nested path '$' as p21 columns ( baz int ) ) ) plan ( p1 ) ) jt", ECPGt_EOIT, ECPGt_EORT);
#line 26 "sqljson_jsontable.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 26 "sqljson_jsontable.pgc"

  // error

  { ECPGdisconnect(__LINE__, "CURRENT");
#line 29 "sqljson_jsontable.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 29 "sqljson_jsontable.pgc"


  return 0;
}
