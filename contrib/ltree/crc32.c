/* contrib/ltree/crc32.c */

/*
 * Implements CRC-32, as used in ltree.
 *
 * Note that the CRC is used in the on-disk format of GiST indexes, so we
 * must stay backwards-compatible!
 */

#include "postgres.h"

/*
 * Below we ignore the fact that LOWER_NODE is defined when compiling with
 * MSVC.  The reason for this is that earlier versions of the MSVC build
 * scripts failed to define LOWER_NODE.  More recent version of the MSVC
 * build scripts parse makefiles which results in LOWER_NODE now being
 * defined.  We check for _MSC_VER here so as not to break pg_upgrade when
 * upgrading from versions MSVC versions where LOWER_NODE was not defined.
 */
#if defined(LOWER_NODE) && !defined(_MSC_VER)
#include <ctype.h>
#define TOLOWER(x)	tolower((unsigned char) (x))
#else
#define TOLOWER(x)	(x)
#endif

#include "crc32.h"
#include "utils/pg_crc.h"

unsigned int
ltree_crc32_sz(const char *buf, int size)
{
	pg_crc32	crc;
	const char *p = buf;

	INIT_TRADITIONAL_CRC32(crc);
	while (size > 0)
	{
		char		c = (char) TOLOWER(*p);

		COMP_TRADITIONAL_CRC32(crc, &c, 1);
		size--;
		p++;
	}
	FIN_TRADITIONAL_CRC32(crc);
	return (unsigned int) crc;
}
