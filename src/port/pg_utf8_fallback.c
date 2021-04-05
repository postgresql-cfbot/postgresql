/*-------------------------------------------------------------------------
 *
 * pg_utf8_fallback.c
 *	  Validate UTF-8 using plain C.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_utf8_fallback.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include "port/pg_utf8.h"


/*
 * See the comment in common/wchar.c under "multibyte sequence validators".
 */
int
pg_validate_utf8_fallback(const unsigned char *s, int len)
{
	const unsigned char *start = s;
	unsigned char b1,
				b2,
				b3,
				b4;

	while (len > 0)
	{
		int			l;

		/* fast path for ASCII-subset characters */
		l = check_ascii(s, len);
		if (l)
		{
			s += l;
			len -= l;
			continue;
		}

		/* Found non-ASCII or zero above, so verify a single character. */
		if (!IS_HIGHBIT_SET(*s))
		{
			if (*s == '\0')
				break;
			l = 1;
		}
		/* code points U+0080 through U+07FF */
		else if (IS_TWO_BYTE_LEAD(*s))
		{
			l = 2;
			if (len < l)
				break;

			b1 = *s;
			b2 = *(s + 1);

			if (!IS_CONTINUATION_BYTE(b2))
				break;

			/* check 2-byte overlong: 1100.000x.10xx.xxxx */
			if (b1 < 0xC2)
				break;
		}
		/* code points U+0800 through U+D7FF and U+E000 through U+FFFF */
		else if (IS_THREE_BYTE_LEAD(*s))
		{
			l = 3;
			if (len < l)
				break;

			b1 = *s;
			b2 = *(s + 1);
			b3 = *(s + 2);

			if (!IS_CONTINUATION_BYTE(b2) ||
				!IS_CONTINUATION_BYTE(b3))
				break;

			/* check 3-byte overlong: 1110.0000 1001.xxxx 10xx.xxxx */
			if (b1 == 0xE0 && b2 < 0xA0)
				break;

			/* check surrogate: 1110.1101 101x.xxxx 10xx.xxxx */
			if (b1 == 0xED && b2 > 0x9F)
				break;
		}
		/* code points U+010000 through U+10FFFF */
		else if (IS_FOUR_BYTE_LEAD(*s))
		{
			l = 4;
			if (len < l)
				break;

			b1 = *s;
			b2 = *(s + 1);
			b3 = *(s + 2);
			b4 = *(s + 3);

			if (!IS_CONTINUATION_BYTE(b2) ||
				!IS_CONTINUATION_BYTE(b3) ||
				!IS_CONTINUATION_BYTE(b4))
				break;

			/*
			 * check 4-byte overlong: 1111.0000 1000.xxxx 10xx.xxxx 10xx.xxxx
			 */
			if (b1 == 0xF0 && b2 < 0x90)
				break;

			/* check too large: 1111.0100 1001.xxxx 10xx.xxxx 10xx.xxxx */
			if ((b1 == 0xF4 && b2 > 0x8F) || b1 > 0xF4)
				break;
		}
		else
			/* invalid byte */
			break;

		s += l;
		len -= l;
	}

	return s - start;
}
