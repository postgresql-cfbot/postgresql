/*
 * Daitch-Mokotoff Soundex
 *
 * Copyright (c) 2021 Finance Norway
 * Author: Dag Lem <dag@nimrod.no>
 *
 * This implementation of the Daitch-Mokotoff Soundex System aims at high
 * performance.
 *
 * - The processing of each phoneme is initiated by an O(1) table lookup.
 * - For phonemes containing more than one character, a coding tree is traversed
 *   to process the complete phoneme.
 * - The (alternate) soundex codes are produced digit by digit in-place in
 *   another tree structure.
 *
 *  References:
 *
 * https://www.avotaynu.com/soundex.htm
 * https://www.jewishgen.org/InfoFiles/Soundex.html
 * https://familypedia.fandom.com/wiki/Daitch-Mokotoff_Soundex
 * https://stevemorse.org/census/soundex.html (dmlat.php, dmsoundex.php)
 * https://github.com/apache/commons-codec/ (dmrules.txt, DaitchMokotoffSoundex.java)
 * https://metacpan.org/pod/Text::Phonetic (DaitchMokotoff.pm)
 *
 * A few notes on other implementations:
 *
 * - All other known implementations have the same unofficial rules for "UE",
 *   these are also adapted by this implementation (0, 1, NC).
 * - The only other known implementation which is capable of generating all
 *   correct soundex codes in all cases is the JOS Soundex Calculator at
 *   https://www.jewishgen.org/jos/jossound.htm
 * - "J" is considered (only) a vowel in dmlat.php
 * - The official rules for "RS" are commented out in dmlat.php
 * - Identical code digits for adjacent letters are not collapsed correctly in
 *   dmsoundex.php when double digit codes are involved. E.g. "BESST" yields
 *   744300 instead of 743000 as for "BEST".
 * - "J" is considered (only) a consonant in DaitchMokotoffSoundex.java
 * - "Y" is not considered a vowel in DaitchMokotoffSoundex.java
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
*/

#include "daitch_mokotoff.h"

#include "postgres.h"
#include "utils/builtins.h"
#include "mb/pg_wchar.h"

#include <string.h>

/* Internal C implementation */
static char *_daitch_mokotoff(char *word, char *soundex, size_t n);


PG_FUNCTION_INFO_V1(daitch_mokotoff);
Datum
daitch_mokotoff(PG_FUNCTION_ARGS)
{
	text	   *arg = PG_GETARG_TEXT_PP(0);
	char	   *string,
			   *tmp_soundex;
	text	   *soundex;

	/*
	 * The maximum theoretical soundex size is several KB, however in practice
	 * anything but contrived synthetic inputs will yield a soundex size of
	 * less than 100 bytes. We thus allocate and free a temporary work buffer,
	 * and return only the actual soundex result.
	 */
	string = pg_server_to_any(text_to_cstring(arg), VARSIZE_ANY_EXHDR(arg), PG_UTF8);
	tmp_soundex = palloc(DM_MAX_SOUNDEX_CHARS);

	if (!_daitch_mokotoff(string, tmp_soundex, DM_MAX_SOUNDEX_CHARS))
	{
		/* No encodable characters in input. */
		pfree(tmp_soundex);
		PG_RETURN_NULL();
	}

	soundex = cstring_to_text(pg_any_to_server(tmp_soundex, strlen(tmp_soundex), PG_UTF8));
	pfree(tmp_soundex);

	PG_RETURN_TEXT_P(soundex);
}


typedef dm_node dm_nodes[DM_MAX_NODES];
typedef dm_node * dm_leaves[DM_MAX_LEAVES];


/* Template for new node in soundex code tree. */
static const dm_node start_node = {
	.soundex_length = 0,
	.soundex = "000000 ",		/* Six digits + joining space */
	.is_leaf = 0,
	.last_update = 0,
	.code_digit = '\0',
	.prev_code_digits = {'\0', '\0'},
	.next_code_digits = {'\0', '\0'},
	.prev_code_index = 0,
	.next_code_index = 0,
	.next_nodes = {NULL}
};

/* Dummy soundex codes at end of input. */
static dm_codes end_codes[2] =
{
	{
		"X", "X", "X"
	}
};


/* Initialize soundex code tree node for next code digit. */
static void
initialize_node(dm_node * node, int last_update)
{
	if (node->last_update < last_update)
	{
		node->prev_code_digits[0] = node->next_code_digits[0];
		node->prev_code_digits[1] = node->next_code_digits[1];
		node->next_code_digits[0] = '\0';
		node->next_code_digits[1] = '\0';
		node->prev_code_index = node->next_code_index;
		node->next_code_index = 0;
		node->is_leaf = 0;
		node->last_update = last_update;
	}
}


/* Update soundex code tree node with next code digit. */
static void
add_next_code_digit(dm_node * node, int code_index, char code_digit)
{
	/* OR in index 1 or 2. */
	node->next_code_index |= code_index;

	if (!node->next_code_digits[0])
	{
		node->next_code_digits[0] = code_digit;
	}
	else if (node->next_code_digits[0] != code_digit)
	{
		node->next_code_digits[1] = code_digit;
	}
}


/* Mark soundex code tree node as leaf. */
static void
set_leaf(dm_leaves leaves_next, int *num_leaves_next, dm_node * node)
{
	if (!node->is_leaf)
	{
		node->is_leaf = 1;
		leaves_next[(*num_leaves_next)++] = node;
	}
}


/* Find next node corresponding to code digit, or create a new node. */
static dm_node * find_or_create_node(dm_nodes nodes, int *num_nodes,
									 dm_node * node, char code_digit)
{
	dm_node   **next_nodes;
	dm_node    *next_node;

	for (next_nodes = node->next_nodes; (next_node = *next_nodes); next_nodes++)
	{
		if (next_node->code_digit == code_digit)
		{
			return next_node;
		}
	}

	next_node = &nodes[(*num_nodes)++];
	*next_nodes = next_node;

	*next_node = start_node;
	memcpy(next_node->soundex, node->soundex, sizeof(next_node->soundex));
	next_node->soundex_length = node->soundex_length;
	next_node->soundex[next_node->soundex_length++] = code_digit;
	next_node->code_digit = code_digit;
	next_node->next_code_index = node->prev_code_index;

	return next_node;
}


/* Update node for next code digit(s). */
static int
update_node(dm_nodes nodes, dm_node * node, int *num_nodes,
			dm_leaves leaves_next, int *num_leaves_next,
			int letter_no, int prev_code_index, int next_code_index,
			char *next_code_digits, int digit_no)
{
	int			i;
	char		next_code_digit = next_code_digits[digit_no];
	int			num_dirty_nodes = 0;
	dm_node    *dirty_nodes[2];

	initialize_node(node, letter_no);

	if (node->soundex_length == DM_MAX_CODE_DIGITS)
	{
		/* Keep completed soundex code. */
		set_leaf(leaves_next, num_leaves_next, node);
		return 0;
	}

	if (node->prev_code_index && !(node->prev_code_index & prev_code_index))
	{
		/*
		 * If the sound (vowel / consonant) of this letter encoding doesn't
		 * correspond to the coding index of the previous letter, we skip this
		 * letter encoding. Note that currently, only "J" can be either a
		 * vowel or a consonant.
		 */
		return 1;
	}

	if (next_code_digit == 'X' ||
		(digit_no == 0 &&
		 (node->prev_code_digits[0] == next_code_digit ||
		  node->prev_code_digits[1] == next_code_digit)))
	{
		/* The code digit is the same as one of the previous (i.e. not added). */
		dirty_nodes[num_dirty_nodes++] = node;
	}

	if (next_code_digit != 'X' &&
		(digit_no > 0 ||
		 node->prev_code_digits[0] != next_code_digit ||
		 node->prev_code_digits[1]))
	{
		/* The code digit is different from one of the previous (i.e. added). */
		node = find_or_create_node(nodes, num_nodes, node, next_code_digit);
		initialize_node(node, letter_no);
		dirty_nodes[num_dirty_nodes++] = node;
	}

	for (i = 0; i < num_dirty_nodes; i++)
	{
		/* Add code digit leading to the current node. */
		add_next_code_digit(dirty_nodes[i], next_code_index, next_code_digit);

		if (next_code_digits[++digit_no])
		{
			update_node(nodes, dirty_nodes[i], num_nodes,
						leaves_next, num_leaves_next,
						letter_no, prev_code_index, next_code_index,
						next_code_digits, digit_no);
		}
		else
		{
			set_leaf(leaves_next, num_leaves_next, dirty_nodes[i]);
		}
	}

	return 1;
}


/* Update soundex tree leaf nodes. Return 1 when all nodes are completed. */
static int
update_leaves(dm_nodes nodes, int *num_nodes,
			  dm_leaves leaves[2], int *ix_leaves, int *num_leaves,
			  int letter_no, dm_codes * codes, dm_codes * next_codes)
{
	int			i,
				j,
				k,
				code_index;
	dm_code    *code,
			   *next_code;
	int			num_leaves_next = 0;
	int			ix_leaves_next = (*ix_leaves + 1) & 1;	/* Alternate ix: 0, 1 */
	int			finished = 1;

	for (i = 0; i < *num_leaves; i++)
	{
		dm_node    *node = leaves[*ix_leaves][i];

		/* One or two alternate code sequences. */
		for (j = 0; j < 2 && (code = codes[j]) && code[0][0]; j++)
		{
			/* Coding for previous letter - before vowel: 1, all other: 2 */
			int			prev_code_index = (code[0][0] > '1') + 1;

			/* One or two alternate next code sequences. */
			for (k = 0; k < 2 && (next_code = next_codes[k]) && next_code[0][0]; k++)
			{
				/* Determine which code to use. */
				if (letter_no == 0)
				{
					/* This is the first letter. */
					code_index = 0;
				}
				else if (next_code[0][0] <= '1')
				{
					/* The next letter is a vowel. */
					code_index = 1;
				}
				else
				{
					/* All other cases. */
					code_index = 2;
				}

				/* One or two sequential code digits. */
				if (update_node(nodes, node, num_nodes,
								leaves[ix_leaves_next], &num_leaves_next,
								letter_no, prev_code_index, code_index,
								code[code_index], 0))
				{
					finished = 0;
				}
			}
		}
	}

	*ix_leaves = ix_leaves_next;
	*num_leaves = num_leaves_next;

	return finished;
}


/* Mapping from ISO8859-1 to upper-case ASCII */
static const char tr_iso8859_1_to_ascii_upper[] =
/*
"`abcdefghijklmnopqrstuvwxyz{|}~                                  ¡¢£¤¥¦§¨©ª«¬ ®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"
*/
"`ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}~                                  !                             ?AAAAAAECEEEEIIIIDNOOOOO*OUUUUYDSAAAAAAECEEEEIIIIDNOOOOO/OUUUUYDY";

static char
iso8859_1_to_ascii_upper(unsigned char c)
{
	return c >= 0x60 ? tr_iso8859_1_to_ascii_upper[c - 0x60] : c;
}


/* Convert an UTF-8 character to ISO-8859-1.
 * Unconvertable characters are returned as '?'.
 * NB! Beware of the domain specific conversion of Ą, Ę, and Ţ/Ț.
 */
static char
utf8_to_iso8859_1(char *str, int *ix)
{
	const char	unknown = '?';
	unsigned char c,
				c2;
	unsigned int code_point;

	/* First byte. */
	c = (unsigned char) str[(*ix)++];
	if (c < 0x80)
	{
		/* ASCII code point. */
		if (c >= '[' && c <= ']')
		{
			/* Codes reserved for Ą, Ę, and Ţ/Ț. */
			return unknown;
		}

		return c;
	}

	/* Second byte. */
	c2 = (unsigned char) str[(*ix)++];
	if (!c2)
	{
		/* The UTF-8 character is cut short (invalid code point). */
		(*ix)--;
		return unknown;
	}

	if (c < 0xE0)
	{
		/* Two-byte character. */
		code_point = ((c & 0x1F) << 6) | (c2 & 0x3F);
		if (code_point < 0x100)
		{
			/* ISO-8859-1 code point. */
			return code_point;
		}
		else if (code_point == 0x0104 || code_point == 0x0105)
		{
			/* Ą/ą */
			return '[';
		}
		else if (code_point == 0x0118 || code_point == 0x0119)
		{
			/* Ę/ę */
			return '\\';
		}
		else if (code_point == 0x0162 || code_point == 0x0163 ||
				 code_point == 0x021A || code_point == 0x021B)
		{
			/* Ţ/ţ or Ț/ț */
			return ']';
		}
		else
		{
			return unknown;
		}
	}

	/* Third byte. */
	if (!str[(*ix)++])
	{
		/* The UTF-8 character is cut short (invalid code point). */
		(*ix)--;
		return unknown;
	}

	if (c < 0xF0)
	{
		/* Three-byte character. */
		return unknown;
	}

	/* Fourth byte. */
	if (!str[(*ix)++])
	{
		/* The UTF-8 character is cut short (invalid code point). */
		(*ix)--;
	}

	return unknown;
}


/* Return next character, converted from UTF-8 to uppercase ASCII. */
static char
read_char(char *str, int *ix)
{
	return iso8859_1_to_ascii_upper(utf8_to_iso8859_1(str, ix));
}


/* Read next ASCII character, skipping any characters not in [A-\]]. */
static char
read_valid_char(char *str, int *ix)
{
	char		c;

	while ((c = read_char(str, ix)))
	{
		if (c >= 'A' && c <= ']')
		{
			break;
		}
	}

	return c;
}


/* Return sound coding for "letter" (letter sequence) */
static dm_codes *
read_letter(char *str, int *ix)
{
	char		c,
				cmp;
	int			i,
				j;
	dm_letter  *letters;
	dm_codes   *codes;

	/* First letter in sequence. */
	if (!(c = read_valid_char(str, ix)))
	{
		return NULL;
	}
	letters = &letter_[c - 'A'];
	codes = letters->codes;
	i = *ix;

	/* Any subsequent letters in sequence. */
	while ((letters = letters->letters) && (c = read_valid_char(str, &i)))
	{
		for (j = 0; (cmp = letters[j].letter); j++)
		{
			if (cmp == c)
			{
				/* Letter found. */
				letters = &letters[j];
				if (letters->codes)
				{
					/* Coding for letter sequence found. */
					codes = letters->codes;
					*ix = i;
				}
				break;
			}
		}
		if (!cmp)
		{
			/* The sequence of letters has no coding. */
			break;
		}
	}

	return codes;
}


/* Generate all Daitch-Mokotoff soundex codes for word, separated by space. */
static char *
_daitch_mokotoff(char *word, char *soundex, size_t n)
{
	int			i = 0,
				j;
	int			letter_no = 0;
	int			ix_leaves = 0;
	int			num_nodes = 0,
				num_leaves = 0;
	dm_codes   *codes,
			   *next_codes;
	dm_node    *nodes;
	dm_leaves  *leaves;

	/* First letter. */
	if (!(codes = read_letter(word, &i)))
	{
		/* No encodable character in input. */
		return NULL;
	}

	/* Allocate memory for node tree. */
	nodes = palloc(sizeof(dm_nodes));
	leaves = palloc(2 * sizeof(dm_leaves));

	/* Starting point. */
	nodes[num_nodes++] = start_node;
	leaves[ix_leaves][num_leaves++] = &nodes[0];

	while (codes)
	{
		next_codes = read_letter(word, &i);

		/* Update leaf nodes. */
		if (update_leaves(nodes, &num_nodes,
						  leaves, &ix_leaves, &num_leaves,
						  letter_no, codes, next_codes ? next_codes : end_codes))
		{
			/* All soundex codes are completed to six digits. */
			break;
		}

		codes = next_codes;
		letter_no++;
	}

	/* Concatenate all generated soundex codes. */
	for (i = 0, j = 0;
		 i < num_leaves && j + DM_MAX_CODE_DIGITS + 1 <= n;
		 i++, j += DM_MAX_CODE_DIGITS + 1)
	{
		memcpy(&soundex[j], leaves[ix_leaves][i]->soundex, DM_MAX_CODE_DIGITS + 1);
	}

	/* Terminate string. */
	soundex[j - 1] = '\0';

	pfree(leaves);
	pfree(nodes);

	return soundex;
}
