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

#include "postgres.h"

#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"


/*
 * The soundex coding chart table is adapted from
 * https://www.jewishgen.org/InfoFiles/Soundex.html
 * See daitch_mokotoff_header.pl for details.
*/

/* Generated coding chart table */
#include "daitch_mokotoff.h"

#define DM_CODE_DIGITS 6

/* Node in soundex code tree */
struct dm_node
{
	int			soundex_length; /* Length of generated soundex code */
	char		soundex[DM_CODE_DIGITS];	/* Soundex code */
	int			is_leaf;		/* Candidate for complete soundex code */
	int			last_update;	/* Letter number for last update of node */
	char		code_digit;		/* Last code digit, 0 - 9 */

	/*
	 * One or two alternate code digits leading to this node. If there are two
	 * digits, one of them is always an 'X'. Repeated code digits and 'X' lead
	 * back to the same node.
	 */
	char		prev_code_digits[2];
	/* One or two alternate code digits moving forward. */
	char		next_code_digits[2];
	/* ORed together code index(es) used to reach current node. */
	int			prev_code_index;
	int			next_code_index;
	/* Possible nodes branching out from this node - digits 0-9. */
	struct dm_node *children[10];
	/* Next node in linked list. Alternating index for each iteration. */
	struct dm_node *next[2];
};

typedef struct dm_node dm_node;


/* Internal C implementation */
static int	daitch_mokotoff_coding(char *word, ArrayBuildState *soundex, MemoryContext tmp_ctx);


PG_FUNCTION_INFO_V1(daitch_mokotoff);

Datum
daitch_mokotoff(PG_FUNCTION_ARGS)
{
	text	   *arg = PG_GETARG_TEXT_PP(0);
	char	   *string;
	ArrayBuildState *soundex;
	Datum		retval;
	MemoryContext mem_ctx,
				tmp_ctx;

	tmp_ctx = AllocSetContextCreate(CurrentMemoryContext,
									"daitch_mokotoff temporary context",
									ALLOCSET_DEFAULT_SIZES);
	mem_ctx = MemoryContextSwitchTo(tmp_ctx);

	string = pg_server_to_any(text_to_cstring(arg), VARSIZE_ANY_EXHDR(arg), PG_UTF8);
	soundex = initArrayResult(TEXTOID, tmp_ctx, false);

	if (!daitch_mokotoff_coding(string, soundex, tmp_ctx))
	{
		/* No encodable characters in input. */
		MemoryContextSwitchTo(mem_ctx);
		MemoryContextDelete(tmp_ctx);
		PG_RETURN_NULL();
	}

	retval = makeArrayResult(soundex, mem_ctx);
	MemoryContextSwitchTo(mem_ctx);
	MemoryContextDelete(tmp_ctx);

	PG_RETURN_DATUM(retval);
}


/* Template for new node in soundex code tree. */
static const dm_node start_node = {
	.soundex_length = 0,
	.soundex = "000000",		/* Six digits */
	.is_leaf = 0,
	.last_update = 0,
	.code_digit = '\0',
	.prev_code_digits = {'\0', '\0'},
	.next_code_digits = {'\0', '\0'},
	.prev_code_index = 0,
	.next_code_index = 0,
	.children = {NULL},
	.next = {NULL}
};

/* Dummy soundex codes at end of input. */
static const dm_codes end_codes[2] =
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
set_leaf(dm_node * first_node[2], dm_node * last_node[2], dm_node * node, int ix_node)
{
	if (!node->is_leaf)
	{
		node->is_leaf = 1;

		if (first_node[ix_node] == NULL)
		{
			first_node[ix_node] = node;
		}
		else
		{
			last_node[ix_node]->next[ix_node] = node;
		}

		last_node[ix_node] = node;
		node->next[ix_node] = NULL;
	}
}


/* Find next node corresponding to code digit, or create a new node. */
static dm_node *
find_or_create_child_node(dm_node * parent, char code_digit, ArrayBuildState *soundex, MemoryContext tmp_ctx)
{
	int			i = code_digit - '0';
	dm_node   **nodes = parent->children;
	dm_node    *node = nodes[i];

	if (node)
	{
		/* Found existing child node. Skip completed nodes. */
		return node->soundex_length < DM_CODE_DIGITS ? node : NULL;
	}

	/* Create new child node. */
	node = palloc(sizeof(dm_node));
	nodes[i] = node;

	*node = start_node;
	memcpy(node->soundex, parent->soundex, sizeof(parent->soundex));
	node->soundex_length = parent->soundex_length;
	node->soundex[node->soundex_length++] = code_digit;
	node->code_digit = code_digit;
	node->next_code_index = node->prev_code_index;

	if (node->soundex_length < DM_CODE_DIGITS)
	{
		return node;
	}
	else
	{
		/* Append completed soundex code to soundex array. */
		accumArrayResult(soundex,
						 PointerGetDatum(cstring_to_text_with_len(node->soundex, DM_CODE_DIGITS)),
						 false,
						 TEXTOID,
						 tmp_ctx);
		return NULL;
	}
}


/* Update node for next code digit(s). */
static void
update_node(dm_node * first_node[2], dm_node * last_node[2], dm_node * node, int ix_node,
			int letter_no, int prev_code_index, int next_code_index,
			const char *next_code_digits, int digit_no,
			ArrayBuildState *soundex, MemoryContext tmp_ctx)
{
	int			i;
	char		next_code_digit = next_code_digits[digit_no];
	int			num_dirty_nodes = 0;
	dm_node    *dirty_nodes[2];

	initialize_node(node, letter_no);

	if (node->prev_code_index && !(node->prev_code_index & prev_code_index))
	{
		/*
		 * If the sound (vowel / consonant) of this letter encoding doesn't
		 * correspond to the coding index of the previous letter, we skip this
		 * letter encoding. Note that currently, only "J" can be either a
		 * vowel or a consonant.
		 */
		return;
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
		node = find_or_create_child_node(node, next_code_digit, soundex, tmp_ctx);
		if (node)
		{
			initialize_node(node, letter_no);
			dirty_nodes[num_dirty_nodes++] = node;
		}
	}

	for (i = 0; i < num_dirty_nodes; i++)
	{
		/* Add code digit leading to the current node. */
		add_next_code_digit(dirty_nodes[i], next_code_index, next_code_digit);

		if (next_code_digits[++digit_no])
		{
			update_node(first_node, last_node, dirty_nodes[i], ix_node,
						letter_no, prev_code_index, next_code_index,
						next_code_digits, digit_no,
						soundex, tmp_ctx);
		}
		else
		{
			/* Add incomplete leaf node to linked list. */
			set_leaf(first_node, last_node, dirty_nodes[i], ix_node);
		}
	}
}


/* Update soundex tree leaf nodes. */
static void
update_leaves(dm_node * first_node[2], int *ix_node, int letter_no,
			  const dm_codes * codes, const dm_codes * next_codes,
			  ArrayBuildState *soundex, MemoryContext tmp_ctx)
{
	int			i,
				j,
				code_index;
	dm_node    *node,
			   *last_node[2];
	const		dm_code *code,
			   *next_code;
	int			ix_node_next = (*ix_node + 1) & 1;	/* Alternating index: 0, 1 */

	/* Initialize for new linked list of leaves. */
	first_node[ix_node_next] = NULL;
	last_node[ix_node_next] = NULL;

	/* Process all nodes. */
	for (node = first_node[*ix_node]; node; node = node->next[*ix_node])
	{
		/* One or two alternate code sequences. */
		for (i = 0; i < 2 && (code = codes[i]) && code[0][0]; i++)
		{
			/* Coding for previous letter - before vowel: 1, all other: 2 */
			int			prev_code_index = (code[0][0] > '1') + 1;

			/* One or two alternate next code sequences. */
			for (j = 0; j < 2 && (next_code = next_codes[j]) && next_code[0][0]; j++)
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
				update_node(first_node, last_node, node, ix_node_next,
							letter_no, prev_code_index, code_index,
							code[code_index], 0,
							soundex, tmp_ctx);
			}
		}
	}

	*ix_node = ix_node_next;
}


/* Mapping from ISO8859-1 to upper-case ASCII */
static const char iso8859_1_to_ascii_upper[] =
/*
"`abcdefghijklmnopqrstuvwxyz{|}~                                  ¡¢£¤¥¦§¨©ª«¬ ®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"
*/
"`ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}~                                  !                             ?AAAAAAECEEEEIIIIDNOOOOO*OUUUUYDSAAAAAAECEEEEIIIIDNOOOOO/OUUUUYDY";


/* Return next character, converted from UTF-8 to uppercase ASCII. */
static char
read_char(unsigned char *str, int *ix)
{
	/* Substitute character for skipped code points. */
	const char	na = '\x1a';
	pg_wchar	c;

	/* Decode UTF-8 character to ISO 10646 code point. */
	str += *ix;
	c = utf8_to_unicode(str);
	*ix += pg_utf_mblen(str);

	if (c >= (unsigned char) '[' && c <= (unsigned char) ']')
	{
		/* ASCII characters [, \, and ] are reserved for Ą, Ę, and Ţ/Ț. */
		return na;
	}
	else if (c < 0x60)
	{
		/* Non-lowercase ASCII character. */
		return c;
	}
	else if (c < 0x100)
	{
		/* ISO-8859-1 code point, converted to upper-case ASCII. */
		return iso8859_1_to_ascii_upper[c - 0x60];
	}
	else
	{
		/* Conversion of non-ASCII characters in the coding chart. */
		switch (c)
		{
			case 0x0104:
			case 0x0105:
				/* Ą/ą */
				return '[';
			case 0x0118:
			case 0x0119:
				/* Ę/ę */
				return '\\';
			case 0x0162:
			case 0x0163:
			case 0x021A:
			case 0x021B:
				/* Ţ/ţ or Ț/ț */
				return ']';
			default:
				return na;
		}
	}
}


/* Read next ASCII character, skipping any characters not in [A-\]]. */
static char
read_valid_char(char *str, int *ix)
{
	char		c;

	while ((c = read_char((unsigned char *) str, ix)))
	{
		if (c >= 'A' && c <= ']')
		{
			break;
		}
	}

	return c;
}


/* Return sound coding for "letter" (letter sequence) */
static const dm_codes *
read_letter(char *str, int *ix)
{
	char		c,
				cmp;
	int			i,
				j;
	const		dm_letter *letters;
	const		dm_codes *codes;

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


/* Generate all Daitch-Mokotoff soundex codes for word. */
static int
daitch_mokotoff_coding(char *word, ArrayBuildState *soundex, MemoryContext tmp_ctx)
{
	int			i = 0;
	int			letter_no = 0;
	int			ix_node = 0;
	const		dm_codes *codes,
			   *next_codes;
	dm_node    *first_node[2],
			   *node;

	/* First letter. */
	if (!(codes = read_letter(word, &i)))
	{
		/* No encodable character in input. */
		return 0;
	}

	/* Starting point. */
	first_node[ix_node] = palloc(sizeof(dm_node));
	*first_node[ix_node] = start_node;

	/*
	 * Loop until either the word input is exhausted, or all generated soundex
	 * codes are completed to six digits.
	 */
	while (codes && first_node[ix_node])
	{
		next_codes = read_letter(word, &i);

		/* Update leaf nodes. */
		update_leaves(first_node, &ix_node, letter_no,
					  codes, next_codes ? next_codes : end_codes,
					  soundex, tmp_ctx);

		codes = next_codes;
		letter_no++;
	}

	/* Append all remaining (incomplete) soundex codes. */
	for (node = first_node[ix_node]; node; node = node->next[ix_node])
	{
		accumArrayResult(soundex,
						 PointerGetDatum(cstring_to_text_with_len(node->soundex, DM_CODE_DIGITS)),
						 false,
						 TEXTOID,
						 tmp_ctx);
	}

	return 1;
}
