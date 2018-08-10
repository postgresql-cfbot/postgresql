/*-------------------------------------------------------------------------
 *
 * spell.h
 *
 * Declarations for ISpell dictionary
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * src/include/tsearch/dicts/spell.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __SPELL_H__
#define __SPELL_H__

#include "regex/regex.h"
#include "tsearch/dicts/regis.h"
#include "tsearch/ts_public.h"

#define ISPELL_INVALID_INDEX	(0x7FFFF)
#define ISPELL_INVALID_OFFSET	(0xFFFFFFFF)

/*
 * SPNode and SPNodeData are used to represent prefix tree (Trie) to store
 * a words list.
 */
typedef struct
{
	uint32		val:8,
				isword:1,
	/* Stores compound flags listed below */
				compoundflag:4,
	/* Index of an entry of the AffixData field */
				affix:19;
	/* Offset to a node of the DictNodes field */
	uint32		node_offset;
} SPNodeData;

/*
 * Names of FF_ are correlated with Hunspell options in affix file
 * http://hunspell.sourceforge.net/
 */
#define FF_COMPOUNDONLY		0x01
#define FF_COMPOUNDBEGIN	0x02
#define FF_COMPOUNDMIDDLE	0x04
#define FF_COMPOUNDLAST		0x08
#define FF_COMPOUNDFLAG		( FF_COMPOUNDBEGIN | FF_COMPOUNDMIDDLE | \
							FF_COMPOUNDLAST )
#define FF_COMPOUNDFLAGMASK		0x0f

typedef struct SPNode
{
	uint32		length;
	SPNodeData	data[FLEXIBLE_ARRAY_MEMBER];
} SPNode;

#define SPNHDRSZ	(offsetof(SPNode,data))

/*
 * Represents an entry in a words list.
 */
typedef struct spell_struct
{
	union
	{
		/*
		 * flag is filled in by NIImportDictionary(). After
		 * NISortDictionary(), d is used instead of flag.
		 */
		char	   *flag;
		/* d is used in mkSPNode() */
		struct
		{
			/* Reference to an entry of the AffixData field */
			int			affix;
			/* Length of the word */
			int			len;
		}			d;
	}			p;
	char		word[FLEXIBLE_ARRAY_MEMBER];
} SPELL;

#define SPELLHDRSZ	(offsetof(SPELL, word))

/*
 * Represents an entry in an affix list.
 */
typedef struct aff_struct
{
	/* FF_SUFFIX or FF_PREFIX */
	uint16		type:1,
				flagflags:7,
				issimple:1,
				isregis:1,
				flaglen:2;

	/* 8 bytes could be too mach for repl, find and mask, but who knows */
	uint8		replen;
	uint8		findlen;
	uint8		masklen;

	/*
	 * fields stores the following data (each ends with \0):
	 * - repl
	 * - find
	 * - mask
	 * - flag - one character (if FM_CHAR),
	 *          two characters (if FM_LONG),
	 *          number, >= 0 and < 65536 (if FM_NUM).
	 */
	char		fields[FLEXIBLE_ARRAY_MEMBER];
} AFFIX;

#define AF_FLAG_MAXSIZE		5		/* strlen(65536) */
#define AF_REPL_MAXSIZE		255		/* 8 bytes */

#define AFFIXHDRSZ	(offsetof(AFFIX, fields))

#define AffixFieldRepl(af)	((af)->fields)
#define AffixFieldFind(af)	((af)->fields + (af)->replen + 1)
#define AffixFieldMask(af)	(AffixFieldFind(af) + (af)->findlen + 1)
#define AffixFieldFlag(af)	(AffixFieldMask(af) + (af)->masklen + 1)
#define AffixGetSize(af)	(AFFIXHDRSZ + (af)->replen + 1 + (af)->findlen + 1 \
							 + (af)->masklen + 1 + strlen(AffixFieldFlag(af)) + 1)

/*
 * Stores compiled regular expression of affix. AffixReg uses mask field of
 * AFFIX as a regular expression.
 */
typedef struct AffixReg
{
	bool		iscompiled;
	union
	{
		regex_t		regex;
		Regis		regis;
	}			r;
} AffixReg;

/*
 * affixes use dictionary flags too
 */
#define FF_COMPOUNDPERMITFLAG	0x10
#define FF_COMPOUNDFORBIDFLAG	0x20
#define FF_CROSSPRODUCT			0x40

/*
 * Don't change the order of these. Initialization sorts by these,
 * and expects prefixes to come first after sorting.
 */
#define FF_SUFFIX				1
#define FF_PREFIX				0

/*
 * AffixNode and AffixNodeData are used to represent prefix tree (Trie) to store
 * an affix list.
 */
typedef struct
{
	uint8		val;
	uint32		affstart;
	uint32		affend;
	/* Offset to a node of the PrefixNodes or SuffixNodes field */
	uint32		node_offset;
} AffixNodeData;

typedef struct AffixNode
{
	uint32		isvoid:1,
				length:31;
	AffixNodeData data[FLEXIBLE_ARRAY_MEMBER];
} AffixNode;

#define ANHRDSZ		   (offsetof(AffixNode, data))

typedef struct NodeArray
{
	char	   *Nodes;
	uint32		NodesSize;	/* allocated size of Nodes */
	uint32		NodesEnd;	/* end of data in Nodes */
} NodeArray;

#define NodeArrayGet(na, of) \
	(((of) == ISPELL_INVALID_OFFSET) ? NULL : (na)->Nodes + (of))

typedef struct
{
	/* Index of an affix of the Affix field */
	uint32		affix;
	int			len;
	bool		issuffix;
} CMPDAffix;

/*
 * Type of encoding affix flags in Hunspell dictionaries
 */
typedef enum
{
	FM_CHAR,					/* one character (like ispell) */
	FM_LONG,					/* two characters */
	FM_NUM						/* number, >= 0 and < 65536 */
} FlagMode;

/*
 * Structure to store Hunspell options. Flag representation depends on flag
 * type. These flags are about support of compound words.
 */
typedef struct CompoundAffixFlag
{
	union
	{
		/* Flag name if flagMode is FM_CHAR or FM_LONG */
		char	   *s;
		/* Flag name if flagMode is FM_NUM */
		uint32		i;
	}			flag;
	/* we don't have a bsearch_arg version, so, copy FlagMode */
	FlagMode	flagMode;
	uint32		value;
} CompoundAffixFlag;

#define FLAGNUM_MAXSIZE		(1 << 16)

typedef struct IspellDictData
{
	FlagMode	flagMode;
	bool		usecompound;

	bool		useFlagAliases;

	uint32		nAffixData;
	uint32		AffixDataStart;

	uint32		AffixOffsetStart;
	uint32		AffixStart;
	uint32		nAffix;

	uint32		DictNodesStart;
	uint32		PrefixNodesStart;
	uint32		SuffixNodesStart;

	uint32		CompoundAffixStart;

	/*
	 * data stores:
	 * - AffixData - array of affix sets
	 * - Affix - sorted array of affixes
	 * - DictNodes - prefix tree of a word list
	 * - PrefixNodes - prefix tree of a prefix list
	 * - SuffixNodes - prefix tree of a suffix list
	 * - CompoundAffix - array of compound affixes
	 */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} IspellDictData;

#define IspellDictDataHdrSize	(offsetof(IspellDictData, data))

#define DictAffixDataOffset(d)	((d)->data)
#define DictAffixData(d)		((d)->data + (d)->AffixDataStart)
#define DictAffixDataGet(d, i) \
	(((i) == ISPELL_INVALID_INDEX) ? NULL : \
	 (AssertMacro(i < (d)->nAffixData), \
	  DictAffixData(d) + ((uint32 *) DictAffixDataOffset(d))[i]))

#define DictAffixOffset(d)		((d)->data + (d)->AffixOffsetStart)
#define DictAffix(d)			((d)->data + (d)->AffixStart)
#define DictAffixGet(d, i) \
	(((i) == ISPELL_INVALID_INDEX) ? NULL : \
	 (AssertMacro(i < (d)->nAffix), \
	  DictAffix(d) + ((uint32 *) DictAffixOffset(d))[i]))

#define DictDictNodes(d)		((d)->data + (d)->DictNodesStart)
#define DictPrefixNodes(d)		((d)->data + (d)->PrefixNodesStart)
#define DictSuffixNodes(d)		((d)->data + (d)->SuffixNodesStart)
#define DictNodeGet(node_start, of) \
	(((of) == ISPELL_INVALID_OFFSET) ? NULL : (char *) (node_start) + (of))

#define DictCompoundAffix(d)	((d)->data + (d)->CompoundAffixStart)

/*
 * IspellDictBuild is used to initialize IspellDictData struct.  This is a
 * temprorary structure which is setup by NIStartBuild() and released by
 * NIFinishBuild().
 */
typedef struct IspellDictBuild
{
	MemoryContext buildCxt;		/* temp context for construction */

	IspellDictData *dict;
	uint32		dict_size;

	/* Temporary data */

	/* Array of Hunspell options in affix file */
	CompoundAffixFlag *CompoundAffixFlags;
	/* number of entries in CompoundAffixFlags array */
	int			nCompoundAffixFlag;
	/* allocated length of CompoundAffixFlags array */
	int			mCompoundAffixFlag;

	/* Array of all words in the dict file */
	SPELL	  **Spell;
	int			nSpell;			/* number of valid entries in Spell array */
	int			mSpell;			/* allocated length of Spell array */

	/* Data for IspellDictData */

	/* Array of all affixes in the aff file */
	AFFIX	  **Affix;
	int			nAffix;			/* number of valid entries in Affix array */
	int			mAffix;			/* allocated length of Affix array */
	uint32		AffixSize;

	/* Array of sets of affixes */
	uint32	   *AffixDataOffset;
	int			nAffixData;		/* number of affix sets */
	int			mAffixData;		/* allocated number of affix sets */
	char	   *AffixData;
	uint32		AffixDataSize;	/* allocated size of AffixData */
	uint32		AffixDataEnd;	/* end of data in AffixData */

	/* Prefix tree which stores a word list */
	NodeArray	DictNodes;

	/* Prefix tree which stores a prefix list */
	NodeArray	PrefixNodes;

	/* Prefix tree which stores a suffix list */
	NodeArray	SuffixNodes;

	/* Array of compound affixes */
	CMPDAffix  *CompoundAffix;
	int			nCompoundAffix;	/* number of entries of CompoundAffix */
} IspellDictBuild;

#define AffixDataGet(d, i)		((d)->AffixData + (d)->AffixDataOffset[i])

/*
 * IspellDict is used within NINormalizeWord.
 */
typedef struct IspellDict
{
	/*
	 * Pointer to a DSM location of IspellDictData. Should be retreived per
	 * every dispell_lexize() call.
	 */
	IspellDictData *dict;
	/*
	 * Array of regular expression of affixes. Each regular expression is
	 * compiled only on demand.
	 */
	AffixReg   *reg;
	/*
	 * Memory context for compiling regular expressions.
	 */
	MemoryContext dictCtx;
} IspellDict;

extern TSLexeme *NINormalizeWord(IspellDict *Conf, char *word);

extern void NIStartBuild(IspellDictBuild *ConfBuild);
extern void NIImportAffixes(IspellDictBuild *ConfBuild, const char *filename);
extern void NIImportDictionary(IspellDictBuild *ConfBuild,
							   const char *filename);
extern void NISortDictionary(IspellDictBuild *ConfBuild);
extern void NISortAffixes(IspellDictBuild *ConfBuild);
extern void NICopyData(IspellDictBuild *ConfBuild);
extern void NIFinishBuild(IspellDictBuild *ConfBuild);

#endif
