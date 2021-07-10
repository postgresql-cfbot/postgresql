/*-------------------------------------------------------------------------
 *
 * zson.c
 * 	An extension for transparent JSONB compression. Compression is based
 * 	on a shared dictionary of most frequently used strings.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/zson/zson.c
 *
 *-------------------------------------------------------------------------
 */

#include <postgres.h>
#include <port.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <sys/time.h>
#include <limits.h>
#include <string.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(zson_in);
PG_FUNCTION_INFO_V1(zson_out);
PG_FUNCTION_INFO_V1(zson_to_jsonb);
PG_FUNCTION_INFO_V1(jsonb_to_zson);
PG_FUNCTION_INFO_V1(zson_info);

/*
ZSON has the following binary format:

	VARHDRSZ
	zson_version [uint8]
	dict_version [uint32]
	decoded_size [uint32]
	hint [uint8 x PGLZ_HINT_SIZE]
	{
		skip_bytes [uint8]
		... skip_bytes as-is bytes ...
		string_code [uint16], 0 = no_string
	} *
*/

/* Binary format version */
#define ZSON_CURRENT_VERSION 0

/* Number of bytes occupied by zson_version, dict_version and decoded_size */
#define ZSON_HEADER_SIZE (sizeof(uint8) + sizeof(uint32)*2)

/* Extracts zson_version */
#define ZSON_HEADER_VERSION(hdrp) (*(uint8*)hdrp)

/* Extracts dict_version */
#define ZSON_HEADER_DICT_VERSION(hdrp) \
	(*(uint32*)((uint8*)hdrp + sizeof(uint8)))

/* Extracts decoded_size */
#define ZSON_HEADER_DECODED_SIZE(hdrp) \
	(*(uint32*)((uint8*)hdrp + sizeof(uint8) + sizeof(uint32)))

/*
 * Number of zero bytes that give PGLZ/LZ4 a hint that this data is well
 * compressed.
 */
#define PGLZ_HINT_SIZE 32

/* Maximum amount of words that can be stored in the dictionary */
#define DICT_MAX_WORDS (1 << 16)

/* Represents a single word in the dictionary */
typedef struct
{
	/* The code of the word */
	uint16		code;
	/* If true, the next word in dictionary starts with the same nbytes bytes */
	bool		check_next;
	/* Number of bytes (not letters) in the word except trailing \0 */
	size_t		nbytes;
	/* Pointer to the word */
	char	   *word;
} Word;

/* Represents the dictionary */
typedef struct
{
	/* Dictionary ID */
	int32		dict_id;
	/* Number of words in the dictionary */
	uint32		nwords;
	/* Word to code mapping sorted by word */
	Word		words[DICT_MAX_WORDS];
	/* Code to word index mapping */
	uint16		code_to_word[DICT_MAX_WORDS];
} Dict;

/* The list of dictionaries */
typedef struct DictListItem
{
	/* Pointer to the dictionary */
	Dict	   *pdict;
	union
	{
		/*
		 * The first list item is a dummy one. It stores the timestamp when the
		 * dictionary cache was last time cleaned up.
		 */
		time_t		last_clean_sec;
		/*
		 * All the rest list items are real. They store the timestamp when the
		 * corresponding dictinary was last time used.
		 */
		time_t		last_used_sec;
	};

	/* Pointer to the next list item */
	struct DictListItem *next;
} DictListItem;

/* Check if there was a new dictionary created every N seconds */
#define DICT_ID_CACHE_TIME_SEC 60

/* Dictionary cache is cleaned up every N seconds */
#define DICT_LIST_CLEAN_INTERVAL_SEC 60

/*
 * Discard the dictionary from the cache if it was not used for more than
 * given number of seconds.
 */
#define DICT_LIST_TTL_SEC 120

/* Reserved word code */
#define DICT_INVALID_CODE 0

/* Dictionary cache */
static DictListItem dictList = { 0 };

/* Cached value of the latest dictionary id */
static int32 cachedDictId = -1;

/* Timestamp when cachedDictId was last time updated */
static time_t cachedDictIdLastUpdatedSec = 0;

/* Saved query plans */
static SPIPlanPtr savedPlanGetDictId = NULL;
static SPIPlanPtr savedPlanLoadDict = NULL;

/* Loads dictionary with the given id from zson_dict table. */
static Dict *
dict_load(int32 dict_id)
{
	int			row;
	Datum		qvalues[] = {Int32GetDatum(dict_id)};
	Dict	   *pdict = calloc(sizeof(Dict), 1);

	if (pdict == NULL)
		return NULL;

	pdict->dict_id = dict_id;

	SPI_connect();

	if (savedPlanLoadDict == NULL)
	{
		/* Initialize savedPlanLoadDict */

		Oid			argtypes[] = {INT4OID};

		savedPlanLoadDict = SPI_prepare(
					"SELECT word_id, word FROM zson_dict WHERE dict_id = $1 "
					"ORDER BY word", 1, argtypes);

		if (savedPlanLoadDict == NULL)
			elog(ERROR, "Error preparing query");

		if (SPI_keepplan(savedPlanLoadDict))
			elog(ERROR, "Error keeping plan");
	}

	if (SPI_execute_plan(savedPlanLoadDict, qvalues, NULL,
						 true, DICT_MAX_WORDS) < 0)
	{
		elog(ERROR, "Failed to load dictionary");
	}

	for (row = 0; row < SPI_processed; row++)
	{
		bool		isnull;
		char	   *wordcopy;
		uint32		word_id = DatumGetInt32(
					SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc,
					1, &isnull)
					);
		char	   *word = DatumGetCString(DirectFunctionCall1(textout,
					SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc,
					2, &isnull)
					));

		size_t		wordlen = strlen(word);

		/*
		 * zson_learn() doesn't create dictionaries with strings shorter
		 * than 2 symbols, but user could fill zson_dict table manually.
		 */
		if (wordlen < 2)
			continue;

		wordcopy = malloc(wordlen + 1);
		if (wordcopy == NULL)
			elog(ERROR, "Failed to allocate memory");

		strcpy(wordcopy, word);
		pdict->words[pdict->nwords].code = (uint16) word_id;
		pdict->words[pdict->nwords].word = wordcopy;
		pdict->words[pdict->nwords].nbytes = wordlen;

		pdict->code_to_word[(uint16) word_id] = pdict->nwords;

		if ((pdict->nwords > 0) &&
			(pdict->words[pdict->nwords - 1].nbytes <
			 pdict->words[pdict->nwords].nbytes))
		{
			pdict->words[pdict->nwords - 1].check_next =
				(memcmp(pdict->words[pdict->nwords].word,
						pdict->words[pdict->nwords - 1].word,
						pdict->words[pdict->nwords - 1].nbytes
						) == 0);
		}

		pdict->nwords++;
	}

	SPI_finish();

	return pdict;
}

/* Frees the memory allocated for given dictionary */
static void
dict_free(Dict * pdict)
{
	uint32		i;

	for (i = 0; i < pdict->nwords; i++)
		free(pdict->words[i].word);

	free(pdict);
}

/*
 * Returns the dictionary with given id, or NULL in case of error.
 * Unlike dict_load, this procedure uses cache.
 */
static Dict *
dict_get(int32 dict_id)
{
	DictListItem *dict_list_item = &dictList;
	struct timeval tv;

	gettimeofday(&tv, NULL);

	/* Clean cache, if necessary */

	if (tv.tv_sec - dictList.last_clean_sec > DICT_LIST_CLEAN_INTERVAL_SEC)
	{
		DictListItem *prev_dict_list_item = NULL;

		while (dict_list_item)
		{
			if (dict_list_item->pdict &&
				(tv.tv_sec - dict_list_item->last_used_sec >
				 DICT_LIST_TTL_SEC))
			{
				DictListItem *temp = dict_list_item->next;

				dict_free(dict_list_item->pdict);
				free(dict_list_item);

				prev_dict_list_item->next = temp;
				dict_list_item = temp;
			}
			else
			{
				prev_dict_list_item = dict_list_item;
				dict_list_item = dict_list_item->next;
			}
		}

		dict_list_item = &dictList;
	}

	/* Find the dictionary in the cache */
	while (dict_list_item)
	{
		if (dict_list_item->pdict &&
			(dict_list_item->pdict->dict_id == dict_id))
		{
			dict_list_item->last_used_sec = tv.tv_sec;
			return dict_list_item->pdict;
		}

		dict_list_item = dict_list_item->next;
	}

	/* Dictionary was not found. Load the dictionary and add it to the cache */

	dict_list_item = calloc(sizeof(DictListItem), 1);
	if (!dict_list_item)
		return NULL;

	dict_list_item->pdict = dict_load(dict_id);
	if (dict_list_item->pdict == NULL)
	{
		free(dict_list_item);
		return NULL;
	}

	dict_list_item->last_used_sec = tv.tv_sec;
	dict_list_item->next = dictList.next;
	dictList.next = dict_list_item;

	return dict_list_item->pdict;
}

/*
 * Returns id of the latest dictionary. Returns negative value if zson_dict
 * table was not initialized.
 */
static int32
get_current_dict_id()
{
	int32		id;
	bool		isnull;
	struct timeval tv;

	gettimeofday(&tv, NULL);

	if (cachedDictId >= 0 &&
		tv.tv_sec - cachedDictIdLastUpdatedSec < DICT_ID_CACHE_TIME_SEC)
		return cachedDictId;

	SPI_connect();

	if (savedPlanGetDictId == NULL)
	{
		/* Initialize savedPlanGetDictId */

		savedPlanGetDictId = SPI_prepare(
							"SELECT max(dict_id) FROM zson_dict;", 0, NULL);

		if (savedPlanGetDictId == NULL)
			elog(ERROR, "Error preparing query");

		if (SPI_keepplan(savedPlanGetDictId))
			elog(ERROR, "Error keeping plan");
	}

	if (SPI_execute_plan(savedPlanGetDictId, NULL, NULL, true, 1) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "Failed to get current dict_id");

	id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
						SPI_tuptable->tupdesc, 1, &isnull));

	SPI_finish();

	id = isnull ? -1 : id;
	cachedDictId = id;
	cachedDictIdLastUpdatedSec = tv.tv_sec;
	return id;
}

/*
 * Looks for a match in given dictionary using binary search.
 *
 * Arguments:
 *   pdict     - pointer to the dictionary
 *   buff      - pointer to the data we are compressing
 *   buff_size - size of buff
 *
 * Returns the code of found word and stores its length in pnbytes.
 * If no match was found, DICT_INVALID_CODE is returned.
 */
static uint16
dict_find_match(const Dict * pdict,
				const uint8 *buff, size_t buff_size, size_t *pnbytes)
{
	int			res;
	int32		left = 0;
	int32		right = pdict->nwords - 1;
	size_t		best_nbytes = 0;
	uint16		best_code = DICT_INVALID_CODE;

	while (left <= right)
	{
		uint32		current = (left + right) / 2;
		size_t		nbytes = pdict->words[current].nbytes;

		if (nbytes > buff_size)
		{
			/* Current word is greater */
			res = 1;
		}
		else
		{
			res = memcmp(pdict->words[current].word, buff, nbytes);
		}

		if (res == 0)
		{
			/* Match found */
			best_nbytes = nbytes;
			best_code = pdict->words[current].code;

			if ((!pdict->words[current].check_next) || (nbytes == buff_size))
				break;

			/* Maybe there is a longer match */
			left = current + 1;
		}
		else if (res < 0)
		{
			/* Current is less */
			left = current + 1;
		}
		else
		{
			/* Current is greater */
			right = current - 1;
		}
	}

	*pnbytes = best_nbytes;
	return best_code;
}

/* Returns buffer size required to compress JSONB document of given size */
inline static Size
zson_compress_bound(Size size)
{
	return PGLZ_HINT_SIZE + (size / 2 + 1) * 3;
}

/*
 * Compresses data in ZSON format.
 *
 * Arguments:
 *   pdict         - dictionary
 *   src_data_     - source data
 *   src_size      - source data size
 *   encoded_data_ - buffer to store compressed data
 *   pencoded_size - variable to store the size of compressed data
 *
 * Returns true on success, false otherwise.
 */
static bool
zson_compress(const Dict * pdict,
			  const void *src_data_, Size src_size,
			  void *encoded_data_, Size *pencoded_size)
{
	size_t		nbytes;
	Size		inoffset;
	Size		outskipoffset = 0;
	Size		outoffset = 1;
	uint8		skipbytes = 0;
	const uint8 *src_data = src_data_;
	uint8		*encoded_data = ((uint8 *) encoded_data_) + PGLZ_HINT_SIZE;

	memset(encoded_data_, 0, PGLZ_HINT_SIZE);

	for (inoffset = 0; inoffset < src_size;)
	{
		uint16		code = dict_find_match(pdict, &(src_data[inoffset]),
										   src_size - inoffset, &nbytes);

		if (code == DICT_INVALID_CODE)
		{
			skipbytes++;
			encoded_data[outoffset] = src_data[inoffset];
			outoffset++;
			inoffset++;

			if (skipbytes == 255)
			{
				encoded_data[outskipoffset] = skipbytes;

				/* DICT_INVALID_CODE */
				encoded_data[outoffset++] = 0;
				encoded_data[outoffset++] = 0;

				outskipoffset = outoffset++;
				skipbytes = 0;
			}
		}
		else
		{
			encoded_data[outskipoffset] = skipbytes;
			encoded_data[outoffset++] = code >> 8;
			encoded_data[outoffset++] = code & 0xFF;
			outskipoffset = outoffset++;
			skipbytes = 0;
			inoffset += nbytes;
		}
	}

	encoded_data[outskipoffset] = skipbytes;
	*pencoded_size = outoffset + PGLZ_HINT_SIZE;

	return true;
}

/*
 * Decompresses data from ZSON format.
 *
 * Arguments:
 *   pdict         - dictionary
 *   encoded_data_ - data to decompress
 *   encoded_size  - size of encoded_data
 *   decoded_data_ - buffer to store decompressed data
 *   decoded_size  - expected size of decoded data
 *
 * Returns true on success, false otherwise.
 */
static bool
zson_decompress(const Dict * pdict,
					const void *encoded_data_, Size encoded_size,
					void *decoded_data_, Size decoded_size)
{
	Size		inoffset = 0;
	Size		outoffset = 0;
	uint16		code,
				idx;
	uint8		skipbytes;
	const uint8 *encoded_data = ((uint8 *) encoded_data_) + PGLZ_HINT_SIZE;
	uint8		*decoded_data = decoded_data_;

	encoded_size -= PGLZ_HINT_SIZE;

	for (inoffset = 0; inoffset < encoded_size;)
	{
		skipbytes = encoded_data[inoffset++];

		if (skipbytes > decoded_size - outoffset)
			return false;

		if (skipbytes > encoded_size - inoffset)
			return false;

		memcpy(
			   &(decoded_data[outoffset]),
			   &(encoded_data[inoffset]),
			   skipbytes
			);

		outoffset += skipbytes;
		inoffset += skipbytes;

		if (encoded_size == inoffset && decoded_size == outoffset)
		{
			/* end of input - its OK */
			break;
		}

		if (2 > encoded_size - inoffset)
			return false;

		code = (uint16) encoded_data[inoffset++];
		code = (code << 8) | (uint16) encoded_data[inoffset++];

		if (code != DICT_INVALID_CODE)
		{
			idx = pdict->code_to_word[code];

			if (pdict->words[idx].nbytes > decoded_size - outoffset)
				return false;

			memcpy(
				   &(decoded_data[outoffset]),
				   pdict->words[idx].word,
				   pdict->words[idx].nbytes
				);

			outoffset += pdict->words[idx].nbytes;
		}
	}

	return true;
}

/* Converts JSONB to ZSON */
Datum
jsonb_to_zson(PG_FUNCTION_ARGS)
{
	Jsonb	   *jsonb = PG_GETARG_JSONB_P(0);
	uint8	   *jsonb_data = (uint8 *) VARDATA(jsonb);
	Size		jsonb_data_size = VARSIZE(jsonb) - VARHDRSZ;
	uint8	   *encoded_buff,
			   *encoded_header,
			   *encoded_data;
	Size		encoded_size;
	bool		res;
	Dict	   *pdict;
	Size		encoded_buff_size = VARHDRSZ + ZSON_HEADER_SIZE +
									zson_compress_bound(jsonb_data_size);
	int32		dict_id = get_current_dict_id();

	if (dict_id < 0)
		ereport(ERROR,
				(
				 errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to compress JSONB"),
				 errdetail("zson_dict is not initialized"),
				 errhint("You probably forgot to execute zson_learn().")
				 ));

	pdict = dict_get(dict_id);

	if (pdict == NULL)
		elog(ERROR, "Unable to load dictionary");

	encoded_buff = palloc(encoded_buff_size);
	encoded_header = (uint8 *) VARDATA(encoded_buff);
	encoded_data = encoded_header + ZSON_HEADER_SIZE;

	ZSON_HEADER_VERSION(encoded_header) = ZSON_CURRENT_VERSION;
	ZSON_HEADER_DICT_VERSION(encoded_header) = dict_id;
	ZSON_HEADER_DECODED_SIZE(encoded_header) = jsonb_data_size;

	encoded_size = encoded_buff_size - VARHDRSZ - ZSON_HEADER_SIZE;

	res = zson_compress(pdict, jsonb_data, jsonb_data_size,
						encoded_data, &encoded_size);
	if (!res)
		ereport(ERROR,
				(
				 errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to compress JSONB"),
				 errdetail("Procedure zson_compress() returned %d", res),
				 errhint("You probably should report this to pgsql-bugs@")
				 ));

	encoded_size += VARHDRSZ + ZSON_HEADER_SIZE;

	/* Shrink the buffer after compression */
	encoded_buff = repalloc(encoded_buff, encoded_size);
	SET_VARSIZE(encoded_buff, encoded_size);
	PG_RETURN_BYTEA_P(encoded_buff);
}

/* Converts ZSON to JSONB */
Datum
zson_to_jsonb(PG_FUNCTION_ARGS)
{
	bytea	   *encoded_buff = PG_GETARG_BYTEA_P(0);
	uint8	   *encoded_header = (uint8 *) VARDATA(encoded_buff);
	uint8	   *encoded_data = encoded_header + ZSON_HEADER_SIZE;
	Size		encoded_size = VARSIZE(encoded_buff) - VARHDRSZ - ZSON_HEADER_SIZE;
	int			zson_version = ZSON_HEADER_VERSION(encoded_header);
	/* Can't read dict_id and decoded_size until ZSON version is checked */
	uint32		dict_id;
	uint32		decoded_size;
	Jsonb	   *jsonb;
	uint8	   *jsonb_data;
	Dict	   *pdict;
	bool		res;

	if (zson_version > ZSON_CURRENT_VERSION)
		ereport(ERROR,
				(
				 errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unsupported zson version"),
				 errdetail("Saved zson version is %d, extension version is %d",
						   zson_version, ZSON_CURRENT_VERSION),
				 errhint("You probably should upgrade zson extension "
						 "or report a bug to pgsql-bugs@")
				 ));

	/* Now it's safe to read dict_id and decoded_size */
	dict_id = ZSON_HEADER_DICT_VERSION(encoded_header);
	decoded_size = ZSON_HEADER_DECODED_SIZE(encoded_header);
	pdict = dict_get(dict_id);
	if (pdict == NULL)
		elog(ERROR, "Unable to load dictionary");

	jsonb = palloc(decoded_size + VARHDRSZ);
	jsonb_data = (uint8 *) VARDATA(jsonb);

	res = zson_decompress(pdict,
						  encoded_data, encoded_size,
						  jsonb_data, decoded_size);

	if (!res)
		ereport(ERROR,
				(
				 errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to decompress ZSON"),
				 errdetail("Procedure zson_decompress() returned %d", res),
				 errhint("You probably should report this to pgsql-bugs@")
				 ));

	decoded_size += VARHDRSZ;
	SET_VARSIZE(jsonb, decoded_size);
	PG_RETURN_JSONB_P(jsonb);
}

/* Converts cstring to ZSON */
Datum
zson_in(PG_FUNCTION_ARGS)
{
	Datum		string_datum = PG_GETARG_DATUM(0);
	Datum		jsonb_datum = DirectFunctionCall1(jsonb_in, string_datum);
	Datum		zson_datum = DirectFunctionCall1(jsonb_to_zson, jsonb_datum);
	bytea	   *zson_bytea = DatumGetByteaP(zson_datum);

	PG_RETURN_BYTEA_P(zson_bytea);
}

/* Converts ZSON to cstring */
Datum
zson_out(PG_FUNCTION_ARGS)
{
	bytea	   *zson_bytea = PG_GETARG_BYTEA_P(0);
	Datum		zson_datum = PointerGetDatum(zson_bytea);
	Datum		jsonb_datum = DirectFunctionCall1(zson_to_jsonb, zson_datum);
	Datum		string_datum = DirectFunctionCall1(jsonb_out, jsonb_datum);

	PG_RETURN_CSTRING(string_datum);
}

/*  Returns text description of the given ZSON document. */
Datum
zson_info(PG_FUNCTION_ARGS)
{
	bytea	   *zson = PG_GETARG_BYTEA_P(0);

	Size		zson_size = VARSIZE(zson);
	uint8	   *zson_header = (uint8 *) VARDATA(zson);
	uint32		zson_version = ZSON_HEADER_VERSION(zson_header);
	uint32		dict_version = ZSON_HEADER_DICT_VERSION(zson_header);
	uint32		decoded_size = ZSON_HEADER_DECODED_SIZE(zson_header) + VARHDRSZ;

	size_t		buff_size = 1024;
	char	   *string_buff = palloc(buff_size);

	snprintf(string_buff, buff_size,
			 "zson version = %u, dict version = %u, jsonb size = %u, "
			 "zson size (without pglz/lz4 compression) = %u (%.2f%%)",

			 zson_version, dict_version, decoded_size,
			 (uint32) zson_size, (float) zson_size * 100 / (float) decoded_size
		);

	PG_RETURN_CSTRING((Datum) string_buff);
}
