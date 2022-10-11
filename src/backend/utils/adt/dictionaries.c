/*-------------------------------------------------------------------------
 *
 * dictionaries.c
 *	  Conversion functions for dictionary types.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dictionaries.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_dict.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"

/*
 * When compressing a data we treat it as a BLOB, in other words we don't
 * assume anything regarding its internal representation. This is not
 * necessarily the best and/or the only possible approach. However, it is
 * universal and can be reused for JSONB, TEXT, XML and other types. Alternative
 * compression methods can be introduced in the future, so that the user will
 * be able to choose the best one for the task.
 *
 * The compressed data is stored in the following format:
 *
 * (struct varlena)
 * dictionary_id     [uint32]
 * decompressed_size [uint32]
 * algorithm_version [uint8]
 *
 * (repeated) {
 *   number of bytes to copy as-is     [uint8]
 *   ... bytes to copy as-is ...
 *   dictionary entry id, or 0 to skip [uint32]
 * }
 *
 * Compressed data is a variable-length type and thus has the 'struct varlena'
 * header. The code below doesn't consider it being a part of the payload
 * (e.g. see the DICT_COMP_HEADER_SIZE definition).
 *
 * Storing dictionary_id may seem redundant, but without it dictionary_out()
 * and dictionary_jsonb() have no way to distinguish one dictionary type from
 * another. dictionary_in() and jsonb_dictionary() get the dictionary id through
 * the 'typemod' argument.
 *
 * Currently algorithm_version is always 0. In the future it will allow us to
 * introduce new features (e.g. usage of varints) and/or lazily migrate the
 * data to other compression methods.
 *
 * Compression and decompression are implemented based on a binary search over
 * DictEntry[] array (see pg_dict.h). For compression the array is sorted by
 * dictentries and for decompression - by oid's.
 *
 */

/* Size of the header in the compressed data */
#define DICT_COMP_HEADER_SIZE (sizeof(uint32)*2 + sizeof(uint8))

/* Extracts dictionary_id from the compressed data */
#define DICT_COMP_DICTIONARY_ID(hdrp) \
	(*(uint32*)hdrp)

/* Extracts decompressed_size from the compressed data */
#define DICT_COMP_DECOMPRESSED_SIZE(hdrp) \
	(*(uint32*)((uint8*)hdrp + sizeof(uint32)))

/* Extracts algorithm_version from the compressed data */
#define DICT_COMP_ALGORITHM_VERSION(hdrp) \
	(*(uint8*)((uint8*)hdrp + sizeof(uint32)*2))

/* Current algorithm_version */
#define DICT_COMP_CURRENT_ALGORITHM_VERSION 0

/*
 * bsearch_arg() callback for finding DictEntry by oid.
 */
static int
find_by_oid_cb(const void *key, const void *current, void *arg)
{
	/* Note that oids are unsigned, so we should be careful here */
	if (*(Oid *) key < ((DictEntry *) current)->oid)
		return -1;
	else if (*(Oid *) key == ((DictEntry *) current)->oid)
		return 0;				/* found! */
	else
		return 1;
}

/*
 * qsort() callback for sorting DictEntry[] by oids.
 */
static int
sort_by_oids_cb(const void *left, const void *right)
{
	/* Note that oids are unsigned, so we should be careful here */
	if (((DictEntry *) left)->oid < ((DictEntry *) right)->oid)
		return -1;

	/* Oids are unique so this callback will never return 0 */
	return 1;
}

/*
 * Finds a DictEntry which dictentry field matches *data and returns its Oid.
 * If there are several matching entries, the largest is returned. The length
 * of the found entry is written to *found_length on success. On failure
 * InvalidOid is returned and found_length is zeroed.
 *
 * The implementation is similar to bsearch_arg(). The procedure can't be used
 * directly because we are looking not by the exact match. We could generalize
 * this case but the signature of the function becomes so complicated that it
 * doesn't seem to worth the effort.
 */
static Oid
compress_find_oid(Dictionary dict, const uint8 *data, Size data_size, Size *found_length)
{
	int			res;
	int32		left = 0;
	int32		right = dict->nentries - 1;
	Size		best_length = 0;
	Oid			best_match = InvalidOid;

	while (left <= right)
	{
		int32		current = (left + right) / 2;
		Size		nbytes = (Size)dict->entries[current].length;

		if (nbytes > data_size)
		{
			/* current can be less or greater depending on the prefix */
			res = memcmp(dict->entries[current].data, data, data_size);

			/* if prefixes match, current is greater */
			if (res == 0)
				res = 1;
		}
		else
			res = memcmp(dict->entries[current].data, data, nbytes);

		if (res == 0)			/* match found */
		{
			best_length = nbytes;
			best_match = dict->entries[current].oid;

			if (nbytes == data_size)
				break;

			/* maybe there is a larger match */
			left = current + 1;
		}
		else if (res < 0)		/* current is less */
			left = current + 1;
		else					/* current is greater */
			right = current - 1;
	}

	*found_length = best_length;
	return best_match;
}

/*
 * Finds a DictEntry by Oid using a binary search. The dictionary should be
 * sorted by oids before the call. Returns NULL if nothing was found.
 */
static DictEntry *
decompress_find_dictentry(Dictionary dict, Oid oid)
{
	return (DictEntry *) bsearch_arg(&oid, dict->entries, dict->nentries, sizeof(DictEntry), find_by_oid_cb, NULL);
}

/*
 * Estimates the worst-case compressed size of the data of given size.
 * Worst-case scenario happens when the dictionary consists of single-character
 * entries. In this case every byte will be encoded as 6 bytes:
 *     0x00, (0 bytes to copy as-is), 4 bytes of the entry Oid
 *
 * This procedure doesn't account for the header size.
 *
 * AALEKSEEV FIXME don't use dictionary entries shorter than 5 bytes
 */
static Size
worst_case_compressed_size(Size insize)
{
	return insize * 6;
}

/*
 * Compresses the data using the provided dictionary. The dictionary should
 * be sorted by dictentries before the call. Output buffer should be at least
 * worst_case_compressed_size(src_size) bytes in size.
 */
static void
compress(Dictionary dict,
		 const void *src_data_, Size src_size,
		 void *encoded_data_, Size *pencoded_size)
{
	Size		nbytes;
	Size		inoffset;
	Size		outskipoffset = 0;
	Size		outoffset = 1;
	uint8		skipbytes = 0;
	const uint8 *src_data = src_data_;
	uint8	   *encoded_data = ((uint8 *) encoded_data_);

	for (inoffset = 0; inoffset < src_size;)
	{
		Oid			code = compress_find_oid(dict, &(src_data[inoffset]),
											 src_size - inoffset, &nbytes);

		if (code == InvalidOid)
		{
			skipbytes++;
			encoded_data[outoffset] = src_data[inoffset];
			outoffset++;
			inoffset++;

			if (skipbytes == 255)
			{
				encoded_data[outskipoffset] = skipbytes;
				encoded_data[outoffset++] = 0;	/* InvalidOid */
				encoded_data[outoffset++] = 0;
				encoded_data[outoffset++] = 0;
				encoded_data[outoffset++] = 0;
				outskipoffset = outoffset++;
				skipbytes = 0;
			}
		}
		else
		{
			encoded_data[outskipoffset] = skipbytes;
			encoded_data[outoffset++] = (code >> 24) & 0xFF;
			encoded_data[outoffset++] = (code >> 16) & 0xFF;
			encoded_data[outoffset++] = (code >> 8) & 0xFF;
			encoded_data[outoffset++] = code & 0xFF;
			outskipoffset = outoffset++;
			skipbytes = 0;
			inoffset += nbytes;
		}
	}

	/* Double check that we didn't write out of buffer */
	Assert(outoffset < worst_case_compressed_size(src_size));

	encoded_data[outskipoffset] = skipbytes;
	*pencoded_size = outoffset;
}

/*
 * Report an internal error in decompress() procedure below.
 * Under normal circumstances this should never happen.
 */
static void
decompress_error()
{
	ereport(ERROR,
			(
			 errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("Unable to decompress a dictionary type"),
			 errdetail("The compressed data seems to be corrupted"),
			 errhint("Please report the steps to reproduce the issue to pgsql-bugs@")
			));
}

/*
 * Decompresses the data using the provided dictionary. The dictionary should
 * be sorted by oids before the call.
 */
static void
decompress(Dictionary dict,
		   const void *encoded_data_, Size encoded_size,
		   void *decoded_data_, Size decoded_size)
{
	Size		inoffset = 0;
	Size		outoffset = 0;
	Oid			code;
	uint8		skipbytes;
	const uint8 *encoded_data = ((uint8 *) encoded_data_);
	uint8	   *decoded_data = decoded_data_;

	for (inoffset = 0; inoffset < encoded_size;)
	{
		skipbytes = encoded_data[inoffset++];

		if (skipbytes > decoded_size - outoffset)
			decompress_error();

		if (skipbytes > encoded_size - inoffset)
			decompress_error();

		memcpy(
			   &(decoded_data[outoffset]),
			   &(encoded_data[inoffset]),
			   skipbytes
			);

		outoffset += skipbytes;
		inoffset += skipbytes;

		if ((encoded_size == inoffset) && (decoded_size == outoffset))
			break;				/* end of input - its OK */

		if (encoded_size - inoffset < 4)
			decompress_error();

		code = (Oid) encoded_data[inoffset++];
		code = (code << 8) | (Oid) encoded_data[inoffset++];
		code = (code << 8) | (Oid) encoded_data[inoffset++];
		code = (code << 8) | (Oid) encoded_data[inoffset++];

		if (code != InvalidOid)
		{
			Size		entrylen;
			DictEntry  *entry = decompress_find_dictentry(dict, code);

			if (entry == NULL)
				decompress_error();

			Assert(entry->oid == code);
			entrylen = (Size)entry->length;

			if (entrylen > decoded_size - outoffset)
				decompress_error();

			memcpy(
				   &(decoded_data[outoffset]),
				   entry->data,
				   entrylen
				);

			outoffset += entrylen;
		}
	}

	Assert(decoded_size == outoffset);
}

/*
 * Converts a cstring to a dictionary.
 */
Datum
dictionary_in(PG_FUNCTION_ARGS)
{
	const char *instr = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Jsonb	   *jsonb;
	bytea	   *dict;

	Assert(typmod != -1);

	jsonb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(instr)));
	dict = DatumGetByteaP(DirectFunctionCall2(jsonb_dictionary, JsonbPGetDatum(jsonb), Int32GetDatum(typmod)));

	PG_RETURN_BYTEA_P(dict);
}

/*
 * Converts a dictionary to a cstring.
 */
Datum
dictionary_out(PG_FUNCTION_ARGS)
{
	bytea	   *dict = PG_GETARG_BYTEA_P(0);
	Jsonb	   *jsonb = DatumGetJsonbP(DirectFunctionCall1(dictionary_jsonb, PointerGetDatum(dict)));
	const char *outstr = DatumGetCString(DirectFunctionCall1(jsonb_out, JsonbPGetDatum(jsonb)));

	PG_RETURN_CSTRING(outstr);
}

/*
 * Coverts JSONB to a dictionary type.
 *
 * AALEKSEEV FIXME: if the compressed document ends up being larger than
 * the original one write a corresponding flag and copy all the data as-is.
 */
Datum
jsonb_dictionary(PG_FUNCTION_ARGS)
{
	Dictionary	dict;
	Jsonb	   *jsonb = PG_GETARG_JSONB_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	uint8	   *jsonb_data = (uint8 *) VARDATA(jsonb);
	Size		jsonb_data_size = VARSIZE(jsonb) - VARHDRSZ;
	uint8	   *encoded_buff,
			   *encoded_header,
			   *encoded_data;
	Size		encoded_size,
				encoded_buff_size;

	Assert(typmod != -1);

	dict = DictEntriesRead(typmod);
	qsort((void *) dict->entries, dict->nentries, sizeof(DictEntry), sort_by_oids_cb);

	encoded_buff_size = VARHDRSZ + DICT_COMP_HEADER_SIZE + worst_case_compressed_size(jsonb_data_size);
	encoded_buff = palloc(encoded_buff_size);
	encoded_header = (uint8 *) VARDATA(encoded_buff);
	encoded_data = encoded_header + DICT_COMP_HEADER_SIZE;

	DICT_COMP_DICTIONARY_ID(encoded_header) = typmod;
	DICT_COMP_DECOMPRESSED_SIZE(encoded_header) = jsonb_data_size;
	DICT_COMP_ALGORITHM_VERSION(encoded_header) = DICT_COMP_CURRENT_ALGORITHM_VERSION;

	encoded_size = encoded_buff_size - VARHDRSZ - DICT_COMP_HEADER_SIZE;

	compress(dict, jsonb_data, jsonb_data_size,
			 encoded_data, &encoded_size);

	encoded_size += VARHDRSZ + DICT_COMP_HEADER_SIZE;

	encoded_buff = repalloc(encoded_buff, encoded_size);
	SET_VARSIZE(encoded_buff, encoded_size);

	DictEntriesFree(dict);

	PG_RETURN_BYTEA_P(encoded_buff);
}

/*
 * Converts a dictionary type to JSONB.
 */
Datum
dictionary_jsonb(PG_FUNCTION_ARGS)
{
	bytea	   *encoded_buff = PG_GETARG_BYTEA_P(0);
	uint8	   *encoded_header = (uint8 *) VARDATA(encoded_buff);
	uint8	   *encoded_data = encoded_header + DICT_COMP_HEADER_SIZE;
	Size		encoded_size = VARSIZE(encoded_buff) - VARHDRSZ - DICT_COMP_HEADER_SIZE;
	int			alg_version = DICT_COMP_ALGORITHM_VERSION(encoded_header);
	Oid			dictOid;		/* cannot read until algorithm version is
								 * checked */
	uint32		decoded_size;	/* cannot read until algorithm version is
								 * checked */
	Jsonb	   *jsonb;
	uint8	   *jsonb_data;
	Dictionary	dict;

	if (alg_version > DICT_COMP_CURRENT_ALGORITHM_VERSION)
		ereport(ERROR,
				(
				 errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unsupported compression algorithm version"),
				 errdetail("Saved algorithm version is %d, current version is %d",
						   alg_version, DICT_COMP_CURRENT_ALGORITHM_VERSION),
				 errhint("The data is either corrupted or imported from "
						 "the future version of PostgreSQL")
				 ));

	dictOid = DICT_COMP_DICTIONARY_ID(encoded_header);
	decoded_size = DICT_COMP_DECOMPRESSED_SIZE(encoded_header);

	dict = DictEntriesRead(dictOid);

	jsonb = palloc(decoded_size + VARHDRSZ);
	jsonb_data = (uint8 *) VARDATA(jsonb);

	decompress(dict, encoded_data, encoded_size, jsonb_data, decoded_size);

	decoded_size += VARHDRSZ;
	SET_VARSIZE(jsonb, decoded_size);

	DictEntriesFree(dict);

	PG_RETURN_JSONB_P(jsonb);
}

/*
 * Converts a dictionary type to bytea.
 */
Datum
dictionary_bytea(PG_FUNCTION_ARGS)
{
	bytea *compressed_data = PG_GETARG_BYTEA_P(0);
	PG_RETURN_BYTEA_P(compressed_data);
}