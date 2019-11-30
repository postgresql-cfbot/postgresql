/*
 * src/bin/pgbench/symbol_table.c
 *
 * Copyright (c) 2000-2019, PostgreSQL Global Development Group
 * ALL RIGHTS RESERVED;
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
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifndef STANDALONE
#include "postgres_fe.h"
#include "pgbench.h"
#endif

#ifdef WIN32
#include <windows.h>
#define pg_mutex_t HANDLE
#define pg_mutex_init(m) *(m) = CreateMutex(NULL, FALSE, NULL)
#define pg_mutex_lock(m) WaitForSingleObject(m, INFINITE)
#define pg_mutex_unlock(m) ReleaseMutex(m)
#define pg_mutex_destroy(m) CloseHandle(m)
#elif defined(ENABLE_THREAD_SAFETY)
#include <pthread.h>
#define pg_mutex_t pthread_mutex_t
#define pg_mutex_init(m) pthread_mutex_init(&(m), NULL)
#define pg_mutex_lock(m) pthread_mutex_lock(&(m))
#define pg_mutex_unlock(m) pthread_mutex_unlock(&(m))
#define pg_mutex_destroy(m) pthread_mutex_destroy(&(m))
#else
#define pthread_mutex_t void *
#define pthread_mutex_init(m, p) 0
#define pthread_mutex_lock(m)
#define pthread_mutex_unlock(m)
#define pthread_mutex_destroy(m)
#endif

/*----
 *
 * thread-safe simplistic symbol table management.
 *
 * Implement a string -> int table based on a tree on bytes.
 * The data structure makes more sense if many searches are expected.
 *
 * Each '\0'-terminated string is store in the tree at the first non-unique
 * byte prefix, possibly including the final '\0' if the string is a prefix of
 * another name.
 *
 * Storage requirement n * strlen(s).
 *
 * Storing a symbol costs strlen(s) on average.
 *
 * Finding a symbol costs strlen(s).
 *
 * When symbols are removed, their slot is reused.
 */

typedef struct Symbol
{
	char   *name;
	int		number;
} Symbol;

typedef struct SymbolTableNode
{
	enum { is_free, is_symb, is_tree }	status;
	union {
		int						symbol;	/* is_free or is_symb */
		struct SymbolTableNode *tree;	/* is_tree */
	} u;
} SymbolTableNode;

#define INITIAL_SYMBOL_SIZE 1

struct SymbolTable_st
{
	int					nsymbols;		/* next symbol number */
	int					stored;			/* current #symbols in table */
	int					next_avail;		/* last removed symbol for reuse */
	SymbolTableNode	    root;			/* symbol tree root */
	int					symbols_size;	/* size of following array */
	Symbol			   *symbols;		/* array of existing symbols */
	pg_mutex_t			lock;
};

/* allocate a branching node of empty leaves */
static SymbolTableNode *
newSymbolTableNode(void)
{
	SymbolTableNode * tree = malloc(sizeof(SymbolTableNode) * 256);

	for (int i = 0; i < 256; i++)
	{
		tree[i].status = is_free;
		tree[i].u.symbol = -1;
	}

	return tree;
}

/* allocate a symbol table */
SymbolTable
newSymbolTable(void)
{
	SymbolTable st = malloc(sizeof(struct SymbolTable_st));

	st->nsymbols = 0;
	st->stored = 0;
	st->next_avail = -1; /* means none */
	st->root.status = is_free;
	st->root.u.symbol = -1;
	st->symbols_size = INITIAL_SYMBOL_SIZE;
	st->symbols = malloc(sizeof(Symbol) * st->symbols_size);
	if (pg_mutex_init(st->lock) != 0)
		abort();

	return st;
}

/* recursively free a symbol table tree */
static void
freeSymbolTableNode(SymbolTableNode * tree)
{
	if (tree->status == is_tree)
	{
		for (int i = 0; i < 256; i++)
			freeSymbolTableNode(& tree->u.tree[i]);
		free(tree->u.tree);
	}
}

/* free symbol table st */
void
freeSymbolTable(SymbolTable st)
{
	freeSymbolTableNode(& st->root);
	free(st->symbols);
	pg_mutex_destroy(st->lock);
	free(st);
}

/* how many symbols have been seen */
int
numberOfSymbols(SymbolTable st)
{
	return st->nsymbols;
}

/* dump tree to out, in byte order */
static void
dumpSymbolTableNode(FILE *out, SymbolTable st, SymbolTableNode *node)
{
	if (node == NULL)
		fprintf(out, "(null)");
	else
		switch (node->status)
		{
			case is_tree:
				for (int i = 0; i < 256; i++)
					dumpSymbolTableNode(out, st, & node->u.tree[i]);
				break;
			case is_symb:
			{
				int n = node->u.symbol;
				/* we could check n == number */
				fprintf(out, "%s -> %d\n", st->symbols[n].name, st->symbols[n].number);
				break;
			}
			case is_free:
				if (node->u.symbol != -1)
					fprintf(out, "free: %d\n", node->u.symbol);
				break;
			default:
				/* cannot happen */
				abort();
		}
}

void
dumpSymbolTable(FILE * out, const char *msg, SymbolTable st)
{
	fprintf(out, "SymbolTable %s dump: %d symbols (%d seen, %d avail)\n",
			msg, st->stored, st->nsymbols, st->next_avail);
	dumpSymbolTableNode(out, st, & st->root);
	for (int i = 0; i < st->nsymbols; i++)
		if (st->symbols[i].name != NULL)
			fprintf(out, "[%d]: \"%s\"\n", i, st->symbols[i].name);
		else
			fprintf(out, "[%d]: *\n", i);
}

/* add new symbol to a node. NOT thread-safe. */
static int
addSymbol(SymbolTable st, SymbolTableNode *node, const char *name)
{
	int		number;

	if (st->next_avail != -1)
	{
		/* reuse freed number */
		number = st->next_avail;
		st->next_avail = st->symbols[number].number;
		st->symbols[number].number = -1;
	}
	else
		/* allocate a new number */
		number = st->nsymbols++;

	if (number >= st->symbols_size)
	{
		st->symbols_size *= 2;
		st->symbols = realloc(st->symbols, sizeof(Symbol) * st->symbols_size);
	}

	st->symbols[number].name = pg_strdup(name);
	/* not as silly as it looks: .number is -1 if symbol is removed */
	st->symbols[number].number = number;

	node->status = is_symb;
	node->u.symbol = number;

	st->stored++;
	return number;
}

/* get existing id or create a new one for name in st */
int
getOrCreateSymbolId(SymbolTable st, const char *name)
{
	SymbolTableNode	*node = & st->root;
	Symbol			*symb;
	int i = 0;

	/* get down to a leaf */
	while (node->status == is_tree)
		node = & node->u.tree[(unsigned char) name[i++]];

	if (node->status == is_free)
	{
		int		number;

		/* empty leaf, let us use it */
		pg_mutex_lock(st->lock);
		number = addSymbol(st, node, name);
		pg_mutex_unlock(st->lock);

		return number;
	}
	/* else we have an existing symbol, which is name or a prefix of name */

	symb = & st->symbols[node->u.symbol];

	if ((i > 0 && strcmp(symb->name + i - 1, name + i - 1) == 0) ||
			 strcmp(symb->name, name) == 0)
		/* already occupied by same name */
		return symb->number;
	else /* it contains a prefix of name */
	{
		int			prev,
					number;

		/* should lock be acquire before? or redo the descent? */
		pg_mutex_lock(st->lock);
		prev = node->u.symbol;

		/* expand tree from current node */
		do
		{
			SymbolTableNode		*prev_node;

			node->status = is_tree;
			node->u.tree = newSymbolTableNode();
			prev_node = & node->u.tree[(unsigned char) st->symbols[prev].name[i]];
			prev_node->status = is_symb;
			prev_node->u.symbol = prev;
			node = & node->u.tree[(unsigned char) name[i++]];
		} while (node->status != is_free);

		number = addSymbol(st, node, name);
		pg_mutex_unlock(st->lock);

		return number;
	}
}

/* return symbol number that exists or -1 */
int
getSymbolId(SymbolTable st, const char * name)
{
	SymbolTableNode	   *node = & st->root;
	int					i = 0;
	int					n;

	while (node->status == is_tree)
		node = & node->u.tree[(unsigned char) name[i++]];

	n = node->u.symbol;

	if (node->status == is_symb &&
		((i>0 && strcmp(st->symbols[n].name + i - 1, name + i - 1) == 0) ||
		  strcmp(st->symbols[n].name, name) == 0))
		return st->symbols[n].number; /* must be n */
	else
		return -1;
}

/* return symbol name if exists, or NULL */
char *
getSymbolName(SymbolTable st, int number)
{
	return 0 <= number && number < st->nsymbols ? st->symbols[number].name : NULL;
}

/* remove name from st, return its number or -1 if not found */
int
rmSymbolId(SymbolTable st, const char * name)
{
	SymbolTableNode	   *node = & st->root;
	int					i = 0,
						num;

	pg_mutex_lock(st->lock);

	while (node->status == is_tree)
		node = & node->u.tree[(unsigned char) name[i++]];

	num = node->u.symbol;

	if (node->status == is_symb && strcmp(st->symbols[num].name, name) == 0)
	{
		free(st->symbols[num].name);
		st->symbols[num].name = NULL;
		st->symbols[num].number = st->next_avail;
		st->next_avail = num;
		node->status = is_free;
		node->u.symbol = -1;
		st->stored--;
	}
	else
		/* symbol is not there */
		num = -1;

	pg_mutex_unlock(st->lock);

	return num;
}
