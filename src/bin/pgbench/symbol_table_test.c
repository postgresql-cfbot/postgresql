/* standalone compilation for testing
 * cc -O -o symbol_table_test symbol_table_test.c
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define pg_strdup strdup
typedef enum { false, true } bool;
struct SymbolTable_st;
typedef struct SymbolTable_st *SymbolTable;
SymbolTable newSymbolTable(void);
void freeSymbolTable(SymbolTable st);
void dumpSymbolTable(FILE *out, const char *msg, SymbolTable st);
int numberOfSymbols(SymbolTable st);
int getOrCreateSymbolId(SymbolTable st, const char *name);
int getSymbolId(SymbolTable st, const char *name);
char *getSymbolName(SymbolTable st, int number);
int rmSymbolId(SymbolTable st, const char *name);

#define STANDALONE
#include "symbol_table.c"

#define SYMBOLS 30

int
main(void)
{
	char *names[SYMBOLS] = {
		"calvin", "hobbes", "calvin", "susie", "moe", "rosalyn",
		"v", "va", "var", "var1", "var2", "var3",
		"var11", "var1", "var11", "hobbes", "moe", "v", "",
		"é", "été", "ét", "étage", "étirer", "étape",
		"你好!", "你好", "你",
		"µài©çéèæĳœâå€çþýû¶ÂøÊ±æðÛÎÔ¹«»©®®ß¬", "hello world"
	};

	SymbolTable st = newSymbolTable();

	for (int i = 0; i < SYMBOLS; i++)
	{
		fprintf(stdout, "# %s (%ld)\n", names[i], strlen(names[i]));
		int prev = getSymbolId(st, names[i]);
		int add = getOrCreateSymbolId(st, names[i]);
		int post = getSymbolId(st, names[i]);
		int rm = rmSymbolId(st, "moe");
		fprintf(stdout, "%d %d %d %d\n", prev, add, post, rm);
	}

	dumpSymbolTable(stdout, "first", st);

	for (int i = SYMBOLS - 1; i >= 0; i--)
		fprintf(stdout, "rm \"%s\": %d\n", names[i], rmSymbolId(st, names[i]));

	dumpSymbolTable(stdout, "cleaned", st);

	for (int i = SYMBOLS - 1; i >= 0; i--)
		fprintf(stdout, "add \"%s\": %d\n", names[i], getOrCreateSymbolId(st, names[i]));

	dumpSymbolTable(stdout, "end", st);

	freeSymbolTable(st);
	return 0;
}
