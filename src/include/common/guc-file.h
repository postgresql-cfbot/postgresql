#ifndef GUC_FILE_H
#define GUC_FILE_H

#include "c.h"

/*
 * Parsing the configuration file(s) will return a list of name-value pairs
 * with source location info.  We also abuse this data structure to carry
 * error reports about the config files.  An entry reporting an error will
 * have errmsg != NULL, and might have NULLs for name, value, and/or filename.
 *
 * If "ignore" is true, don't attempt to apply the item (it might be an error
 * report, or an item we determined to be duplicate).  "applied" is set true
 * if we successfully applied, or could have applied, the setting.
 */
typedef struct ConfigVariable
{
	char	   *name;
	char	   *value;
	char	   *errmsg;
	char	   *filename;
	int			sourceline;
	bool		ignore;
	bool		applied;
	struct ConfigVariable *next;
} ConfigVariable;

extern bool ParseConfigFile(const char *config_file, bool strict,
				const char *calling_file, int calling_lineno,
				int depth, int elevel,
				ConfigVariable **head_p, ConfigVariable **tail_p);
extern bool ParseConfigFp(FILE *fp, const char *config_file,
			  int depth, int elevel,
			  ConfigVariable **head_p, ConfigVariable **tail_p);
extern bool ParseConfigDirectory(const char *includedir,
					 const char *calling_file, int calling_lineno,
					 int depth, int elevel,
					 ConfigVariable **head_p,
					 ConfigVariable **tail_p);
extern void FreeConfigVariables(ConfigVariable *list);

extern int	guc_name_compare(const char *namea, const char *nameb);

extern void record_config_file_error(const char *errmsg,
						 const char *config_file,
						 int lineno,
						 ConfigVariable **head_p,
						 ConfigVariable **tail_p);

#endif							/* GUC_FILE_H */
