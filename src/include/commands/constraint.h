#ifndef CONSTRAINT_H
#define CONSTRAINT_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress CreateAssertion(CreateAssertionStmt *stmt);
extern ObjectAddress RenameAssertion(RenameStmt *stmt);

#endif