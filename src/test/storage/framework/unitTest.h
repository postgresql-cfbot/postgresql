//
// Created by John Morris on 10/20/22.
//

#ifndef FILTER_UNITTESTFRAMEWORK_H
#define FILTER_UNITTESTFRAMEWORK_H
#include "unitTestInternal.h"
void testMain(void);
char *progname;
extern void InitFileAccess(void);
int main(int argc, char **argv)
{
	progname = argv[0];
	MemoryContextInit();
	InitFileAccess();
    testMain();
}


#endif //FILTER_UNITTESTFRAMEWORK_H
