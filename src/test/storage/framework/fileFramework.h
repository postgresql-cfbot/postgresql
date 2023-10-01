/* */
#ifndef FILTER_FILEFRAMEWORK_H
#define FILTER_FILEFRAMEWORK_H

//#include "storage/iostack.h"


#define PG_TESTSTACK 0

/* Function type to create an IoStack with the given block size */
typedef void *(*CreateStackFn)(size_t blockSize);

void seekTest(CreateStackFn createStack, char *nameFmt);
void singleSeekTest(CreateStackFn createStack, char *nameFmt, off_t fileSize, size_t bufSize);

void streamTest(CreateStackFn createStack, char *nameFmt);
void singleStreamTest(CreateStackFn createStack, char *nameFmt, off_t fileSize, size_t bufSize);

void readSeekTest(CreateStackFn createStack, char *nameFmt);
void singleReadSeekTest(CreateStackFn createStack, char *nameFmt, off_t fileSize, size_t bufSize);

#include "unitTestInternal.h"


#endif //FILTER_FILEFRAMEWORK_H
