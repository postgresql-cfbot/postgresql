/*
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/fcntl.h>
#include "./framework/fileFramework.h"
#include "./framework/unitTest.h"


#define createStack NULL

void testMain()
{
    system("rm -rf " TEST_DIR "vfd; mkdir -p " TEST_DIR "vfd");



	beginTest("Storage");
    beginTestGroup("Vfd Stack");
	singleSeekTest(createStack, TEST_DIR "vfd/testfile_%u_%u.dat", 1024, 4096);
    seekTest(createStack, TEST_DIR "vfd/testfile_%u_%u.dat");
}
