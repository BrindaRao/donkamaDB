#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

char *testName;

#define TESTPF "extra_test.bin"
SM_FileHandle fh;

static void testReadAndWrite(void);

int main(void) {
    testName = "";
    initStorageManager();
    testReadAndWrite();
    return 0;
}

void testReadAndWrite(void) {
    testName = "test Read/Write Block";
    SM_PageHandle memPage = (SM_PageHandle)malloc(PAGE_SIZE);
    memset(memPage, 0, PAGE_SIZE);
    int i, flag;

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    // test write
    ASSERT_TRUE((ensureCapacity(1, &fh) == RC_OK), "new file, pageNum = 1");
    ASSERT_TRUE((appendEmptyBlock(&fh) == RC_OK), "append, pageNum = 2");
    ASSERT_TRUE((ensureCapacity(2, &fh) == RC_OK), "valid pageNum == 3");
    ASSERT_TRUE((appendEmptyBlock(&fh) == RC_OK), "new file, pageNum = 3");
    ASSERT_TRUE((ensureCapacity(3, &fh) == RC_OK), "valid pageNum == 3");
    printf("pass ensureCapacity\n");
    printf("pass appendEmptyBlock\n");


    ASSERT_TRUE((writeCurrentBlock(&fh, memPage) == RC_OK), "writeCurrentBlock, pageNum = 3");
    ASSERT_TRUE((ensureCapacity(3, &fh) == RC_OK), "valid pageNum == 3");
    printf("pass writeCurrentBlock\n");

    ASSERT_TRUE((writeBlock(fh.totalNumPages - 1, &fh, memPage) == RC_OK), "writeLastBlock, pageNum = 3");
    ASSERT_TRUE((ensureCapacity(3, &fh) == RC_OK), "valid pageNum == 3");
    ASSERT_TRUE((writeBlock(fh.totalNumPages + 1, &fh, memPage) != RC_OK), "write error page, pageNum = 3");
    ASSERT_TRUE((ensureCapacity(3, &fh) == RC_OK), "valid pageNum == 3");
    memPage[100] = 's'; // on page 2, position 100 = 's'
    ASSERT_TRUE((writeBlock(2, &fh, memPage) == RC_OK), "write 2, pageNum = 3");
    ASSERT_TRUE((getBlockPos(&fh) == 2), "valid curPagePos == 2");
    ASSERT_TRUE((ensureCapacity(3, &fh) == RC_OK), "valid pageNum == 3");
    printf("pass writeBlock\n");
    printf("pass getBlockPos\n");

    // test read
    ASSERT_TRUE((readFirstBlock(&fh, memPage) == RC_OK), "read first");
    ASSERT_TRUE((getBlockPos(&fh) == 0), "valid curPagePos == 0");
    flag = 0;
    for (i = 0; i < PAGE_SIZE; i++) {
        if (memPage[i] != 0) {
            flag = 1;
            break;
        }
    }
    ASSERT_TRUE((flag == 0), "valid block 0");
    printf("pass readFirstBlock\n");

    ASSERT_TRUE((readNextBlock(&fh, memPage) == RC_OK), "read next block");
    ASSERT_TRUE((getBlockPos(&fh) == 1), "valid curPagePos == 1");
    flag = 0;
    for (i = 0; i < PAGE_SIZE; i++) {
        if (memPage[i] != 0) {
            flag = 1;
            break;
        }
    }
    ASSERT_TRUE((flag == 0), "valid block 1");
    printf("pass readNextBlock\n");

    ASSERT_TRUE((readBlock(2, &fh, memPage) == RC_OK), "read block 2");
    ASSERT_TRUE((getBlockPos(&fh) == 2), "valid curPagePos == 2");
    flag = 0;
    for (i = 0; i < PAGE_SIZE; i++) {
        if (i != 100 && memPage[i] != 0) {
            flag = 1;
            break;
        }
    }
    if (memPage[100] != 's') {
        flag = 1;
    }
    ASSERT_TRUE((flag == 0), "valid block 2");
    printf("pass readBlock\n");

    ASSERT_TRUE((readLastBlock(&fh, memPage) == RC_OK), "read last block");
    ASSERT_TRUE((getBlockPos(&fh) == 2), "valid curPagePos == 2");
    flag = 0;
    for (i = 0; i < PAGE_SIZE; i++) {
        if (i != 100 && memPage[i] != 0) {
            flag = 1;
            break;
        }
    }
    if (memPage[100] != 's') {
        flag = 1;
    }
    ASSERT_TRUE((flag == 0), "valid last");
    printf("pass readLastBlock\n");

    ASSERT_TRUE((readNextBlock(&fh, memPage) != RC_OK), "read non-exist page");
    ASSERT_TRUE((getBlockPos(&fh) == 2), "valid curPagePos == 2");

    ASSERT_TRUE((readPreviousBlock(&fh, memPage) == RC_OK), "read previous block");
    ASSERT_TRUE((getBlockPos(&fh) == 1), "valid curPagePos == 1");
    flag = 0;
    for (i = 0; i < PAGE_SIZE; i++) {
        if (memPage[i] != 0) {
            flag = 1;
            break;
        }
    }
    ASSERT_TRUE((flag == 0), "valid block 1");
    printf("pass readPreviousBlock\n");

    ASSERT_TRUE((readCurrentBlock(&fh, memPage) == RC_OK), "read current block");
    ASSERT_TRUE((getBlockPos(&fh) == 1), "valid curPagePos == 1");
    flag = 0;
    for (i = 0; i < PAGE_SIZE; i++) {
        if (memPage[i] != 0) {
            flag = 1;
            break;
        }
    }
    ASSERT_TRUE((flag == 0), "valid block 1");
    printf("pass readCurrentBlock\n");

    TEST_CHECK(closePageFile (&fh));
    TEST_CHECK(destroyPageFile (TESTPF));
    TEST_DONE();
}
