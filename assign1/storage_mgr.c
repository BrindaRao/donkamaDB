#include <stdio.h>
#include <stdlib.h>

#include "storage_mgr.h"

void initStorageManager(void) {
    printf("%s\n", "Initialized...");
}

RC createPageFile(char *fileName) {
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    return RC_OK;
}
