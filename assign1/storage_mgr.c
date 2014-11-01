#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>

#include "storage_mgr.h"

/*
    Private
*/

#define RC_ERROR -1
#define RC_VALID 31415926

// a valid FileHandle, its mgmtInfo should be RC_VALID
int isValid(SM_FileHandle *fHandle) {
    if ((int)fHandle->mgmtInfo == RC_VALID) {
        return 1;
    }
    return 0;
}

int getFilePageNumber(FILE *fp) {
    struct stat buf;
    fstat(fileno(fp), &buf);
    return buf.st_size / PAGE_SIZE;
}

int getLastPageNumber(char *fileName) {
    FILE *fp = fopen(fileName, "r");
    int result = getFilePageNumber(fp);
    fclose(fp);
    return result;
}

/*
    Manipulating Page Files
*/

void initStorageManager(void) {
    printf("%s\n", "Initialized...");
}

RC createPageFile(char *fileName) {
    FILE *fp;
    SM_FileHandle fh;
    RC r;

    // if file exists
    fp = fopen(fileName, "r");
    if (fp != NULL) {
        fclose(fp);
        return RC_ERROR;
    }
    // else create file
    fp = fopen(fileName, "w");
    SM_PageHandle memPage = (SM_PageHandle)malloc(PAGE_SIZE);
    memset(memPage, 0, PAGE_SIZE);
    if (fseek(fp, 0, SEEK_SET) != 0) {
        return RC_WRITE_FAILED;
    }
    if (fwrite(memPage, 1, PAGE_SIZE, fp) != PAGE_SIZE) {
        return RC_WRITE_FAILED;
    }
    fclose(fp);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    if (isValid(fHandle)) {
        return RC_OK;
    }

    FILE *fp;

    fp = fopen(fileName, "r");
    if (fp == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    fHandle->fileName = fileName;
    fHandle->totalNumPages = getFilePageNumber(fp);
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = (void *)RC_VALID;
    fclose(fp);
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    fHandle->mgmtInfo = NULL;
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    FILE *fp;

    // if file not exists
    fp = fopen(fileName, "r");
    if (fp == NULL) {
        return RC_ERROR;
    }
    fclose(fp);
    // else
    remove(fileName);
    return RC_OK;
}

/*
    Reading Files
*/

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    FILE *fp;

    if (!isValid(fHandle)) {
        return RC_ERROR;
    }

    fp = fopen(fHandle->fileName, "r");
    if (pageNum >= fHandle->totalNumPages || pageNum < 0) {
        fclose(fp);
        return RC_READ_NON_EXISTING_PAGE;
    }
    fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
    fread(memPage, 1, PAGE_SIZE, fp);
    fHandle->curPagePos = pageNum;
    fclose(fp);
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

/*
    Writing Files
*/

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    FILE *fp;

    if (!isValid(fHandle)) {
        return RC_ERROR;
    }

    fp = fopen(fHandle->fileName, "r+");
    if (pageNum > fHandle->totalNumPages) {
        fclose(fp);
        return RC_READ_NON_EXISTING_PAGE;
    }
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
        fclose(fp);
        return RC_WRITE_FAILED;
    }
    if (fwrite(memPage, 1, PAGE_SIZE, fp) != PAGE_SIZE) {
        fclose(fp);
        return RC_WRITE_FAILED;
    }
    if (fHandle->totalNumPages == pageNum) {
        fHandle->totalNumPages++;
    }
    fHandle->curPagePos = pageNum;
    fclose(fp);
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    SM_PageHandle memPage = (SM_PageHandle)malloc(PAGE_SIZE);
    memset(memPage, 0, PAGE_SIZE);
    return writeBlock(fHandle->totalNumPages, fHandle, memPage);
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    FILE *fp;

    if (!isValid(fHandle)) {
        return RC_ERROR;
    }

    fp = fopen(fHandle->fileName, "r");
    if (fp == NULL) {
        fclose(fp);
        printf("file not found\n");
        return RC_FILE_NOT_FOUND;
    }
    if (fHandle->totalNumPages != getFilePageNumber(fp)) {
        fclose(fp);
        printf("totalNumPages not real\n");
        return RC_ERROR;
    }
    if (fHandle->totalNumPages != numberOfPages) {
        fclose(fp);
        printf("totalNumPages not equal\n");
        return RC_ERROR;
    }
    fclose(fp);
    return RC_OK;
}
