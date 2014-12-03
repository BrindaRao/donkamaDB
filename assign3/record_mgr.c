#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define BF_MODE RS_FIFO
#define BF_PAGE 3

typedef struct mgmt {
    int records;
    BM_BufferPool *bm;
    int lengthofrecord;
    int slotperpage;
    int last;
} mgmt;

void print_value(char *name, Value *value) {
    printf("Value %s, Type : %d : ", name, value->dt);
    switch (value->dt) {
        case DT_INT:
            printf("%d", value->v);
            break;
        case DT_FLOAT:
            printf("%f", value->v);
            break;
        case DT_BOOL:
            printf("%d", value->v);
            break;
        case DT_STRING:
            printf("%d : %s", value->v, value->v);
            break;
    }
    printf("\n");
}

void print_record(Record *record, Schema *schema) {
    int i;
    Value *v = (Value *)record->data;
    printf("Record total : %d, data : %d\n", schema->numAttr, record->data);
    for (i = 0; i < schema->numAttr; i++) {
        print_value(schema->attrNames[i], &v[i]);
    }
}

int add_int(char *str, int len, int n) {
    memcpy(str + len, &n, sizeof(int));
    return len + sizeof(int);
}

int get_int(char *str, int len, int *n) {
    memcpy(n, str + len, sizeof(int));
    return len + sizeof(int);
}

int add_string(char *str, int len, char *value) {
    int strl = strlen(value);
    memcpy(str + len, &strl, sizeof(int));
    memcpy(str + len + sizeof(int), value, strlen(value) + 1);
    return len + strl + sizeof(int) + 1;
}

int get_string(char *str, int len, char *value) {
    int strl = 0;
    memcpy(&strl, str + len, sizeof(int));
    memcpy(value, str + len + sizeof(int), strl + 1);
    return len + strl + sizeof(int) + 1;
}

int add_schema(char *str, int len, Schema *schema) {
    int i, pos = len;
    pos = add_int(str, pos, schema->numAttr);
    pos = add_int(str, pos, schema->keySize);
    for (i = 0; i < schema->keySize; i++) {
        pos = add_int(str, pos, schema->keyAttrs[i]);
    }
    for (i = 0; i < schema->numAttr; i++) {
        pos = add_int(str, pos, (int)schema->dataTypes[i]);
    }
    for (i = 0; i < schema->numAttr; i++) {
        pos = add_int(str, pos, schema->typeLength[i]);
    }
    for (i = 0; i < schema->numAttr; i++) {
        pos = add_string(str, pos, schema->attrNames[i]);
    }
    return pos;
}

int get_schema(char *str, int len, Schema **schema) {
    int i, total = 0, key = 0, pos = len;

    pos = get_int(str, pos, &total);
    pos = get_int(str, pos, &key);

    // printf("get schema : total %d\n", total);

    int *keys = (int *) malloc(sizeof(int) * key);
    DataType *dt = (DataType *) malloc(sizeof(DataType) * total);
    int *tl = (int *) malloc(sizeof(int) * total);
    char **names = (char **) malloc(sizeof(char*) * total);
    int d;

    for (i = 0; i < key; i++) {
        pos = get_int(str, pos, keys + i);
    }
    for (i = 0; i < total; i++) {
        pos = get_int(str, pos, &d);
        dt[i] = (DataType) d;
        // printf("get datatype : %d\n", dt[i]);
    }
    for (i = 0; i < total; i++) {
        pos = get_int(str, pos, tl + i);
    }
    for (i = 0; i < total; i++) {
        names[i] = (char *) malloc(sizeof(char) * tl[i]);
        pos = get_string(str, pos, names[i]);
        // printf("get string : %s\n", names[i]);
    }

    *schema = createSchema(total, names, dt, tl, key, keys);
    // printf("get schema : loc %x\n", *schema);
    return pos;
}

int slot_per_page(int size) {
    int r = PAGE_SIZE / size;
    int v = PAGE_SIZE / size;
    while (r * (size + 1) >= PAGE_SIZE) {
        r = r - 1;
    }
    return r;
}

RC initRecordManager(void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}

/*
    **Information in Block 0
    - Total Records
    - Schema

    **Information in Other Block (1+)
    - Number of Records
    - [P * char] for Available slot
    - [P * L * char] for every Records

    Space per record (L)
    Slot per page (P)
*/

RC createTable(char *name, Schema *schema) {
    SM_FileHandle fh;
    SM_PageHandle ph;
    createPageFile(name);
    openPageFile(name, &fh);
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
    memset(ph, 0, sizeof(char) * PAGE_SIZE);

    // write information to pagehandle
    // ...
    int pos = 0;
    pos = add_int(ph, pos, 0);
    pos = add_schema(ph, pos, schema);
    writeBlock (0, &fh, ph);

    // since all the flag is zero, we don't need write anything
    // just append an empty block
    appendEmptyBlock(&fh);

    closePageFile(&fh);
    free(ph);
    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name) {
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    mgmt *m = (mgmt *) malloc(sizeof(mgmt));
    Schema **schema;

    initBufferPool(bm, name, BF_PAGE, BF_MODE, NULL);
    pinPage(bm, ph, 0);
    // ph->data is the content of page 0
    // read information from page 0
    // ...

    int pos = 0, tr = 0;
    pos = get_int(ph->data, pos, &tr);
    pos = get_schema(ph->data, pos, schema);

    // print all the informations
    // printf("open page : total %d\n", tr);
    // printf("open page : schema t %d\n", (*schema)->numAttr);
    // printf("open page : schema key %d\n", (*schema)->keySize);
    // int i;
    // for (i = 0; i < (*schema)->numAttr; i++) {
    //     printf("%s\n", (*schema)->attrNames[i]);
    // }
    // for (i = 0; i < (*schema)->numAttr; i++) {
    //     printf("%d\n", (*schema)->dataTypes[i]);
    // }
    // for (i = 0; i < (*schema)->numAttr; i++) {
    //     printf("%d\n", (*schema)->typeLength[i]);
    // }
    // for (i = 0; i < (*schema)->keySize; i++) {
    //     printf("%d\n", (*schema)->keyAttrs[i]);
    // }
    // end prints

    m->bm = bm;
    m->records = tr;
    m->lengthofrecord = getRecordSize(*schema);
    m->slotperpage = slot_per_page(m->lengthofrecord);
    m->last = 1;

    rel->name = name;
    rel->schema = *schema;
    rel->mgmtData = (void *)m;
    // unpinPage(bm, ph);
    free(ph);
    return RC_OK;
}

RC closeTable(RM_TableData *rel) {
    mgmt *m = (mgmt *)rel->mgmtData;
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    pinPage(m->bm, ph, 0);
    // update number of records
    add_int(ph->data, 0, m->records);
    markDirty(m->bm, ph);
    unpinPage(m->bm, ph);
    shutdownBufferPool(m->bm);
    free(m->bm);
    free(rel->mgmtData);
    return RC_OK;
}

RC deleteTable(char *name) {
    destroyPageFile(name);
    return RC_OK;
}

int getNumTuples(RM_TableData *rel) {
    return ((mgmt *) rel->mgmtData)->records;
}

void record_to_string(char *str, Record *record) {

}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {
    int slot, size, last, newslot;
    mgmt *m = (mgmt *)rel->mgmtData;
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    BM_BufferPool *bm = m->bm;

    size = m->lengthofrecord;
    last = m->last;
    slot = m->slotperpage;

    printf("Insert page %d totalslot %d\n", last, slot);

    char *flag = (char *) malloc(sizeof(char) * slot);
    memset(flag, 0, sizeof(char) * slot);

    // check last page is avaliable
    pinPage(bm, ph, last);
    memcpy(flag, ph->data, sizeof(char) * slot);
    unpinPage(bm, ph);

    // for (newslot = slot - 1; newslot >=0; newslot--) {
    //     if (flag[newslot] == 1) {
    //         break;
    //     }
    // }
    // // if no available slot
    // if (newslot == slot - 1) {
    //     last++;
    //     m->last = last;
    //     newslot = -1;
    //     memset(flag, 0, sizeof(char) * slot);
    // }
    // newslot = newslot + 1;
    // flag[newslot] = 1;

    // printf("Insert page %d slot %d\n", last, newslot);

    // char *strvalue = (char *) malloc(sizeof(char) * size);
    // record_to_string(strvalue, record);

    // pinPage(bm, ph, last);
    // // update slog flag
    // memcpy(ph->data, flag, sizeof(char) * slot);
    // // insert record
    // memcpy(ph->data + newslot * size + sizeof(char) * slot, strvalue, size);
    // markDirty(bm, ph);
    // unpinPage(bm, ph);

    // record->id.page = last;
    // record->id.slot = newslot;
    // ((mgmt *) rel->mgmtData)->records += 1;
    // free(flag);
    // free(strvalue);
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {

    ((mgmt *) rel->mgmtData)->records -= 1;
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    return RC_OK;
}


// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {
    return RC_OK;
}

RC closeScan(RM_ScanHandle *scan) {
    return RC_OK;
}


// dealing with schemas
int getRecordSize(Schema *schema) {
    int i, sum;
    sum = 0;
    for (i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT :
                sum += sizeof(int);
                break;
            case DT_STRING :
                sum += schema->typeLength[i];
                break;
            case DT_FLOAT :
                sum += sizeof(float);
                break;
            case DT_BOOL :
                sum += sizeof(bool);
                break;
        }
    }
    return sum;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    // recieve all the parameters and malloc space to store schema
    Schema *sc;
    sc = (Schema *) malloc(sizeof(Schema));
    sc->numAttr = numAttr;
    sc->attrNames = attrNames;
    sc->dataTypes = dataTypes;
    sc->typeLength = typeLength;
    sc->keyAttrs = keys;
    sc->keySize = keySize;
    return sc;
}

RC freeSchema(Schema *schema) {
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord(Record **record, Schema *schema) {
    Record *r = (Record *) malloc(sizeof(Record));
    // for a record, data is a array of values, so we should malloc space.
    Value *v = (Value *) malloc(sizeof(Value) * schema->numAttr);
    r->data = (char *)v;
    *record = r;
    return RC_OK;
}

RC freeRecord(Record *record) {
    free(record->data);
    free(record);
    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    Value *v, *r;
    v = (Value *)record->data;
    // should malloc a new value that store the value from record
    r = (Value *) malloc(sizeof(Value));
    r->dt = schema->dataTypes[attrNum];
    switch (r->dt) {
        case DT_INT:
            r->v.intV = v[attrNum].v.intV;
            break;
        case DT_FLOAT:
            r->v.floatV = v[attrNum].v.floatV;
            break;
        case DT_BOOL:
            r->v.boolV = v[attrNum].v.boolV;
            break;
        case DT_STRING:
            // use strcpy to copy all the strings
            r->v.stringV = (char *) malloc(sizeof(char) * schema->typeLength[attrNum]);
            strcpy(r->v.stringV, v[attrNum].v.stringV);
            break;
    }
    // remember to return pointer of new value
    *value = r;
    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    Value *v = (Value *)record->data;
    memcpy(v + attrNum, value, sizeof(Value)); // use memcpy to copy entire value
    if (schema->dataTypes[attrNum] == DT_STRING) {
        // if datatype is string, remember to copy all the char(strcpy)
        v[attrNum].v.stringV = (char *) malloc(schema->typeLength[attrNum]);
        strcpy(v[attrNum].v.stringV, value->v.stringV);
    }
    return RC_OK;
}
