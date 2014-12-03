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
    // BM_BufferPool *bm;
    int lengthofrecord;
    int slotperpage;
    int last;
    char *name;
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
            printf("%d : %d : %s", value->v, strlen(value->v.stringV), value->v);
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

int add_float(char *str, int len, float n) {
    memcpy(str + len, &n, sizeof(float));
    return len + sizeof(float);
}

int get_float(char *str, int len, float *n) {
    memcpy(n, str + len, sizeof(float));
    return len + sizeof(float);
}

int add_string(char *str, int len, char *value) {
    int strl = strlen(value);
    memcpy(str + len, &strl, sizeof(int));
    memcpy(str + len + sizeof(int), value, strlen(value));
    return len + strl + sizeof(int);
}

int get_string(char *str, int len, char *value) {
    int strl = 0;
    memcpy(&strl, str + len, sizeof(int));
    memcpy(value, str + len + sizeof(int), strl);
    value[strl] = '\0';
    return len + strl + sizeof(int);
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
    // add total record
    pos = add_int(ph, pos, 0);
    // add schema
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
    SM_FileHandle fh;
    SM_PageHandle ph;
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
    memset(ph, 0, sizeof(char) * PAGE_SIZE);

    mgmt *m = (mgmt *) malloc(sizeof(mgmt));
    Schema *schema;

    openPageFile(name, &fh);
    readBlock(0, &fh, ph);

    // ph->data is the content of page 0
    // read information from page 0
    // ...

    int pos = 0, tr = 0;
    pos = get_int(ph, pos, &tr);
    pos = get_schema(ph, pos, &schema);

    // printf("schema lloc %x\n", schema);

    // // print all the informations
    // printf("open page : total %d\n", tr);
    // printf("open page : schema t %d\n", schema->numAttr);
    // printf("open page : schema key %d\n", schema->keySize);
    // int i;
    // for (i = 0; i < schema->numAttr; i++) {
    //     printf("%s\n", schema->attrNames[i]);
    // }
    // for (i = 0; i < schema->numAttr; i++) {
    //     printf("%d\n", schema->dataTypes[i]);
    // }
    // for (i = 0; i < schema->numAttr; i++) {
    //     printf("%d\n", schema->typeLength[i]);
    // }
    // for (i = 0; i < schema->keySize; i++) {
    //     printf("%d\n", schema->keyAttrs[i]);
    // }
    // // end prints

    m->name = (char *) malloc(sizeof(char) * strlen(name));
    strcpy(m->name, name);
    m->records = tr;
    m->lengthofrecord = getRecordStringSize(schema);
    m->slotperpage = slot_per_page(m->lengthofrecord);
    m->last = 1;

    rel->name = name;
    rel->schema = schema;
    rel->mgmtData = (void *)m;
    closePageFile(&fh);
    free(ph);
    return RC_OK;
}

RC closeTable(RM_TableData *rel) {
    SM_FileHandle fh;
    SM_PageHandle ph;
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
    memset(ph, 0, sizeof(char) * PAGE_SIZE);

    mgmt *m = (mgmt *)rel->mgmtData;

    openPageFile(m->name, &fh);
    readBlock(0, &fh, ph);

    // update number of records
    add_int(ph, 0, m->records);
    writeBlock (0, &fh, ph);
    closePageFile(&fh);

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

void record_to_string(char *str, Record *record, Schema *sc) {
    int i, pos = 0;
    Value *v = (Value *)record->data;
    for (i = 0; i < sc->numAttr; i++) {
        switch (sc->dataTypes[i]) {
            case DT_INT :
                pos = add_int(str, pos, v[i].v.intV);
                break;
            case DT_STRING :
                pos = add_string(str, pos, v[i].v.stringV);
                break;
            case DT_FLOAT :
                pos = add_float(str, pos, v[i].v.floatV);
                break;
            case DT_BOOL :
                pos = add_int(str, pos, (short)v[i].v.boolV);
                break;
        }
    }
}

void string_to_record(char *str, Record *record, Schema *sc) {
    int i, pos = 0;
    Value *val;
    char *c;
    int a;
    float b;
    for (i = 0; i < sc->numAttr; i++) {
        val = ((Value *)record->data) + i;
        switch (sc->dataTypes[i]) {
            case DT_INT :
                a = 0;
                pos = get_int(str, pos, &a);
                MAKE_VALUE(val, DT_INT, a);
                // printf("get %d\n", a);
                break;
            case DT_STRING :
                c = (char *) malloc(sizeof(char) * sc->typeLength[i]);
                pos = get_string(str, pos, c);
                // printf("get %s\n", c);
                MAKE_STRING_VALUE(val, c);
                free(c);
                break;
            case DT_FLOAT :
                b = 0;
                pos = get_float(str, pos, &b);
                MAKE_VALUE(val, DT_FLOAT, b);
                break;
            case DT_BOOL :
                a = 0;
                pos = get_int(str, pos, &a);
                MAKE_VALUE(val, DT_INT, a);
                break;
        }
        setAttr(record, sc, i, val);
        freeVal(val);
    }
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {
    int slot, size, last, newslot;
    mgmt *m = (mgmt *)rel->mgmtData;

    size = m->lengthofrecord;
    last = m->last;
    slot = m->slotperpage;

    // printf("Insert page %d totalslot %d\n", last, slot);

    char *flag = (char *) malloc(sizeof(char) * slot);
    memset(flag, 0, sizeof(char) * slot);

    // check last page is avaliable

    SM_FileHandle fh;
    SM_PageHandle ph;
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
    memset(ph, 0, sizeof(char) * PAGE_SIZE);

    openPageFile(m->name, &fh);
    readBlock(last, &fh, ph);
    memcpy(flag, ph, sizeof(char) * slot);

    // pinPage(bm, ph, last);
    // memcpy(flag, ph->data, sizeof(char) * slot);
    // unpinPage(bm, ph);

    for (newslot = slot - 1; newslot >=0; newslot--) {
        if (flag[newslot] == 1) {
            break;
        }
    }
    // if no available slot
    if (newslot == slot - 1) {
        last++;
        m->last = last;
        newslot = -1;
        memset(flag, 0, sizeof(char) * slot);
    }
    newslot = newslot + 1;
    flag[newslot] = 1;

    char *strvalue = (char *) malloc(sizeof(char) * size);
    record_to_string(strvalue, record, rel->schema);

    readBlock(last, &fh, ph);
    memcpy(ph, flag, sizeof(char) * slot);
    memcpy(ph + newslot * size + sizeof(char) * slot, strvalue, size);
    // printf("write offset : %u : %d\n", newslot * size + sizeof(char) * slot, size);

    // pinPage(bm, ph, last);
    // // update slog flag
    // memcpy(ph->data, flag, sizeof(char) * slot);
    // // insert record
    // memcpy(ph->data + newslot * size + sizeof(char) * slot, strvalue, size);
    // markDirty(bm, ph);
    // unpinPage(bm, ph);

    writeBlock (last, &fh, ph);
    closePageFile(&fh);

    record->id.page = last;
    record->id.slot = newslot;
    ((mgmt *) rel->mgmtData)->records += 1;
    free(flag);
    free(strvalue);
    free(ph);
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {

    ((mgmt *) rel->mgmtData)->records -= 1;
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    int slot, size, last, newslot;
    mgmt *m = (mgmt *)rel->mgmtData;

    size = m->lengthofrecord;
    last = record->id.page;
    slot = m->slotperpage;
    newslot = record->id.slot;

    printf("updating page %d slot %d\n", last, newslot);

    SM_FileHandle fh;
    SM_PageHandle ph;
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
    memset(ph, 0, sizeof(char) * PAGE_SIZE);

    char *strvalue = (char *) malloc(sizeof(char) * size);
    record_to_string(strvalue, record, rel->schema);

    readBlock(last, &fh, ph);
    memcpy(ph + newslot * size + sizeof(char) * slot, strvalue, size);
    // printf("write offset : %u : %d\n", newslot * size + sizeof(char) * slot, size);

    // pinPage(bm, ph, last);
    // // update slog flag
    // memcpy(ph->data, flag, sizeof(char) * slot);
    // // insert record
    // memcpy(ph->data + newslot * size + sizeof(char) * slot, strvalue, size);
    // markDirty(bm, ph);
    // unpinPage(bm, ph);

    writeBlock (last, &fh, ph);
    closePageFile(&fh);

    free(strvalue);
    free(ph);
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    // printf("start record loc : %x\n", record);
    mgmt *m = (mgmt *)rel->mgmtData;
    char *strvalue = (char *) malloc(sizeof(char) * m->lengthofrecord);

    // read file
    SM_FileHandle fh;
    SM_PageHandle ph;
    ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
    memset(ph, 0, sizeof(char) * PAGE_SIZE);
    openPageFile(m->name, &fh);
    readBlock(id.page, &fh, ph);
    closePageFile(&fh);

    // get string and covert to records
    memcpy(strvalue, ph + sizeof(char) * m->slotperpage + id.slot * m->lengthofrecord, m->lengthofrecord);

    // printf("read offset : %u : %d\n", sizeof(char) * m->slotperpage + id.slot * m->lengthofrecord, m->lengthofrecord);

    // createRecord(&record, rel->schema);
    // printf("create record loc : %x\n", record);

    string_to_record(strvalue, record, rel->schema);

    record->id.page = id.page;
    record->id.slot = id.slot;

    // printf("finish record loc : %x\n", record);
    // print_record(record, rel->schema);

    free(strvalue);
    free(ph);
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
int getRecordStringSize(Schema *schema) {
    int i, sum;
    sum = 0;
    for (i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT :
                sum += sizeof(int);
                break;
            case DT_STRING :
                sum += schema->typeLength[i] + 4;
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
