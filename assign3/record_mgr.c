#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"

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

RC initRecordManager(void *mgmtData) {
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}

RC createTable(char *name, Schema *schema) {
    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name) {
    return RC_OK;
}

RC closeTable(RM_TableData *rel) {
    return RC_OK;
}

RC deleteTable(char *name) {
    return RC_OK;
}

int getNumTuples(RM_TableData *rel) {
    return 0;
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
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
