#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"

// struct for test records
typedef struct TestRecord {
    int a;
    char *b;
    int c;
} TestRecord;

// helper methods
Record *testRecord(Schema *schema, int a, char *b, int c);
Schema *testSchema (void);
Record *fromTestRecord (Schema *schema, TestRecord in);

void myscans(void) {
    RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
    TestRecord inserts[] = { 
        {1, "aaaa", 3}, 
        {2, "bbbb", 2},
        {3, "cccc", 1},
        {4, "dddd", 3},
        {5, "eeee", 5},
        {6, "ffff", 1},
        {7, "gggg", 3},
        {8, "hhhh", 3},
        {9, "iiii", 2},
        {10, "jjjj", 5},
    };
    TestRecord scanOneResult[] = { 
        {3, "cccc", 1},
        {6, "ffff", 1},
    };
    bool flag;
    int sum = 0, sumexpacted = 0;
    TestRecord realInserts[100];
    int numInserts = 100, scanSizeOne = 2, i;
    Record *r;
    RID *rids;
    Schema *schema;
    RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
    Expr *sel, *left, *right;
    int rc;

    testName = "test creating a new table and inserting tuples";
    schema = testSchema();
    rids = (RID *) malloc(sizeof(RID) * numInserts);

    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(createTable("test_table_r",schema));
    TEST_CHECK(openTable(table, "test_table_r"));

    // insert rows into table
    for(i = 0; i < numInserts; i++) {
        realInserts[i] = inserts[i%10];
        realInserts[i].a = i;
        if (realInserts[i].c == 1) {
            sumexpacted++;
        }
        r = fromTestRecord(schema, realInserts[i]);
        TEST_CHECK(insertRecord(table,r)); 
        rids[i] = r->id;
    }

    TEST_CHECK(closeTable(table));
    TEST_CHECK(openTable(table, "test_table_r"));

    // run some scans
    MAKE_CONS(left, stringToValue("i1"));
    MAKE_ATTRREF(right, 2);
    MAKE_BINOP_EXPR(sel, left, right, OP_COMP_EQUAL);

    TEST_CHECK(startScan(table, sc, sel));
    while((rc = next(sc, r)) == RC_OK) {
        for(i = 0; i < scanSizeOne; i++) {
            flag = memcmp(fromTestRecord(schema, scanOneResult[i])->data,r->data,getRecordSize(schema));
            ASSERT_TRUE(flag, "found match record");
        }
        if (flag) {
                sum++;
        }
    }
    if (rc != RC_RM_NO_MORE_TUPLES) {
        TEST_CHECK(rc);
    }
    TEST_CHECK(closeScan(sc));

    ASSERT_EQUALS_INT(sumexpacted, sum, "match total records");

    // clean up
    TEST_CHECK(closeTable(table));
    TEST_CHECK(deleteTable("test_table_r"));
    TEST_CHECK(shutdownRecordManager());

    free(table);
    free(sc);
    freeExpr(sel);
    TEST_DONE();
}

// test name
char *testName;

int main(void) {
    testName = "";
    myscans();
    return 0;
}

Schema * testSchema(void) {
    Schema *result;
    char *names[] = { "a", "b", "c" };
    DataType dt[] = { DT_INT, DT_STRING, DT_INT };
    int sizes[] = { 0, 4, 0 };
    int keys[] = {0};
    int i;
    char **cpNames = (char **) malloc(sizeof(char*) * 3);
    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
    int *cpSizes = (int *) malloc(sizeof(int) * 3);
    int *cpKeys = (int *) malloc(sizeof(int));

    for(i = 0; i < 3; i++) {
        cpNames[i] = (char *) malloc(2);
        strcpy(cpNames[i], names[i]);
    }
    memcpy(cpDt, dt, sizeof(DataType) * 3);
    memcpy(cpSizes, sizes, sizeof(int) * 3);
    memcpy(cpKeys, keys, sizeof(int));

    result = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);

    return result;
}

Record * fromTestRecord(Schema *schema, TestRecord in) {
  return testRecord(schema, in.a, in.b, in.c);
}

Record * testRecord(Schema *schema, int a, char *b, int c) {
    Record *result;
    Value *value;

    TEST_CHECK(createRecord(&result, schema));

    MAKE_VALUE(value, DT_INT, a);
    TEST_CHECK(setAttr(result, schema, 0, value));
    freeVal(value);

    MAKE_STRING_VALUE(value, b);
    TEST_CHECK(setAttr(result, schema, 1, value));
    freeVal(value);

    MAKE_VALUE(value, DT_INT, c);
    TEST_CHECK(setAttr(result, schema, 2, value));
    freeVal(value);

    return result;
}
