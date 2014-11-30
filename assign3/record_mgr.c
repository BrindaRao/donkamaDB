#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

#include "record_mgr.h"
#include "buffer_mgr.h"
#define MAX 30

int totalTables = 0;
int cfd;

typedef struct scanHandle
{
    int curPos;
    char * data;
    Expr * cond;
}scanHandle;

typedef struct BufferFrame
{
    BM_PageHandle ph;
    int fixcount;
    bool dirty;
    int recordPos;
    struct BufferFrame *next;
}BufferFrame;

typedef struct tableList
{
    char * name;
    Schema *schema;
    int schemaSize;
    int totalTuples;
    BM_BufferPool *bm;
}tableList;

tableList tb[MAX];

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    scanHandle * s;
    char * data;
    BM_BufferPool *bm;
    BM_PageHandle *page;
    BufferFrame* bf;
    page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
	bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    
    
    bm = (BM_BufferPool *)rel->mgmtData;
    bf = (BufferFrame*)bm->mgmtData;
    page->data = bf->ph.data;
    page->pageNum = bf->ph.pageNum;
    
    s = (scanHandle *)malloc(sizeof(scanHandle));
    //data = rel->mgmtData;
    s->data = page->data;
    s->curPos = 0;
    s->cond = cond;
    
    scan->rel = rel;
    scan->mgmtData = s;
    
    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    int rc, recordSize, pos, tuples;
    char * data;
    RID id;
    scanHandle * sh;
    Expr *cond;
    RM_TableData *t;
    Schema * sc;
    Value *result;
    Record * temp;
    
    temp = (Record *)malloc(sizeof(Record));
    temp->data = (char *)malloc(sizeof(char));
    t = (RM_TableData *)scan->rel;
    sc = (Schema *) t->schema;
    sh = (scanHandle *)scan->mgmtData;
    cond = (Expr *)sh->cond;
    pos = sh->curPos;
    recordSize = getRecordSize(sc);
    tuples = getNumTuples(t);
    id.page = 1;
    
    data = (char *)malloc(sizeof(char));
    data = (char *)sh->data;
    
    //printf("data: %p\n",sh->data);
    //printf("CHECKPOINT 5\n");
    //printf("totalTuples: %d\n",tuples);
    //printf("recordSize: %d\n",recordSize);
    //printf("int: %d\nchar: %d\n",sizeof(int),sizeof(char));
    
    if(cond == NULL)
    {
        if(pos >= ((recordSize+sizeof(int) + sizeof(char)) * tuples))
            return RC_RM_NO_MORE_TUPLES;
        
        record->data = (char *)(data + pos + sizeof(int));
        pos = pos + recordSize + sizeof(int) + sizeof(char);
        sh->curPos = pos;
        scan->mgmtData = sh;
    }
    else
    {
        while(1)
        {
            //printf("CHECKPOINT 6\n");
            if(pos >= ((recordSize+sizeof(int) + sizeof(char)) * tuples))
                return RC_RM_NO_MORE_TUPLES;
            
            id.slot = pos;
            //printf("%p\n",temp);
            //printf("pos: %d\n",id.slot);
            getRecord(t, id, temp);
            //printf("A: %d\n", *(temp->data));
            rc = evalExpr(temp, sc, cond, &result);
            //printf("funcA: %d\n",*(sh->data + 14));
            //printf("funcC: %d\n",*(sh->data + 14 + sizeof(int)*2 + sizeof(char)*4));
            //printf("TF: %d\n",result->v.boolV);
            if(result->v.boolV)
            {
                *(record) = *(temp);
                //printf("funcA: %d\n",*(record->data));
                //printf("funcC: %d\n",*(record->data + sizeof(int) + sizeof(char)*4));
                pos = pos + recordSize + sizeof(int) + sizeof(char);
                sh->curPos = pos;
                scan->mgmtData = sh;
                break;
            }
            
            pos = pos + recordSize + sizeof(int) + sizeof(char);
        }
    }
    //printf("funcA: %d\n",*(record->data));
    //printf("funcC: %d\n",*(record->data + sizeof(int) + sizeof(char)*4));
    return RC_OK;
}

RC closeScan (RM_ScanHandle *scan)
{
    scanHandle *sh;
    
    sh = (scanHandle *)scan->mgmtData;
    
    //free(scan->rel);
    //free(sh->data);
    //free(sh->cond);
    //free(sh);
    
    //free(scan);
    
    return RC_OK;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema * schema;
    
    schema = (Schema *)malloc(sizeof(Schema));
    
    schema->dataTypes = dataTypes;
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    
    return schema;
}

RC freeSchema (Schema *schema)
{
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    
    free(schema);
    
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema)
{
    Record *r;
    RID * id;
    char *data;
    int recordSize;
    
    recordSize = getRecordSize(schema);
    
    r = (Record *)malloc(sizeof(Record));
    data = (char *)malloc(sizeof(char));
    id = (RID *)malloc(sizeof(RID));
    r->id = *id;
    r->data = data;
    
    *(record) = r;
    
    //printf("r->data_point: %p\n",(*record)->data);
    
    return RC_OK;
}

RC freeRecord (Record *record)
{
    free(record->data);
    
    free(record);
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    int i, pos=0;
    char *data;
    DataType *dt;
    Value *v;
    
    data = record->data;
    dt = schema->dataTypes;
    
    for(i=0;i<attrNum;i++)
    {
        if(dt[i] == DT_INT)
            pos += sizeof(int);
        else if(dt[i] == DT_STRING)
            pos += (schema->typeLength[i]) * sizeof(char);
        else if(dt[i] == DT_FLOAT)
            pos += sizeof(float);
        else if(dt[i] == DT_BOOL)
            pos += sizeof(bool);
    }
    
    v = (Value *)malloc(sizeof(Value));
    v->dt = dt[i];
    if(dt[i] == DT_INT)
        v->v.intV = *(int *)(data+pos);
    else if(dt[i] == DT_STRING)
    {
        v->v.stringV = malloc((schema->typeLength[i]) * sizeof(char));
        memcpy(v->v.stringV,data+pos,(schema->typeLength[i]) * sizeof(char));
        //v->v.stringV = (char *)(data+pos);
        //printf("%s\n",v->v.stringV);
    }
    else if(dt[i] == DT_FLOAT)
        v->v.floatV = *(float *)(data+pos);
    else if(dt[i] == DT_BOOL)
        v->v.boolV = * (bool *)(data+pos);
    
    *(value) = v;
    
    return RC_OK;
    
}


RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    int i, pos=0;
    char *data;
    DataType *dt;
    Value *v;
    
    data = (char *)malloc(sizeof(char));
    dt = (DataType *)malloc(sizeof(DataType));
    v = (Value *)malloc(sizeof(Value));
    
    data = record->data;
    dt = schema->dataTypes;
    
    //printf("data_point: %p\n",data);
    //printf("schema_point: %p\n",schema);
    
    for(i=0;i<attrNum;i++)
    {
        if(dt[i] == DT_INT)
            pos += sizeof(int);
        else if(dt[i] == DT_STRING)
            pos += (schema->typeLength[i]) * sizeof(char);
        else if(dt[i] == DT_FLOAT)
            pos += sizeof(float);
        else if(dt[i] == DT_BOOL)
            pos += sizeof(bool);
    }
    
    if(value->dt == DT_INT)
        *((int *)(data+pos)) = (int)(value->v.intV);
    else if(value->dt == DT_STRING)
    {
        strcpy((data+pos),value->v.stringV);
        //*((char *)(data+pos)) = (char *)(value->v.stringV);
        //printf("value: %s\n",((record->data)+pos));
    }
    else if(value->dt == DT_FLOAT)
        *((float *)(data+pos)) = (float)(value->v.floatV);
    else if(value->dt == DT_BOOL)
        *((bool *)(data+pos)) = (bool)(value->v.boolV);
    
    //printf("schema_numAttr: %d\n",schema->numAttr);
    record->data = data;
    
    return RC_OK;

}

RC initRecordManager (void *mgmtData)
{
    //I'm not sure what should the init do
    //printf("init Successful!\n");
    
    return RC_OK;
}

RC shutdownRecordManager ()
{
    //检查是否有未保存的修改
    //将所有为保存的修改写回硬盘
    //删除所有schema，释放相应空间
    return RC_OK;
}

/*
void * dealAttr(Schema *schema, int num)
{
    DataType *dt = schema->dataTypes;
    
    if(*(dt+num) == DT_INT)
        return (intNode *)malloc(sizeof(intNode));
    else if(*(dt+num) == DT_STRING)
        return (strNode *)malloc(sizeof(strNode));
    else if(*(dt+num) == DT_FLOAT)
        return (floatNode *)malloc(sizeof(floatNode));
    else if(*(dt+num) == DT_BOOL)
        return (boolNode *)malloc(sizeof(boolNode));
    else
    {
        printf("ERROR IN FUNCTION dealAttr");
        return NULL;
    }
    
}*/

RC createTable (char *name, Schema *schema)
{
    //写 table 信息到链表的相关变量
    int i = 0,test,dataCount = 0,rc;
    int attrNum = schema->numAttr;
    int keyNum = schema->keySize;
    char * buff = (char *)malloc(sizeof(char)*PAGE_SIZE);
    //void * q;
    //attrNode * p = (attrNode *)malloc(sizeof(attrNode));
    //p->next = NULL;
    //p->data = NULL;
    
    //写 table 信息到 page 文件的相关变量
    FILE * pFile;
    char * curName;
    char ** attrNames = (char **)malloc(sizeof(char*) * attrNum);
    DataType *cpDt = (DataType *)malloc(sizeof(DataType) * attrNum);
    int *cpSizes  = (int *)malloc(sizeof(int) * attrNum);
    int *cpKeys = (int *)malloc(sizeof(int) * keyNum);
    
    memcpy(cpDt, schema->dataTypes, sizeof(DataType) * attrNum);
    memcpy(cpSizes, schema->typeLength, sizeof(int) * attrNum);
    memcpy(cpKeys, schema->keyAttrs, sizeof(int) * keyNum);
    
    //根据name创建一个page文件来保存相应的table,以table的名字命名该文件
    pFile=fopen(name, "wb");
    if(pFile == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    
    //将table相关信息存到list中
    tb[totalTables].name = (char *)malloc(sizeof(char));
    strcpy(tb[totalTables].name, name);
    //保存 schema
    tb[totalTables].schema = schema;
    tb[totalTables].totalTuples = 0;
    
    //将参数个数写入 page文件
    fwrite(&(schema->numAttr), sizeof(int), 1, pFile);
    //*((int*)buff) = schema->numAttr;
    //dataCount = dataCount + sizeof(int);
    
    //按照schema生成一个空的table,同时将 schema 信息写入对应 page 文件
    i = 0;
    while(1)
    {
        //写参数名
        curName = (char *)malloc(sizeof(schema->attrNames[i])+1);
        strcpy(curName,schema->attrNames[i]);
        fwrite(curName, sizeof(curName), 1, pFile);
        //strcpy((buff+dataCount), curName);
        //dataCount = dataCount + strlen(curName);
        
        i++;
        if(i == attrNum)
            break;
    }
    
    BM_BufferPool *bm;
    BM_PageHandle *page;
    BufferFrame* bf;
    page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
	bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
	
	rc = initBufferPool(bm, name, 60, RS_FIFO, NULL);
    //printf("rc: %d\n",rc);
    bf = bm->mgmtData;
    bf->ph.pageNum = 1;
    
    fwrite(cpDt, sizeof(DataType), attrNum, pFile);
    fwrite(cpSizes, sizeof(int), attrNum, pFile);
    fwrite(cpKeys, sizeof(int), keyNum, pFile);
    fwrite(&keyNum, sizeof(int), 1, pFile);
    
    fwrite(buff, sizeof(char), dataCount, pFile);
    
    tb[totalTables].schemaSize = ftell(pFile);
    tb[totalTables].bm = bm;
    
    totalTables++;
    fclose(pFile);
    
    return rc;
}

RC openTable (RM_TableData *rel, char *name)
{
    int i;
    
    for(i=0;i<totalTables;i++)
        if(strcmp(tb[i].name, name) == 0)
            break;
    
    if(i == totalTables)
    {
        printf("No such table");
        return RC_FILE_NOT_FOUND;
    }
    
    rel->name = (char *)malloc(sizeof(char));
    strcpy(rel->name, name);
    
    rel->schema = tb[i].schema;
    //printf("rel->schema: %p\n",rel->schema);
    //printf("rel->schema->numAttr: %d\n",(rel->schema)->numAttr);
    
    /*
    BM_BufferPool *bm;
    BM_PageHandle *page;
    BufferFrame* bf;
    page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
	bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
	
	rc = initBufferPool(bm, name, 60, RS_FIFO, NULL);
    bf = bm->mgmtData;
    bf->ph.pageNum = 1;*/
    
    rel->mgmtData = tb[i].bm;
    
    return RC_OK;
}

RC closeTable (RM_TableData *rel)
{
    int rc;
	BM_BufferPool *bm;
    char *name, *buff;
    FILE * fp;
    BM_PageHandle *page;
    page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
    BufferFrame* bf;
    
    name = (char *)malloc(sizeof(char));
	bm = (BM_BufferPool *)rel->mgmtData;
    buff = (char *)malloc(sizeof(char));
    
    /*
    bf = (BufferFrame*)bm->mgmtData;
    page->pageNum = bf->ph.pageNum;
    page->data = bf->ph.data;
    buff = (char *)page->data;
    strcpy(name,rel->name);
    
    fp = fopen(name,"w");
    fwrite(buff,sizeof(buff),1,fp);
    fclose(fp);*/
    
    //shutdownBufferPool(bm);
	
    //free(rel->name);
    //free(rel->schema);
    //free(rel->mgmtData);
    
    //free(rel);
    
    return RC_OK;
}

RC deleteTable (char *name)
{
    int i;
    for(i=0;i<totalTables;i++)
        if(strcmp(tb[i].name,name) == 0)
            break;
    
    if(i == totalTables)
    {
        printf("No such table");
        return RC_OK;
    }
    
    //printf("i: %d\ntotalTables:%d\n",i,totalTables);
    
    for(;i+1<totalTables;i++)
    {
        strcpy(tb[i].name, tb[i+1].name);
        tb[i].schema = tb[i+1].schema;
        tb[i].bm = tb[i+1].bm;
        tb[i].totalTuples = tb[i+1].totalTuples;
        tb[i].schemaSize = tb[i+1].schemaSize;
    }
    
    totalTables -= 1;
    
    return RC_OK;
}

int getNumTuples (RM_TableData *rel)
{
    int i;
    char * name;
    
    name = (char *)malloc(sizeof(char));
    
    strcpy(name, rel->name);
    
    for(i=0;i<totalTables;i++)
        if(strcmp(name,tb[i].name)==0)
            return tb[i].totalTuples;
    
    return -1;
}

int getRecordSize (Schema *schema)
{
    int i,num, size = 0;
    DataType * dt;
    
    dt = (DataType *)malloc(sizeof(DataType));
    dt = schema->dataTypes;
    num = schema->numAttr;
    //printf("num: %d\n",num);
    
    for(i=0;i<num;i++)
    {
        if(dt[i] == DT_INT)
            size += sizeof(int);
        else if(dt[i] == DT_STRING)
            size += schema->typeLength[i] * sizeof(char);
        else if(dt[i] == DT_FLOAT)
            size += sizeof(float);
        else if(dt[i] == DT_BOOL)
            size += sizeof(bool);
        else
        {
            printf("ERROR IN FUNCTION getRecordSize.\n");
            return -1;
        }
    }
    
    return size;
}

RC insertRecord (RM_TableData *rel, Record *record)
{
    int i;
    int recordSize, totalTuples, slot, pageNum, recordPerPage;
    char * temp;
    char * name;
    BM_BufferPool *bm;
    BufferFrame *bf;
    BM_PageHandle *page;
    
    name = (char *)malloc(sizeof(char));
    strcpy(name, rel->name);
    
    //printf("rel: %p\n",rel->schema);
    //printf("numAttr: %d\n",(rel->schema)->numAttr);
    
    for(i=0;i<totalTables;i++)
        if(strcmp(name,tb[i].name)==0)
            break;
    
    if(i == totalTables)
        printf("ERROR IN FUNCTION insertRecord");
    
    page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
    bm = (BM_BufferPool *)rel->mgmtData;
    bf = (BufferFrame *)bm->mgmtData;
    //page->pageNum = (bf->ph).pageNum;
    //page->data = (bf->ph).data;
	
    if(getRecordSize(rel->schema) == -1)
        return RC_FILE_NOT_FOUND;
    recordSize = getRecordSize(rel->schema) + sizeof(int)+sizeof(char); //每个 record 后面有个相应的分节符和 tombstone
    //printf("recordSize: %d\n",recordSize);
    
    totalTuples = getNumTuples(rel);
    
    recordPerPage = PAGE_SIZE/recordSize;
    pageNum = totalTuples/recordPerPage + 1;
    
    slot = totalTuples * (recordSize);
    
    page->pageNum = pageNum;
    (bf->ph).pageNum = pageNum;
    page->data = (bf->ph).data;
    
    record->id.page = page->pageNum;
    record->id.slot = slot;
    
    //将 record 写入 buff
    //pinPage(bm,page,pageNum);
    //(bf->ph).data = page->data;
    
    temp = (char *)malloc(recordSize);
    *((int *) temp) = 0; //tombstone
    memcpy(temp + sizeof(int), record->data, recordSize-sizeof(int)-sizeof(char));
    *((char *) temp+recordSize-sizeof(char)) = '\n';
    
    memcpy((page->data+slot), temp, recordSize);
    //printf("funcA: %d\n",*(page->data+slot+sizeof(int)));
    //printf("funcC: %d\n",*(page->data+slot + sizeof(int)*2 + sizeof(char)*4));
    
    //printf("insert: %p\n",page->data);
    
    markDirty(bm, page);
    //unpinPage(bm, page);
    
    free(temp);
    
    tb[i].totalTuples += 1;
    
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id)
{
    int i, slot;
    char *name;
    
    BM_BufferPool *bm;
    BufferFrame *bf;
    BM_PageHandle *page;
    
    page =(BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
    bm = (BM_BufferPool *)rel->mgmtData;
    bf = (BufferFrame *)bm->mgmtData;
    page->pageNum = bf -> ph.pageNum;
    page->data = bf -> ph.data;
    
    slot = id.slot;
    *((int*)(page->data)+slot) = 1;//将 tombstone 设为1
    
    name = (char *)malloc(sizeof(char));
    strcpy(name, rel->name);
    for(i=0;i<totalTables;i++)
        if(strcmp(name,tb[i].name)==0)
            break;
    
    tb[i].totalTuples -= 1;
    
    return RC_OK;
}


RC updateRecord (RM_TableData *rel, Record *record)
{
    int slot, recordSize;
    
    BM_BufferPool *bm;
    BufferFrame *bf;
    BM_PageHandle *page;
    
    //printf("rel:%p\n",rel);
    //printf("record:%p\n",record);
    
    recordSize = getRecordSize(rel->schema);
    page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    //printf("recordSize:%d\n",recordSize);
    //printf("page:%p\n",page);
    
    bm = (BM_BufferPool *)rel->mgmtData;
    bf = (BufferFrame *)bm->mgmtData;
    //page->pageNum = bf -> ph.pageNum;
    page->data = bf -> ph.data;
    
    //printf("bm:%p\n",bm);
    //printf("bf:%p\n",bf);
    //printf("page->data:%p\n",bf -> ph.data);
    //printf("record->data:%p\n",record->data);
    
    slot = record->id.slot;
    memcpy((page->data)+slot+sizeof(int), record->data, recordSize);
    
    //markDirty(bm,page);
    
    return RC_OK;
    
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    int slot, recordSize;
    
    BM_BufferPool *bm;
    BufferFrame *bf;
    BM_PageHandle *page;
    
    recordSize = getRecordSize(rel->schema);
    //printf("recordSize: %d\n",recordSize);
    
    page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
    bm = (BM_BufferPool *)rel->mgmtData;
    bf = (BufferFrame *)bm->mgmtData;
    page->pageNum = (bf -> ph).pageNum;
    page->data = (bf -> ph).data;
    
    /*
    printf("get_page->data: %p\n",page->data);
    printf("get_page: %d\n",id.page);
    printf("get_slot: %d\n",id.slot);
     */
    //printf("slot: %d\n",id.slot);
    slot = id.slot;
    //printf("CHECK2\n");
    //printf("%p\n",record->data);
    //printf("get_point: %p\n",page->data);
    memcpy(record->data, ((page->data)+slot+sizeof(int)), recordSize);
    //printf("CHECK3\n");
    //printf("funcA: %d\n",*(page->data+slot+sizeof(int)));
    //printf("funcC: %d\n",*(page->data+slot + sizeof(int)*2 + sizeof(char)*4));
    
    record->id.slot = id.slot;
    record->id.page = id.page;
    
    return RC_OK;

}
