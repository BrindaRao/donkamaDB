CC=gcc-4.9
OBJ=test_assign3_1.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o expr.o record_mgr.o rm_serializer.o
OUT=record_mgr

OBJEXPR = test_expr.o expr.o record_mgr.o storage_mgr.o rm_serializer.o dberror.o
OUTEXPR = test_expr

OBJEXT = test_assign3_2.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o expr.o record_mgr.o rm_serializer.o
OUTEXT = extra_test

all: main expr extra
.PHONY: all

main: $(OBJ)
	$(CC) -o $(OUT) $(OBJ)

expr: $(OBJEXPR)
	$(CC) -o $(OUTEXPR) $(OBJEXPR)

extra: $(OBJEXT)
	$(CC) -o $(OUTEXT) $(OBJEXT)

test_assign3_2.o: record_mgr.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h tables.h
buffer_mgr_stat.o: buffer_mgr.h
record_mgr.o: record_mgr.h buffer_mgr.h
buffer_mgr.o: dberror.h storage_mgr.h dt.h
dberror.o: dberror.h
rm_serializer.o: record_mgr.h tables.h dberror.h
expr.o: expr.h dberror.h tables.h
test_expr.o: expr.h dberror.h tables.h record_mgr.h test_helper.h
storage_mgr.o: storage_mgr.h dberror.h
test_assign3_1.o: record_mgr.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h tables.h

clean:
	rm $(OUT) $(OBJ) $(OUTEXPR) $(OUTEXT) test_expr.o test_assign3_2.o
