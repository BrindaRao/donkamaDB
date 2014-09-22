CC=gcc
OBJ=test_assign1_1.o dberror.o storage_mgr.o
OUT=storage_mgr

all: main mytest
.PHONY: all

main: $(OBJ)
	$(CC) -o $(OUT) $(OBJ)

mytest: mytest.o dberror.o storage_mgr.o
	$(CC) -o mytest mytest.o dberror.o storage_mgr.o

storage_mgr.o: dberror.h storage_mgr.h
dberror.o: dberror.h
test_assign1_1.o: storage_mgr.h
mytest.o: storage_mgr.h

clean:
	rm $(OUT) $(OBJ) mytest mytest.o
