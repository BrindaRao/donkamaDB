CC=gcc
OBJ=test_assign1_1.o dberror.o storage_mgr.o
OUT=storage_mgr

all: main extra_test
.PHONY: all

main: $(OBJ)
	$(CC) -o $(OUT) $(OBJ)

extra_test: extra_test.o dberror.o storage_mgr.o
	$(CC) -o extra_test extra_test.o dberror.o storage_mgr.o

storage_mgr.o: dberror.h storage_mgr.h
dberror.o: dberror.h
test_assign1_1.o: storage_mgr.h
extra_test.o: storage_mgr.h

clean:
	rm $(OUT) $(OBJ) extra_test extra_test.o
