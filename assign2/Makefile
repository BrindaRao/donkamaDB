CC=gcc
OBJ=test_assign2_1.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o
OUT=buffer_mgr

all: main extra_test
.PHONY: all

main: $(OBJ)
	$(CC) -o $(OUT) $(OBJ)

extra_test: extra_test.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o
	$(CC) -o extra_test extra_test.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o

buffer_mgr.o: dberror.h storage_mgr.h dt.h
buffer_mgr_stat.o: buffer_mgr.h
dberror.o: dberror.h
storage_mgr.o: storage_mgr.h dberror.h
test_assign2_1.o: storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h
extra_test.o: storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h

clean:
	rm $(OUT) $(OBJ) extra_test extra_test.o
