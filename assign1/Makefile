CC=gcc
SOURCE=test_assign1_1.o dberror.o storage_mgr.o
OUT=storagemgr

main: $(SOURCE)
	$(CC) -o $(OUT) $(SOURCE)
storage_mgr.o: dberror.h storage_mgr.h
dberror.o: dberror.h
test_assign1_1.o: storage_mgr.h

clean:
	rm $(OUT) $(SOURCE)
