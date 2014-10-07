#CS 525 Assignment 1

Please see the formatted version at [Github](https://github.com/MayLava/donkamaDB/tree/master/assign1).

##Group Member:
- **Siyuan Wang** (A20320686)
- **Yangzhi Hong** (A20309977)
- **Han Wang** (A20282706)

##How to run

###Auto Compile
Simply run `./compile` in the directory. It will automatically compile the source code and run test case.

###Manual
- `make` Compile the source code to execute files.
- `./storage_mgr` Unit test provided by class.
- `./extra_text` Extra unit test made by our group.

In case `make clean` is needed before all the commands above if some compiled files exist.

##Implementation
The code implements a simple page file management.

After creating a page file, a file handle is returned. With the file handle, we can write or read data from the page file. When reading or writing data, a specific page number is **necessary**, or some functions such as `readPreviousBlock()`, `readNextBlock()` and `writeCurrentBlock()` will use a page number related to current pointer of file handle.

For all page file, we use standard `fopen()`, `fread()`, `fwrite()`, `fseek()` and `fclose()` to operate files. A file will be opened only when program is reading or writing data. After reading or writing operation, file will be closed immediately. When we open a page file, program will check the file. If the file is valid, a valid sign will be store in file handle. The valid sign indicates that the file is legal and accessible. For every operation of page file, program will check the valid sign.

curPagePos is a integer that always indicate the last operated block number. That means, when after reading or writing a block, current curPagePos will be the number it red or wrote. curPagePos will **NOT** increase automatically. If you want to read blocks one by one, please using `readNextBlock()`. Writing data to a non-exists number will **NOT** automatically create new block, please use `appendEmptyBlock()` then use `writeCurrentBlock()`.

##Details of extra test
Extra test includes test for **all** the functions defined in `storage_mgr.h`. It tests **rare** situations and makes sure that functions will return error code if something goes wrong.

Extra test creates a page file with 3 blocks finally. The position 200 in page 3 is 's', which distinguish it from others and is used to determine functions work well.

The test writes blocks first. Then it reads blocks and checks if matches the data it wrote. In addition, the test also checks pointer of file handle (a.k.a. curPagePos), and capacity of page file.
