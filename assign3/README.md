#CS 525 Assignment 3

Please see the formatted version at [Github](https://github.com/MayLava/donkamaDB/tree/master/assign3). (Update: link current unavailable because someone may see the entire code.)

##Group Member:
- **Siyuan Wang** (A20320686)
- **Yangzhi Hong** (A20309977)
- **Han Wang** (A20282706)

##How to run

###Manual
- `make` Compile the source code to execute files.
- `./record_mgr` Unit test provided by class.
- `./test_expr` Unit test provided by class.
- `./extra_test` Unit test made by **our group**.

In case `make clean` is needed before all the commands above if some compiled files exist.

##Implementation
In this assignment, we implement a record manager to store and scan the record.
There are basic methods for record: create, get, insert, update, delete. Each function is tested and works well.

For scanning, a ScanHandle should be created. It includes expression and scaning statitics. Use method `next()` to scan next satisfied record.

Actually, this code is the second version of this assignment.
At first, the data of Record is a set of value. But this data structure cannot pass the test. Because if the datatype of a value is string, then it will have a `char *`, which is a pointer to a string. Thus, if two values have different pointer, even if the string is totally same, `memcpy` will failed.
Thus, we change tha data of record to string, which successfully pass the test. Moreover, we found the program will be more efficient in this way.

##Details of extra test
Extra test is made for testing scan method. First it insert records to file and then try to find some of them which is satisfied. It inserts more records than official test, use a count to store the number of satisfied record. Next, for every found record, we will count it and finally match it with expected number.

BTW, extra test is made from the program provided by class. We use some of the code but implement a different case to test the code.
