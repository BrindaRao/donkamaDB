#CS 525 Assignment 2

Please see the formatted version at [Github](https://github.com/MayLava/donkamaDB/tree/master/assign2).

##Group Member:
- **Siyuan Wang** (A20320686)
- **Yangzhi Hong** (A20309977)
- **Han Wang** (A20282706)

##How to run

###Manual
- `make` Compile the source code to execute files.
- `./buffer_mgr` Unit test provided by class.

In case `make clean` is needed before all the commands above if some compiled files exist.

##Implementation
The code implements a simple buffer management.

Writing and reading files directly from file takes a lot of time, and buffer management keeps buffer in memory. With the buffer manager, program can make changes inside the memory, and write file once when work finished.

In our architecture, a buffer pool manages several buffer pages. Each buffer pool keeps a list of page handles. Buffer pages has a very important value: priority. When program pins a page and buffer pool is full, our program will automatically find a page with low priority page to replace. Priority works very well on both FIFO and LRU strategy. With the priority structure, our program makes the buffer manager more efficient.

For specific details about the source code, we use two structs to manage buffer page. First struct stores the total write and read I/O counts, and priority count for this buffer pool. Second struct stores statistics about buffer page, such as read and write count for itself, if it is dirty, and its priority. Moreover, our program checks carefully about memory management to avoid memory leak.
