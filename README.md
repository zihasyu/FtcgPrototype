# FtcgPrototype


## Changes
### 2025.1.15
- finished the recipe design and restore. If you want to use restore, uncomment the DeCompressionAll() and restoreFile(readfileList[i]) sections of main().
- fixed some memory safety issues  
- preliminarily testing restore, it can be considered that it is currently a stable version
### 2025.1.14
- Restore is 80% complete
### 2025.1.13
- removed some erase(), BIB can improve performance by a great deal
- Final 8 merge 1 can be removed or retained

### 2025.1.12
- data Compressed data persistence
- Improved log output

### 2025.1.10
- The experimental functions of design2 and design3 have been improved
- Fixed the bug that experiment 3 would output the last tmpgroup twice
- Removed a number of O(n) repeated requests for memory

### 2025.1.9
- Complements the baseline
- Maintained design1 and removed the bug caused by SFB outputting multiple outputs to finishedgroup
- Remove a lot of code redundancy and unclear variable dependencies

### 2025.1.8
- Removed the OpenCV library
- Removed the eigenfrequency method 
- The subclass that uses dedup is rewritten in the dedup stage, and the dedupindex is used instead of the previous fpindex, and only one IsDedup is needed to replace the chunkid assignment and deduplication

### 2024.12.31
Whether or not to store all data in memory before compression can be set on the command line and can be adjusted by the user. -d 1 is considered to be in external memory, -d 0 is regarded as in memory, and it is in memory by default when the suffix is not written, please enter -d 1 when the dataset is larger than your memory space.








