# FtcgPrototype


## Changes
### 2024.12.31
Whether or not to store all data in memory before compression can be set on the command line and can be adjusted by the user. -d 1 is considered to be in external memory, -d 0 is regarded as in memory, and it is in memory by default when the suffix is not written, please enter -d 1 when the dataset is larger than your memory space.

### 2025.1.8
- Removed the OpenCV library
- Removed the eigenfrequency method 
- The subclass that uses dedup is rewritten in the dedup stage, and the dedupindex is used instead of the previous fpindex, and only one IsDedup is needed to replace the chunkid assignment and deduplication

### 2025.1.8
- Complements the baseline
- Maintained design1 and removed the bug caused by SFB outputting multiple outputs to finishedgroup
- Remove a lot of code redundancy and unclear variable dependencies