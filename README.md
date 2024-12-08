# Take Home Solution: External Sort
This directory contains solution for the take home problem of sorting a larger-than-memory file.

# Build
cmake .. && make

# Run
./sort <file_path>

# How does it work?
This is a naive implementation of external sort that utilizes seastar framework.

The code tries to sort the files if it can fit the memory, otherwise it recursively chunks the file until a single chunk can fit into memory.

# Assumptions
The code assumes fixed 4k record size, defined as a constant. The sorted file name is created via appending ".sort" to the source file name.
The program creates temporary files which are automatically deleted.

# Further Improvements
- Code can be simplified and made cleaner by appropriately difining classes and doing error handling. Current code doesn't do pretty much any error handling.
- I would have added tests had I have a bit more time.
- I would avoid hard-coding the merge factor of five (i.e. if a shard cannot be sorted wholly, divide it further into 5 pieces). Making it dynamic is not very hard.
- There are a lot of optimizations to get the best out of the available system. E.g. the merge phase intividuall reads and writes single records. This can be easily be done in batches. We can also smartly prefetch to avoid unnecessary blocking IO.
- Even algorithmically the implementation could be improved by dynamically balancing between number of merge passes and chunk size. See [this](https://en.wikipedia.org/wiki/External_sorting#Additional_passes) for a reference.
- The code needs a lot of small fixes. Many pass-by-values can be pass-by-reference. Right now the code is not consistent in that regard.