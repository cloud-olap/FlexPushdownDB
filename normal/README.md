# FlexPushdownDB

A cloud DBMS of hybrid caching and computation pushdown

### Dependencies

Needed libraries: zlib, openssl, flex, bison, binutil, ltdl

### Compiling

Needed compiler: LLVM-7

To build the exeutable for query execution:

	cmake --build . --target normal-ssb-experiment
  
To build the exeutable for the query generator:

	cmake --build . --target normal-ssb-query-generate-file

### Execution

Put a batch of queries into <build_directory>/normal-ssb/sql/generated/

#### Run a batch of queries:

	./normal-ssb-experiment <cache_size> <mode> <caching_policy>
  
Specification of parameters:

cache size: 

	Float number with GB unit

mode: 

	1 - Pullup
	2 - Pushdown-only
	3 - Caching-only
	4 - Hybrid

caching policy:

	1 - LRU
	2 - LFU
	3 - Weighted-LFU
	4 - Belady

#### Generate a batch of queries:

	./normal-ssb-query-generate-file <type> <size> <skewness>

Specification of parameters:

type:

	1 - SSB workload
	2 - SSB workload with difference of pushdown cost

size:

	Number of queries in the batch
  
skewness:

	Theta of Zipfian distribution
