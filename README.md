# FlexPushdownDB

-----------------

A research cloud OLAP engine using hybrid caching and computation pushdown.

[1] Yifei Yang, Matt Youill, Matthew Woicik, Yizhou Liu, Xiangyao Yu, Marco Serafini, Ashraf Aboulnaga, Michael Stonebraker, FlexPushdownDB: Hybrid Pushdown and Caching in a Cloud DBMS, VLDB 2021.

## Dependencies

To install required dependencies:

```
git clone https://github.com/cloud-olap/FlexPushdownDB
cd FlexPushdownDB
```

```
sudo ./setup.sh
```

Or specifically for Ubuntu systems, run
```
sudo ./tools/project/bin/ubuntu-prerequisites.sh
```

## Build

Compiler needed: LLVM-10 or later versions.

#### To load CMake project:

```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_C_COMPILER=/usr/bin/clang-10 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-10 -G "CodeBlocks - Unix Makefiles" ..
```

#### To build the core of engine:

```
cmake --build . --target normal-pushdown
```

#### To build the executable of query execution using SSB benchmark:

```
cmake --build . --target normal-ssb-experiment
```

#### To build the executable for query generator of SSB queries:

```
cmake --build . --target normal-ssb-query-generate-file
```

## Run

### To run a batch of queries:

Put a batch of queries into <build_directory>/normal-ssb/sql/generated/, or run the query generator shown below, then:

```
./normal-ssb-experiment <cache_size> <mode> <caching_policy>
```

### Configurations:

cache_size: allocated space of the segment cache.

```
Float number with GB unit
```

mode: whether to enable caching and pushdown capabilities.

```
1 - Pullup
2 - Pushdown-only
3 - Caching-only
4 - Hybrid
```

caching_policy: cache replacement policy used in the segment cache.

```
1 - LRU
2 - LFU
3 - Weighted-LFU
4 - Belady
```

### To generate a batch of queries:

```
./normal-ssb-query-generate-file <type> <size> <skewness>
```

### Configurations:

type: the workload type.

```
1 - SSB workload
2 - The workload involving different pushdown cost, used for benchmarking Weighted-LFU caching policy
```

size: number of queries in the batch generated.
  
skewness: parameter of Zipfian distribution, which we adopt in the workload.
