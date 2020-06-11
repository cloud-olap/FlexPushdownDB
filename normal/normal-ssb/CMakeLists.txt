project(normal-ssb VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Benchmark
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-benchmark
        include/normal/ssb/Globals.h
        src/TestUtil.cpp include/normal/ssb/TestUtil.h
        src/Queries.cpp include/normal/ssb/Queries.h
        bench/Main.cpp
        bench/SSBBenchmark.cpp)

target_include_directories(normal-ssb-benchmark PUBLIC include)

target_link_libraries(normal-ssb-benchmark PRIVATE spdlog::spdlog)
target_link_libraries(normal-ssb-benchmark PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-benchmark PRIVATE nanobench::nanobench)
target_link_libraries(normal-ssb-benchmark PRIVATE LLVM-filesystem)
target_link_libraries(normal-ssb-benchmark PRIVATE fmt::fmt)
target_link_libraries(normal-ssb-benchmark PRIVATE normal-plan)
target_link_libraries(normal-ssb-benchmark PRIVATE normal-sql)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-test
        include/normal/ssb/Globals.h
        src/TestUtil.cpp include/normal/ssb/TestUtil.h
        test/Main.cpp
        test/SSBTest.cpp
        src/Queries.cpp include/normal/ssb/Queries.h)

target_include_directories(normal-ssb-test PUBLIC include)

target_link_libraries(normal-ssb-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-ssb-test PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-test PRIVATE fmt::fmt)
target_link_libraries(normal-ssb-test PRIVATE normal-plan)
target_link_libraries(normal-ssb-test PRIVATE normal-sql)
target_link_libraries(normal-ssb-test PRIVATE normal-expression-gandiva)

#-----------------------------------------------------------------------------------------------------------------------
# Data
#-----------------------------------------------------------------------------------------------------------------------

# Generate SSB SF=0.01 data set

add_custom_target(normal-ssb-data
        COMMAND DSS_PATH=${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01 ${SSB_DBGEN_EXECUTABLE} -f -v -s 0.01
        WORKING_DIRECTORY ${SSB_DBGEN_WORKING_DIRECTORY})

# dbgen doesn't generate headers so we add them manually

add_custom_target(normal-ssb-data-headers
        COMMAND sed -i '1s/^/LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_SUPPKEY,LO_ORDERDATE,LO_ORDERPRIORITY,LO_SHIPPRIORITY,LO_QUANTITY,LO_EXTENDEDPRICE,LO_ORDTOTALPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/lineorder.tbl
        COMMAND sed -i '1s/^/P_PARTKEY,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/part.tbl
        COMMAND sed -i '1s/^/S_SUPPKEY,S_NAME,S_ADDRESS,S_CITY,S_NATION,S_REGION,S_PHONE\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/supplier.tbl
        COMMAND sed -i '1s/^/C_CUSTKEY,C_NAME,C_ADDRESS,C_CITY,C_NATION,C_REGION,C_PHONE,C_MKTSEGMENT\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/customer.tbl
        COMMAND sed -i '1s/^/D_DATEKEY,D_DATE,D_DAYOFWEEK,D_MONTH,D_YEAR,D_YEARMONTHNUM,D_YEARMONTH,D_DAYNUMINWEEK,D_DAYNUMINMONTH,D_DAYNUMINYEAR,D_MONTHNUMINYEAR,D_WEEKNUMINYEAR,D_SELLINGSEASON,D_LASTDAYINWEEKFL,D_LASTDAYINMONTHFL,D_HOLIDAYFL,D_WEEKDAYFL\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/date.tbl)

add_dependencies(normal-ssb-data ssb_db_gen)


# Some manually created data

configure_file(data/lineorder.csv ${CMAKE_CURRENT_BINARY_DIR}/data/lineorder.csv COPYONLY)
configure_file(data/date.csv ${CMAKE_CURRENT_BINARY_DIR}/data/date.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-ssb-test)
#showTargetProps(normal-ssb-benchmark)
