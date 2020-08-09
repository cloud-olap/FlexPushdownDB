project(normal-ssb VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# SSB common classes
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-ssb
        include/normal/ssb/Globals.h
        src/TestUtil.cpp include/normal/ssb/TestUtil.h
        src/SQLite3.cpp include/normal/ssb/SQLite3.h
        src/query1_1/Operators.cpp include/normal/ssb/query1_1/Operators.h
        src/query1_1/SQL.cpp include/normal/ssb/query1_1/SQL.h
        src/query1_1/S3SelectQueries.cpp include/normal/ssb/query1_1/S3SelectQueries.h
        src/query1_1/LocalFileSystemQueries.cpp include/normal/ssb/query1_1/LocalFileSystemQueries.h
        src/query1_1/LocalFileSystemTests.cpp include/normal/ssb/query1_1/LocalFileSystemTests.h
        src/query1_1/S3SelectTests.cpp include/normal/ssb/query1_1/S3SelectTests.h src/SqlGenerator.cpp include/normal/ssb/SqlGenerator.h)

target_include_directories(normal-ssb PUBLIC include)

target_link_libraries(normal-ssb PRIVATE doctest::doctest)
target_link_libraries(normal-ssb PRIVATE LLVM-filesystem)
target_link_libraries(normal-ssb PRIVATE fmt::fmt)
target_link_libraries(normal-ssb PRIVATE normal-plan)
target_link_libraries(normal-ssb PRIVATE sqlite3_static)
target_link_libraries(normal-ssb PRIVATE sqlite3_csv_static)
target_link_libraries(normal-ssb PUBLIC normal-core)
target_link_libraries(normal-ssb PUBLIC spdlog::spdlog)


#-----------------------------------------------------------------------------------------------------------------------
# Benchmark
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-benchmark
        bench/Main.cpp
        bench/ATTIC/SSBBenchmark.cpp
        bench/ATTIC/SSBNanoBenchmark.cpp
        bench/SSBBenchmarkQuery1_1FileSF0_01.cpp
        bench/SSBBenchmarkQuery1_1FileSF1.cpp)

target_link_libraries(normal-ssb-benchmark PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-benchmark PRIVATE nanobench::nanobench)
target_link_libraries(normal-ssb-benchmark PRIVATE normal-ssb)
target_link_libraries(normal-ssb-benchmark PRIVATE normal-sql)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-test
        test/Main.cpp
        test/SSBQuery1_1FileTestSF0_01.cpp
        test/SSBQuery1_1FileTestSF1.cpp
        test/SSBQuery1_1S3PullUpTestSF0_01.cpp
        test/SSBQuery1_1SQLTest.cpp
        test/SSBQuery1_1S3PushDownTestSF0_01.cpp
        test/SSBQuery1_1S3HybridTestSF0_01.cpp
        test/MiscTest.cpp)

target_link_libraries(normal-ssb-test PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-test PRIVATE normal-ssb)
target_link_libraries(normal-ssb-test PRIVATE normal-sql)


#-----------------------------------------------------------------------------------------------------------------------
# Data
#-----------------------------------------------------------------------------------------------------------------------

# Generate SSB SF=0.01 data set

add_custom_target(normal-ssb-data-generate
        COMMAND DSS_PATH=${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01 ${SSB_DBGEN_EXECUTABLE} -f -v -s 0.01
        WORKING_DIRECTORY ${SSB_DBGEN_WORKING_DIRECTORY})
add_dependencies(normal-ssb-data-generate ssb_db_gen)

# dbgen doesn't generate headers so we add them manually
add_custom_target(normal-ssb-data-prepend-headers
        COMMAND sed -i '' '1s/^/LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_SUPPKEY,LO_ORDERDATE,LO_ORDERPRIORITY,LO_SHIPPRIORITY,LO_QUANTITY,LO_EXTENDEDPRICE,LO_ORDTOTALPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE\r/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/lineorder.tbl
        COMMAND sed -i '' '1s/^/P_PARTKEY,P_NAME,P_MFGR,P_CATEGORY,P_BRAND1,P_COLOR,P_TYPE,P_SIZE,P_CONTAINER\r/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/part.tbl
        COMMAND sed -i '' '1s/^/S_SUPPKEY,S_NAME,S_ADDRESS,S_CITY,S_NATION,S_REGION,S_PHONE\r/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/supplier.tbl
        COMMAND sed -i '' '1s/^/C_CUSTKEY,C_NAME,C_ADDRESS,C_CITY,C_NATION,C_REGION,C_PHONE,C_MKTSEGMENT\r/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/customer.tbl
        COMMAND sed -i '' '1s/^/D_DATEKEY,D_DATE,D_DAYOFWEEK,D_MONTH,D_YEAR,D_YEARMONTHNUM,D_YEARMONTH,D_DAYNUMINWEEK,D_DAYNUMINMONTH,D_MONTHNUMINYEAR,D_WEEKNUMINYEAR,D_SELLINGSEASON,D_LASTDAYINMONTHFL,D_HOLIDAYFL,D_WEEKDAYFL,D_DAYNUMINYEAR\r/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/date.tbl)
add_dependencies(normal-ssb-data-prepend-headers normal-ssb-data-generate)

add_custom_target(normal-ssb-data)
add_dependencies(normal-ssb-data normal-ssb-data-prepend-headers)


# Copy data to runtime directory

configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/customer.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/customer.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/date.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/date.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/lineorder.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/lineorder.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/part.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/part.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/supplier.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/supplier.tbl COPYONLY)

configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/customer.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/customer.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/date.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/date.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/lineorder.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/lineorder.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/part.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/part.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/supplier.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/supplier.tbl COPYONLY)

#-----------------------------------------------------------------------------------------------------------------------
# Query generator
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-ssb-query-generate
        src/SqlGenerator.cpp include/normal/ssb/SqlGenerator.h)

target_link_libraries(normal-ssb-query-generate PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-query-generate PRIVATE normal-ssb)

#-----------------------------------------------------------------------------------------------------------------------
# Experiment
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-experiment
        experiment/ExperimentMain.cpp
        experiment/Tests.cpp
        experiment/ExperimentUtil.cpp   experiment/ExperimentUtil.h
        experiment/HardcodedPlanTestSinglePartition.cpp
        experiment/HardcodedPlanTestMultiPartition.cpp
        experiment/SqliteTest.cpp)

target_link_libraries(normal-ssb-experiment PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-experiment PRIVATE normal-ssb)
target_link_libraries(normal-ssb-experiment PRIVATE normal-sql)
target_link_libraries(normal-ssb-experiment PRIVATE normal-ssb-query-generate)

# Copy ssb query files to runtime directory

set(SSB_query_file_list
        "query1.1.sql" "query1.2.sql" "query1.3.sql" "query1.1.1.sql"
        "query2.1.sql" "query2.2.sql" "query2.3.sql"
        "query3.1.sql" "query3.2.sql" "query3.3.sql" "query3.4.sql"
        "query4.1.sql" "query4.2.sql" "query4.3.sql")

FOREACH(SSB_query_file ${SSB_query_file_list})
    configure_file(${CMAKE_CURRENT_LIST_DIR}/sql/${SSB_query_file} ${CMAKE_CURRENT_BINARY_DIR}/sql/${SSB_query_file} COPYONLY)
ENDFOREACH(SSB_query_file)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-ssb-test)
#showTargetProps(normal-ssb-benchmark)
