project(normal-ssb VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

#-----------------------------------------------------------------------------------------------------------------------
# SSB common classes
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-ssb
        include/normal/ssb/Globals.h
        src/TestUtil.cpp include/normal/ssb/TestUtil.h
        src/SQLite3.cpp include/normal/ssb/SQLite3.h
        src/SqlGenerator.cpp include/normal/ssb/SqlGenerator.h
        src/SSBSchema.cpp include/normal/ssb/SSBSchema.h src/GenerateSqlFile.cpp
        src/common/Operators.cpp include/normal/ssb/common/Operators.h
        src/query1_1/Operators.cpp include/normal/ssb/query1_1/Operators.h
        src/query1_1/SQL.cpp include/normal/ssb/query1_1/SQL.h
        src/query1_1/S3SelectQueries.cpp include/normal/ssb/query1_1/S3SelectQueries.h
        src/query1_1/LocalFileSystemQueries.cpp include/normal/ssb/query1_1/LocalFileSystemQueries.h
        src/query1_1/LocalFileSystemTests.cpp include/normal/ssb/query1_1/LocalFileSystemTests.h
        src/query1_1/S3SelectTests.cpp include/normal/ssb/query1_1/S3SelectTests.h
        src/query1_1/S3SelectTests.cpp include/normal/ssb/query1_1/S3SelectTests.h
        src/query2_1/Operators.cpp  include/normal/ssb/query2_1/Operators.h
        src/query2_1/LocalFileSystemTests.cpp include/normal/ssb/query2_1/LocalFileSystemTests.h
        src/query2_1/LocalFileSystemQueries.cpp include/normal/ssb/query2_1/LocalFileSystemQueries.h
        src/query2_1/SQL.cpp include/normal/ssb/query2_1/SQL.h )

target_include_directories(normal-ssb PUBLIC include)

target_link_libraries(normal-ssb PRIVATE doctest::doctest)
target_link_libraries(normal-ssb PRIVATE LLVM-filesystem)
target_link_libraries(normal-ssb PRIVATE fmt::fmt)
target_link_libraries(normal-ssb PRIVATE normal-plan)
target_link_libraries(normal-ssb PRIVATE sqlite3_static)
target_link_libraries(normal-ssb PRIVATE sqlite3_csv_static)
#target_link_libraries(normal-ssb PUBLIC normal-pushdown)
target_link_libraries(normal-ssb PUBLIC spdlog::spdlog)
target_link_libraries(normal-ssb PRIVATE ssl crypto)

#-----------------------------------------------------------------------------------------------------------------------
# Benchmark
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-benchmark
        bench/Main.cpp
        bench/ATTIC/SSBBenchmark.cpp
        bench/ATTIC/SSBNanoBenchmark.cpp
        bench/SSBBenchmarkQuery1_1FileSF0_01.cpp
        bench/SSBBenchmarkQuery1_1FileSF1.cpp
        bench/Globals.h)

target_link_libraries(normal-ssb-benchmark doctest::doctest)
target_link_libraries(normal-ssb-benchmark Backward::Backward)
target_link_libraries(normal-ssb-benchmark nanobench::nanobench)
target_link_libraries(normal-ssb-benchmark normal-ssb)
#target_link_libraries(normal-ssb-benchmark normal-sql)


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
        test/SSBQuery2_1FileTestSF0_01.cpp
        test/SSBTestQuery1_1FileParquetSF0_01.cpp
        test/MiscTest.cpp
        test/Globals.h)

target_link_libraries(normal-ssb-test doctest::doctest)
target_link_libraries(normal-ssb-test Backward::Backward)
target_link_libraries(normal-ssb-test normal-ssb)
#target_link_libraries(normal-ssb-test normal-sql)


#-----------------------------------------------------------------------------------------------------------------------
# Tool
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-tool
        tool/Main.cpp)

target_link_libraries(normal-ssb-tool PRIVATE spdlog::spdlog)
target_link_libraries(normal-ssb-tool PRIVATE normal-ssb)
target_link_libraries(normal-ssb-tool PRIVATE normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Data
#-----------------------------------------------------------------------------------------------------------------------

# Generate SSB SF=0.01 data set

add_custom_target(normal-ssb-data-generate
        COMMAND DSS_PATH=${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1 ${SSB_DBGEN_EXECUTABLE} -f -v -s 1
        WORKING_DIRECTORY ${SSB_DBGEN_WORKING_DIRECTORY})
add_dependencies(normal-ssb-data-generate ssb_db_gen)

# dbgen doesn't generate headers so we add them manually
add_custom_target(normal-ssb-data-prepend-headers
        COMMAND sed -i '' '1s/^/LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_SUPPKEY,LO_ORDERDATE,LO_ORDERPRIORITY,LO_SHIPPRIORITY,LO_QUANTITY,LO_EXTENDEDPRICE,LO_ORDTOTALPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/lineorder.tbl
        COMMAND sed -i '' '1s/^/P_PARTKEY,P_NAME,P_MFGR,P_CATEGORY,P_BRAND1,P_COLOR,P_TYPE,P_SIZE,P_CONTAINER\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/part.tbl
        COMMAND sed -i '' '1s/^/S_SUPPKEY,S_NAME,S_ADDRESS,S_CITY,S_NATION,S_REGION,S_PHONE\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/supplier.tbl
        COMMAND sed -i '' '1s/^/C_CUSTKEY,C_NAME,C_ADDRESS,C_CITY,C_NATION,C_REGION,C_PHONE,C_MKTSEGMENT\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/customer.tbl
        COMMAND sed -i '' '1s/^/D_DATEKEY,D_DATE,D_DAYOFWEEK,D_MONTH,D_YEAR,D_YEARMONTHNUM,D_YEARMONTH,D_DAYNUMINWEEK,D_DAYNUMINMONTH,D_DAYNUMINYEAR,D_MONTHNUMINYEAR,D_WEEKNUMINYEAR,D_SELLINGSEASON,D_LASTDAYINWEEKFL,D_LASTDAYINMONTHFL,D_HOLIDAYFL,D_WEEKDAYFL\\n/' ${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/date.tbl)
add_dependencies(normal-ssb-data-prepend-headers normal-ssb-data-generate)

add_custom_target(normal-ssb-data)
add_dependencies(normal-ssb-data normal-ssb-data-prepend-headers)


# Copy data to runtime directory

configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/customer.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/customer.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/date.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/date.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/lineorder.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/lineorder.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/part.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/part.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/supplier.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/supplier.tbl COPYONLY)

#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/customer.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/customer.tbl COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/date.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/date.tbl COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/lineorder.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/lineorder.tbl COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/part.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/part.tbl COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/supplier.tbl ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/supplier.tbl COPYONLY)

configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/parquet/customer.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/parquet/customer.snappy.parquet COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/parquet/date.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/parquet/date.snappy.parquet COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/parquet/lineorder.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/parquet/lineorder.snappy.parquet COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/parquet/part.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/parquet/part.snappy.parquet COPYONLY)
configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf0.01/parquet/supplier.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf0.01/parquet/supplier.snappy.parquet COPYONLY)

#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/parquet/customer.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/parquet/customer.snappy.parquet COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/parquet/date.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/parquet/date.snappy.parquet COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/parquet/lineorder.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/parquet/lineorder.snappy.parquet COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/parquet/part.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/parquet/part.snappy.parquet COPYONLY)
#configure_file(${CMAKE_CURRENT_LIST_DIR}/data/ssb-sf1/parquet/supplier.snappy.parquet ${CMAKE_CURRENT_BINARY_DIR}/data/ssb-sf1/parquet/supplier.snappy.parquet COPYONLY)

#-----------------------------------------------------------------------------------------------------------------------
# Query generator
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-ssb-query-generate
        src/SqlGenerator.cpp include/normal/ssb/SqlGenerator.h
        src/SqlTransformer.cpp include/normal/ssb/SqlTransformer.h)

target_link_libraries(normal-ssb-query-generate PUBLIC spdlog::spdlog)
target_link_libraries(normal-ssb-query-generate PUBLIC fmt::fmt)
target_include_directories(normal-ssb-query-generate PUBLIC include)

add_executable(normal-ssb-query-generate-file
        src/GenerateSqlFile.cpp)

target_link_libraries(normal-ssb-query-generate-file PRIVATE normal-ssb-query-generate)

#-----------------------------------------------------------------------------------------------------------------------
# Experiment
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-experiment
        experiment/ExperimentMain.cpp
        experiment/Tests.cpp
        experiment/ExperimentUtil.cpp   experiment/ExperimentUtil.h
        experiment/SqliteTest.cpp experiment/BeladyTest.cpp
        experiment/Tests.h
        experiment/MathModelTest.cpp experiment/MathModelTest.h)

target_link_libraries(normal-ssb-experiment doctest::doctest)
target_link_libraries(normal-ssb-experiment Backward::Backward)
target_link_libraries(normal-ssb-experiment normal-ssb)
#target_link_libraries(normal-ssb-experiment normal-sql)
target_link_libraries(normal-ssb-experiment normal-plan)
target_link_libraries(normal-ssb-experiment normal-ssb-query-generate)
# need to add and link libraries on mac
if (${APPLE})
  target_link_directories(normal-ssb-experiment PRIVATE /usr/local/opt/binutils/lib)
  target_link_directories(normal-ssb-experiment PRIVATE /usr/local/opt/openssl/lib)
  target_link_directories(normal-ssb-experiment PRIVATE /usr/local/opt/llvm@11/lib)
  target_link_directories(normal-ssb-experiment PRIVATE /usr/local/opt/libdeflate/lib)
  target_link_directories(normal-ssb-experiment PRIVATE /usr/local/lib)
  # fix "dyld: Library not loaded" on mac
  set(path_to_libs libgvplugin_neato_layout.6.dylib
          libgvplugin_core.6.dylib
          @rpath/libaws-cpp-sdk-core.dylib)
  set(change_libs ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_neato_layout.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/graphviz_ep/install/lib/graphviz/libgvplugin_core.6.dylib
          ${CMAKE_BINARY_DIR}/_deps/aws-cpp-sdk_ep/install/lib/libaws-cpp-sdk-core.dylib)
  foreach(path_to_lib change_lib IN ZIP_LISTS path_to_libs change_libs)
  add_custom_command(TARGET normal-ssb-experiment
          COMMAND install_name_tool -change ${path_to_lib} ${change_lib} normal-ssb-experiment)
  endforeach()
endif()

# Copy ssb query files to runtime directory

set(SSB_query_file_list
        "original/query1.1.sql" "original/query1.2.sql" "original/query1.3.sql"
        "original/query2.1.sql" "original/query2.2.sql" "original/query2.3.sql"
        "original/query3.1.sql" "original/query3.2.sql" "original/query3.3.sql" "original/query3.4.sql"
        "original/query4.1.sql" "original/query4.2.sql" "original/query4.3.sql"
        "filterlineorder/query1.1.sql" "filterlineorder/query1.2.sql" "filterlineorder/query1.3.sql"
        "filterlineorder/query2.1.sql" "filterlineorder/query2.2.sql" "filterlineorder/query2.3.sql"
        "filterlineorder/query3.1.sql" "filterlineorder/query3.2.sql" "filterlineorder/query3.3.sql" "filterlineorder/query3.4.sql"
        "filterlineorder/query4.1.sql" "filterlineorder/query4.2.sql" "filterlineorder/query4.3.sql")

FOREACH(SSB_query_file ${SSB_query_file_list})
    configure_file(${CMAKE_CURRENT_LIST_DIR}/sql/${SSB_query_file} ${CMAKE_CURRENT_BINARY_DIR}/sql/${SSB_query_file} COPYONLY)
ENDFOREACH(SSB_query_file)

#-----------------------------------------------------------------------------------------------------------------------
# Scripts
#-----------------------------------------------------------------------------------------------------------------------

configure_file(${CMAKE_CURRENT_LIST_DIR}/math_model.py ${CMAKE_CURRENT_BINARY_DIR}/math_model.py COPYONLY)
file(COPY scripts DESTINATION ${CMAKE_CURRENT_BINARY_DIR} FILE_PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-ssb-test)
#showTargetProps(normal-ssb-benchmark)
