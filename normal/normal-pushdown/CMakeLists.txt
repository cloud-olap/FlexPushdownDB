project("normal-pushdown" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

include(ExternalProject)
find_package(Git REQUIRED)

setDefaults()

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------




#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library("normal-pushdown" STATIC src/io/CSVParser.cpp src/io/CSVParser.h include/normal/pushdown/Globals.h)

target_sources("normal-pushdown" PRIVATE
         src/Collate.cpp include/normal/pushdown/Collate.h
        src/S3SelectScan.cpp include/normal/pushdown/S3SelectScan.h
        src/FileScan.cpp include/normal/pushdown/FileScan.h
        src/Aggregate.cpp include/normal/pushdown/Aggregate.h
        src/AggregateExpression.cpp include/normal/pushdown/AggregateExpression.h
        src/s3/S3SelectParser.cpp src/s3/S3SelectParser.h)

target_compile_features("normal-pushdown" PRIVATE cxx_std_17)
target_compile_options("normal-pushdown" PRIVATE -Wall -Wextra -pedantic)
set_target_properties("normal-pushdown" PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories("normal-pushdown" PUBLIC include)

target_link_libraries("normal-pushdown" PUBLIC "normal-core")
target_link_libraries("normal-pushdown" PRIVATE spdlog)
target_link_libraries("normal-pushdown" PRIVATE aws-cpp-sdk-core)
target_link_libraries("normal-pushdown" PRIVATE aws-cpp-sdk-s3)
target_link_libraries("normal-pushdown" PUBLIC arrow)
target_link_libraries("normal-pushdown" PUBLIC arrow-dataset)
target_link_libraries("normal-pushdown" PUBLIC jemalloc)
target_link_libraries("normal-pushdown" PUBLIC pthread)


#-----------------------------------------------------------------------------------------------------------------------
# Dependency graph
#-----------------------------------------------------------------------------------------------------------------------

add_dependencies("normal-pushdown" "normal-core")
add_dependencies("normal-pushdown" "spdlog")
add_dependencies("normal-pushdown" "aws-cpp-sdk-project")
add_dependencies("normal-pushdown" "arrow")
add_dependencies("normal-pushdown" "arrow-dataset")


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps("normal-pushdown")
