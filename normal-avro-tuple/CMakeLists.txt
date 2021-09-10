project(normal-avro VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

INCLUDE_DIRECTORIES(/usr/local/include)
#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-avro-tuple
        # test
        test/makeTupleTest.cpp
        # source
        src/makeTuple.cpp
        # header
        include/normal/avro_tuple/makeTuple.h
        # schema
        include/normal/avro_tuple/customer.hh include/normal/avro_tuple/customer_d.hh
        include/normal/avro_tuple/date.hh include/normal/avro_tuple/date_d.hh
        include/normal/avro_tuple/lineorder.hh include/normal/avro_tuple/lineorder_d.hh
        include/normal/avro_tuple/part.hh include/normal/avro_tuple/part_d.hh
        include/normal/avro_tuple/supplier.hh include/normal/avro_tuple/supplier_d.hh)

target_include_directories(normal-avro-tuple PUBLIC include)

target_link_libraries(normal-avro-tuple PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-avro-tuple PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-avro-tuple PUBLIC normal-pushdown)
target_link_libraries(normal-avro-tuple PUBLIC /usr/local/lib/libavrocpp.so)

#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-avro-tuple-test
        # test
        test/makeTupleTest.cpp
        # source
        src/makeTuple.cpp
        # header
        include/normal/avro_tuple/makeTuple.h
        # schema
        include/normal/avro_tuple/customer.hh include/normal/avro_tuple/customer_d.hh
        include/normal/avro_tuple/date.hh include/normal/avro_tuple/date_d.hh
        include/normal/avro_tuple/lineorder.hh include/normal/avro_tuple/lineorder_d.hh
        include/normal/avro_tuple/part.hh include/normal/avro_tuple/part_d.hh
        include/normal/avro_tuple/supplier.hh include/normal/avro_tuple/supplier_d.hh)

target_include_directories(normal-avro-tuple-test PUBLIC include)

target_link_libraries(normal-avro-tuple-test PUBLIC normal-avro-tuple)
target_link_libraries(normal-avro-tuple-test PUBLIC /usr/local/lib/libavrocpp.so)