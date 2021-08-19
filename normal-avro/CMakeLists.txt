project(normal-avro VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

INCLUDE_DIRECTORIES(/usr/local/include)
#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-avro STATIC
        include/normal/avro/avroTuple.h src/avroTuple.cpp
        include/normal/avro/customer.hh include/normal/avro/customer_d.hh
        include/normal/avro/date.hh include/normal/avro/date_d.hh
        include/normal/avro/lineorder.hh include/normal/avro/lineorder_d.hh
        include/normal/avro/part.hh include/normal/avro/part_d.hh
        include/normal/avro/supplier.hh include/normal/avro/supplier_d.hh)

target_include_directories(normal-avro PUBLIC include)

target_link_libraries(avro-example PUBLIC /usr/local/lib/libavrocpp.so)
