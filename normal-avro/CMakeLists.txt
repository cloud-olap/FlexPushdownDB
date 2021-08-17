project(normal-avro VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-avro STATIC
        include/avroTuple.h src/avroTuple.cpp
        include/customer.hh include/customer_d.hh
        include/date.hh include/date_d.hh
        include/lineorder.hh include/lineorder_d.hh
        include/part.hh include/part_d.hh
        include/supplier.hh include/supplier_d.hh)

target_include_directories(normal-avro PUBLIC include)
