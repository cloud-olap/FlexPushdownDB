project(normal-avro VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

INCLUDE_DIRECTORIES(/usr/local/include)

#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-avro STATIC
        src/avroTuple.cpp include/normal/avro_tuple/avroTuple.h)

target_include_directories(normal-avro PUBLIC include)

target_link_libraries(normal-avro PUBLIC /usr/local/lib/libavrocpp.so)
