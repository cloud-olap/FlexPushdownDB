cmake_minimum_required(VERSION 3.10)

project(avro-example VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

include_directories(include)


add_executable(avro-example
        src/example.cpp
        include/cpx.hh
        )

target_link_libraries(avro-example PUBLIC /usr/local/lib/libavrocpp.so)


