project(fpdb-calcite VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

#-----------------------------------------------------------------------------------------------------------------------
# Calcite server thrift - cpp
#-----------------------------------------------------------------------------------------------------------------------

add_custom_command(
        DEPENDS
            thrift_executable
            ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift
        OUTPUT
          ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer.cpp
          ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_types.cpp
          ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_server.skeleton.cpp
        COMMAND ${THRIFT_EXECUTABLE} --gen cpp -r -o ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift)

add_library(calciteserver_thrift_cpp STATIC
        ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_types.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp/CalciteServer_server.skeleton.cpp)

set_target_properties(calciteserver_thrift_cpp PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(calciteserver_thrift_cpp PRIVATE thrift_static)
target_link_libraries(calciteserver_thrift_cpp PRIVATE boost_headers)


#-----------------------------------------------------------------------------------------------------------------------
# Calcite server thrift - java
#-----------------------------------------------------------------------------------------------------------------------

add_custom_target(
        calciteserver_thrift_java ALL
        DEPENDS thrift_executable
        COMMAND ${THRIFT_EXECUTABLE} --gen java -r -out ${CMAKE_CURRENT_SOURCE_DIR}/../java/main/java ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/CalciteServer.thrift
        COMMAND mvn clean install -f ${CMAKE_SOURCE_DIR}/fpdb-calcite/java/pom.xml -DskipTests)

#-----------------------------------------------------------------------------------------------------------------------
# Calcite
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-calcite STATIC
        src/CalciteClient.cpp include/fpdb/calcite/CalciteClient.h
        src/CalciteConfig.cpp include/fpdb/calcite/CalciteConfig.h)

target_include_directories(fpdb-calcite PUBLIC include)

set_target_properties(fpdb-calcite PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(fpdb-calcite PUBLIC calciteserver_thrift_cpp)
target_link_libraries(fpdb-calcite PUBLIC thrift_static)
target_link_libraries(fpdb-calcite PUBLIC boost_headers)
target_link_libraries(fpdb-calcite PRIVATE fpdb-util)
add_dependencies(fpdb-calcite calciteserver_thrift_java)
