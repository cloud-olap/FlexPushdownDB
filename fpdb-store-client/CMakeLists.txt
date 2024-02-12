
#-----------------------------------------------------------------------------------------------------------------------
# Library
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-store-client-objlib OBJECT
        src/Module.cpp include/fpdb/store/client/Module.hpp
        src/FPDBStoreClientConfig.cpp include/fpdb/store/client/FPDBStoreClientConfig.h)

set_target_properties(fpdb-store-client-objlib PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(fpdb-store-client-objlib PROPERTIES OUTPUT_NAME fpdb-store-client)

list(APPEND _PUBLIC_LINK_LIBRARIES expected)
list(APPEND _PUBLIC_LINK_LIBRARIES fpdb-util)

list(APPEND _PRIVATE_LINK_LIBRARIES fmt::fmt)

list(APPEND _PUBLIC_SHARED_LINK_LIBRARIES arrow_flight_shared)

list(APPEND _PUBLIC_STATIC_LINK_LIBRARIES arrow_flight_static)

target_include_directories(fpdb-store-client-objlib PUBLIC include)

target_link_libraries(fpdb-store-client-objlib ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb-store-client-objlib ${_PUBLIC_SHARED_LINK_LIBRARIES})
target_link_libraries(fpdb-store-client-objlib ${_PRIVATE_LINK_LIBRARIES})

add_library(fpdb-store-client-static STATIC $<TARGET_OBJECTS:fpdb-store-client-objlib>)
set_target_properties(fpdb-store-client-static PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb-store-client-objlib,OUTPUT_NAME>)
target_include_directories(fpdb-store-client-static PUBLIC $<TARGET_PROPERTY:fpdb-store-client-objlib,INCLUDE_DIRECTORIES>)
target_link_libraries(fpdb-store-client-static PUBLIC ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb-store-client-static PRIVATE ${_PUBLIC_STATIC_LINK_LIBRARIES})
target_link_libraries(fpdb-store-client-static PRIVATE ${_PRIVATE_LINK_LIBRARIES})

add_library(fpdb-store-client-shared SHARED $<TARGET_OBJECTS:fpdb-store-client-objlib>)
set_target_properties(fpdb-store-client-shared PROPERTIES OUTPUT_NAME $<TARGET_PROPERTY:fpdb-store-client-objlib,OUTPUT_NAME>)
target_include_directories(fpdb-store-client-shared PUBLIC $<TARGET_PROPERTY:fpdb-store-client-objlib,INCLUDE_DIRECTORIES>)
target_link_libraries(fpdb-store-client-shared PUBLIC ${_PUBLIC_LINK_LIBRARIES})
target_link_libraries(fpdb-store-client-shared PRIVATE ${_PUBLIC_SHARED_LINK_LIBRARIES})
target_link_libraries(fpdb-store-client-shared PRIVATE ${_PRIVATE_LINK_LIBRARIES})


#-----------------------------------------------------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb-store-client-test
        test/Global.cpp test/Global.hpp test/Main.cpp)

find_package(LibBFD REQUIRED)

target_link_libraries(fpdb-store-client-test fmt::fmt)
target_link_libraries(fpdb-store-client-test spdlog::spdlog)
target_link_libraries(fpdb-store-client-test doctest::doctest)
target_link_libraries(fpdb-store-client-test LibBFD::LibBFD)
target_link_libraries(fpdb-store-client-test Backward::Backward)
target_link_libraries(fpdb-store-client-test fpdb-store-client-static)
