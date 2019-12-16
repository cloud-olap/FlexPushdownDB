# CAF
set(CAF_VERSION "0.17.3")
set(CAF_URL "https://github.com/actor-framework/actor-framework.git")

ExternalProject_Add(caf-project
        PREFIX ${DEPENDENCIES_BASE_DIR}/caf
        INSTALL_DIR ${DEPENDENCIES_BASE_DIR}/caf
        GIT_REPOSITORY ${CAF_URL}
        GIT_TAG ${CAF_VERSION}
        CMAKE_ARGS
        -DCAF_BUILD_STATIC:BOOL=yes
        -DCAF_NO_CURL_EXAMPLES:BOOL=yes
        -DCAF_NO_EXAMPLES:BOOL=yes
        -DCAF_NO_PROTOBUF_EXAMPLES:BOOL=yes
        -DCAF_NO_QT_EXAMPLES:BOOL=yes
        -DCAF_NO_UNIT_TESTS:BOOL=yes
        -DCMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_STANDARD:STRING=${CMAKE_CXX_STANDARD}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX:STRING=${DEPENDENCIES_BASE_DIR}/caf
        )

ExternalProject_Get_Property(caf-project INSTALL_DIR)
set(CAF_INSTALL_DIR ${INSTALL_DIR})


file(MAKE_DIRECTORY ${CAF_INSTALL_DIR}/include) # Include directory needs to exist to run configure step


add_library("caf-core" SHARED IMPORTED)
set_target_properties("caf-core" PROPERTIES IMPORTED_LOCATION ${CAF_INSTALL_DIR}/lib/libcaf_core.so)
set_target_properties("caf-core" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INSTALL_DIR}/include)
add_dependencies("caf-core" "caf-project")
showTargetProps("caf-core")

add_library("caf-core-static" STATIC IMPORTED)
set_target_properties("caf-core-static" PROPERTIES IMPORTED_LOCATION ${CAF_INSTALL_DIR}/lib/libcaf_core_static.a)
set_target_properties("caf-core-static" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INSTALL_DIR}/include)
add_dependencies("caf-core-static" "caf-project")
showTargetProps("caf-core-static")


add_library("caf-io" SHARED IMPORTED)
set_target_properties("caf-io" PROPERTIES IMPORTED_LOCATION ${CAF_INSTALL_DIR}/lib/libcaf_io.so)
set_target_properties("caf-io" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INSTALL_DIR}/include)
add_dependencies("caf-io" "caf-project")
showTargetProps("caf-io")

add_library("caf-io-static" STATIC IMPORTED)
set_target_properties("caf-io-static" PROPERTIES IMPORTED_LOCATION ${CAF_INSTALL_DIR}/lib/libcaf_io_static.a)
set_target_properties("caf-io-static" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INSTALL_DIR}/include)
add_dependencies("caf-io-static" "caf-project")
showTargetProps("caf-io-static")


add_library("caf-openssl" STATIC IMPORTED)
set_target_properties("caf-openssl" PROPERTIES IMPORTED_LOCATION ${CAF_INSTALL_DIR}/lib/libcaf_openssl.so)
set_target_properties("caf-openssl" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INSTALL_DIR}/include)
add_dependencies("caf-openssl" "caf-project")
showTargetProps("caf-openssl")

add_library("caf-openssl-static" STATIC IMPORTED)
set_target_properties("caf-openssl-static" PROPERTIES IMPORTED_LOCATION ${CAF_INSTALL_DIR}/lib/libcaf_openssl_static.a)
set_target_properties("caf-openssl-static" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CAF_INSTALL_DIR}/include)
add_dependencies("caf-openssl-static" "caf-project")
showTargetProps("caf-openssl-static")
