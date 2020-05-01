project(normal-tuple VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-tuple STATIC
        include/normal/tuple/Globals.h
        src/TupleSet2.cpp include/normal/tuple/TupleSet2.h include/normal/tuple/Scalar.cpp include/normal/tuple/Scalar.h include/normal/tuple/Column.cpp include/normal/tuple/Column.h include/normal/tuple/Schema.cpp include/normal/tuple/Schema.h)

target_include_directories(normal-tuple PUBLIC include)

target_link_libraries(normal-tuple PRIVATE spdlog::spdlog)
target_link_libraries(normal-tuple PUBLIC arrow_static)
target_link_libraries(normal-tuple PUBLIC normal-core)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
