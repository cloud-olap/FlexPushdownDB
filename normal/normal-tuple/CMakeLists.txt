project(normal-tuple VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-tuple STATIC
        include/normal/tuple/Globals.h
        src/TupleSet2.cpp include/normal/tuple/TupleSet2.h
        src/Scalar.cpp include/normal/tuple/Scalar.h
        src/Column.cpp include/normal/tuple/Column.h
        src/Schema.cpp include/normal/tuple/Schema.h
        src/ColumnIterator.cpp include/normal/tuple/ColumnIterator.h
        src/ColumnIndex.cpp include/normal/tuple/ColumnIndex.h
        src/TupleSetShowOptions.cpp include/normal/tuple/TupleSetShowOptions.h
        src/ColumnName.cpp include/normal/tuple/ColumnName.h
        src/ColumnBuilder.cpp include/normal/tuple/ColumnBuilder.h)

target_include_directories(normal-tuple PUBLIC include)

target_link_libraries(normal-tuple PRIVATE spdlog::spdlog)
target_link_libraries(normal-tuple PUBLIC fmt::fmt)
target_link_libraries(normal-tuple PUBLIC arrow_static)
target_link_libraries(normal-tuple PUBLIC normal-core)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
