project(fpdb-expression VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-expression STATIC
        include/fpdb/expression/Globals.h
        src/Expression.cpp include/fpdb/expression/Expression.h
        src/Projector.cpp include/fpdb/expression/Projector.h
        src/Filter.cpp include/fpdb/expression/Filter.h)

target_include_directories(fpdb-expression PUBLIC include)

target_link_libraries(fpdb-expression PUBLIC fpdb-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(fpdb-expression)
