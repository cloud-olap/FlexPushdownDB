project(normal-expression VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression STATIC
        include/normal/expression/Globals.h
        include/normal/expression/Expression.h
        include/normal/expression/Expressions.h src/expression/Expressions.cpp
        include/normal/expression/Add.h
        include/normal/expression/Literal.h
        include/normal/expression/Multiply.h
        include/normal/expression/Subtract.h
        include/normal/expression/Divide.h
        include/normal/expression/Column.h
        include/normal/expression/Cast.h)

set_target_properties(normal-expression PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(normal-expression PUBLIC include)

target_link_libraries(normal-expression PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression PUBLIC arrow_static)
target_link_libraries(normal-expression PUBLIC gandiva_static)
target_link_libraries(normal-expression PUBLIC expected)
target_link_libraries(normal-expression PUBLIC normal-core)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-core)
