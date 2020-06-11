project(normal-expression-simple VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression-simple STATIC
        src/Globals.h
        src/Expression.cpp include/normal/expression/simple/Expression.h
        src/Projector.cpp include/normal/expression/simple/Projector.h
        src/Column.cpp include/normal/expression/simple/Column.h
        src/Cast.cpp include/normal/expression/simple/Cast.h)

target_include_directories(normal-expression-simple PUBLIC include)

target_link_libraries(normal-expression-simple PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression-simple PUBLIC arrow_static)
target_link_libraries(normal-expression-simple PUBLIC expected)
target_link_libraries(normal-expression-simple PUBLIC normal-core)
target_link_libraries(normal-expression-simple PUBLIC normal-expression)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-expression-simple)
