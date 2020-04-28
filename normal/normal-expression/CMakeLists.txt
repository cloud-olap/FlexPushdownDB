project(normal-expression VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression STATIC
        src/Globals.h
        src/Expression.cpp include/normal/expression/Expression.h
        src/Projector.cpp include/normal/expression/Projector.h)

target_include_directories(normal-expression PUBLIC include)

target_link_libraries(normal-expression PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression PUBLIC arrow_static)
target_link_libraries(normal-expression PUBLIC expected)
target_link_libraries(normal-expression PUBLIC normal-core)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps(normal-expression)
