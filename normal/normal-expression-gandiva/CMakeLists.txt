project(normal-expression-gandiva VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression-gandiva STATIC
        src/Globals.h
        src/Expression.cpp include/normal/expression/gandiva/Expression.h
        src/Column.cpp include/normal/expression/gandiva/Column.h
        src/Cast.cpp include/normal/expression/gandiva/Cast.h
        src/Projector.cpp include/normal/expression/gandiva/Projector.h)

target_include_directories(normal-expression-gandiva PUBLIC include)

target_link_libraries(normal-expression-gandiva PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression-gandiva PUBLIC arrow_static)
target_link_libraries(normal-expression-gandiva PUBLIC gandiva_static)
target_link_libraries(normal-expression-gandiva PUBLIC expected)
target_link_libraries(normal-expression-gandiva PUBLIC normal-core)
target_link_libraries(normal-expression-gandiva PUBLIC normal-expression)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps(normal-expression-gandiva)
