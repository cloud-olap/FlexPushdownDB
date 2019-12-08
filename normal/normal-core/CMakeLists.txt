project("normal-core" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library("normal-core" STATIC)

target_sources("normal-core" PRIVATE
        src/Operator.cpp include/normal/core/Operator.h
        src/OperatorContext.cpp include/normal/core/OperatorContext.h
        src/OperatorManager.cpp include/normal/core/OperatorManager.h)

target_compile_features("normal-core" PRIVATE cxx_std_17)
set_target_properties("normal-core" PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories("normal-core" PUBLIC include)

target_link_libraries("normal-core" PRIVATE spdlog)


#-----------------------------------------------------------------------------------------------------------------------
# Dependency graph
#-----------------------------------------------------------------------------------------------------------------------

add_dependencies("normal-core" "spdlog-project")


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps("normal-core")
