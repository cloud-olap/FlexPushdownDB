project(normal-sql VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

setDefaults()


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-sql STATIC
        gen-src/normal/sql/NormalSQLVisitor.h
        gen-src/normal/sql/NormalSQLVisitor.cpp)

target_compile_features(normal-sql PRIVATE cxx_std_17)
target_compile_options(normal-sql PRIVATE -Wall -Wextra -pedantic)
set_target_properties(normal-sql PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(normal-sql PUBLIC gen-src)

target_link_libraries(normal-sql PUBLIC antlr4_shared)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
