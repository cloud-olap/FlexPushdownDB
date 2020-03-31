project(normal-sql VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-sql STATIC
        gen-src/normal/sql/NormalSQLVisitor.h
        gen-src/normal/sql/NormalSQLVisitor.cpp)

target_include_directories(normal-sql PUBLIC gen-src)

target_link_libraries(normal-sql PUBLIC antlr4_shared)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
