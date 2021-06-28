project(normal-sql VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-sql STATIC
        gen-src/normal/sql/NormalSQLBaseListener.cpp gen-src/normal/sql/NormalSQLBaseListener.h
        gen-src/normal/sql/NormalSQLBaseVisitor.cpp gen-src/normal/sql/NormalSQLBaseVisitor.h
        gen-src/normal/sql/NormalSQLLexer.cpp gen-src/normal/sql/NormalSQLLexer.h
        gen-src/normal/sql/NormalSQLListener.cpp gen-src/normal/sql/NormalSQLListener.h
        gen-src/normal/sql/NormalSQLParser.cpp gen-src/normal/sql/NormalSQLParser.h
        gen-src/normal/sql/NormalSQLVisitor.cpp gen-src/normal/sql/NormalSQLVisitor.h
        include/normal/sql/Globals.h
        src/Interpreter.cpp include/normal/sql/Interpreter.h
        src/visitor/Visitor.cpp include/normal/sql/visitor/Visitor.h)

target_include_directories(normal-sql PUBLIC include)
target_include_directories(normal-sql PUBLIC gen-src)

target_link_libraries(normal-sql PUBLIC normal-core)
target_link_libraries(normal-sql PUBLIC normal-expression)
target_link_libraries(normal-sql PUBLIC normal-pushdown)
target_link_libraries(normal-sql PUBLIC normal-connector)
target_link_libraries(normal-sql PUBLIC normal-plan)
target_link_libraries(normal-sql PRIVATE spdlog::spdlog)
target_link_libraries(normal-sql PRIVATE antlr4_static)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
