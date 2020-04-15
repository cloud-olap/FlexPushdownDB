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
        src/visitor/Visitor.cpp src/visitor/Visitor.h
        src/logical/ScanLogicalOperator.cpp include/normal/sql/logical/ScanLogicalOperator.h
        src/logical/LogicalOperator.cpp include/normal/sql/logical/LogicalOperator.h
        src/logical/CollateLogicalOperator.cpp include/normal/sql/logical/CollateLogicalOperator.h
        src/logical/AggregateLogicalOperator.cpp include/normal/sql/logical/AggregateLogicalOperator.h
        src/logical/AggregateLogicalFunction.cpp include/normal/sql/logical/AggregateLogicalFunction.h
        src/logical/SumLogicalFunction.cpp include/normal/sql/logical/SumLogicalFunction.h
        src/logical/FileScanLogicalOperator.cpp include/normal/sql/logical/FileScanLogicalOperator.h
        src/logical/S3SelectScanLogicalOperator.cpp include/normal/sql/logical/S3SelectScanLogicalOperator.h
        src/logical/ProjectLogicalOperator.cpp include/normal/sql/logical/ProjectLogicalOperator.h)

target_include_directories(normal-sql PUBLIC include)
target_include_directories(normal-sql PUBLIC gen-src)

target_link_libraries(normal-sql PUBLIC normal-core)
target_link_libraries(normal-sql PUBLIC normal-pushdown)
target_link_libraries(normal-sql PUBLIC normal-connector)
target_link_libraries(normal-sql PRIVATE spdlog::spdlog)
target_link_libraries(normal-sql PRIVATE antlr4_static)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
