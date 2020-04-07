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
        src/Globals.h
        src/Interpreter.cpp include/Interpreter.h
        src/Listener.cpp src/Listener.h
        src/connector/Connector.cpp include/connector/Connector.h
        src/connector/CatalogueEntry.cpp include/connector/CatalogueEntry.h
        src/connector/Catalogue.cpp include/connector/Catalogue.h
        src/connector/s3/S3SelectConnector.cpp include/connector/s3/S3SelectConnector.h
        src/connector/s3/S3SelectCatalogueEntry.cpp include/connector/s3/S3SelectCatalogueEntry.h
        src/connector/local-fs/LocalFileSystemCatalogueEntry.cpp include/connector/local-fs/LocalFileSystemCatalogueEntry.h
        src/connector/local-fs/LocalFileSystemConnector.cpp include/connector/local-fs/LocalFileSystemConnector.h
        src/ast/ScanNode.cpp include/logical/ScanNode.h src/ast/Symbols.cpp src/ast/Symbols.h src/ast/ASTNode.cpp src/ast/ASTNode.h src/ast/CollateNode.cpp src/ast/CollateNode.h src/ast/AggregateNode.cpp src/ast/AggregateNode.h src/ast/AggregateExpression.cpp src/ast/AggregateExpression.h src/ast/AggregateFunction.cpp src/ast/AggregateFunction.h src/ast/SumASTFunction.cpp src/ast/SumASTFunction.h src/ast/FileScanLogicalOperator.cpp src/ast/FileScanLogicalOperator.h src/ast/S3SelectScanLogicalOperator.cpp src/ast/S3SelectScanLogicalOperator.h)

target_include_directories(normal-sql PUBLIC include)
target_include_directories(normal-sql PUBLIC gen-src)

target_link_libraries(normal-sql PUBLIC normal-core)
target_link_libraries(normal-sql PUBLIC normal-pushdown)
target_link_libraries(normal-sql PRIVATE spdlog::spdlog)
target_link_libraries(normal-sql PRIVATE antlr4_static)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
