project(normal-plan VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-plan STATIC
        include/normal/plan/Globals.h
        src/operator_/ScanLogicalOperator.cpp include/normal/plan/operator_/ScanLogicalOperator.h
        src/operator_/LogicalOperator.cpp include/normal/plan/operator_/LogicalOperator.h
        src/operator_/CollateLogicalOperator.cpp include/normal/plan/operator_/CollateLogicalOperator.h
        src/operator_/AggregateLogicalOperator.cpp include/normal/plan/operator_/AggregateLogicalOperator.h
        src/function/AggregateLogicalFunction.cpp include/normal/plan/function/AggregateLogicalFunction.h
        src/function/SumLogicalFunction.cpp include/normal/plan/function/SumLogicalFunction.h
        src/operator_/FileScanLogicalOperator.cpp include/normal/plan/operator_/FileScanLogicalOperator.h
        src/operator_/S3SelectScanLogicalOperator.cpp include/normal/plan/operator_/S3SelectScanLogicalOperator.h
        src/operator_/ProjectLogicalOperator.cpp include/normal/plan/operator_/ProjectLogicalOperator.h
        src/LogicalPlan.cpp include/normal/plan/LogicalPlan.h
        src/Planner.cpp include/normal/plan/Planner.h
        src/PhysicalPlan.cpp include/normal/plan/PhysicalPlan.h
        src/operator_/type/OperatorType.cpp include/normal/plan/operator_/type/OperatorType.h
        src/operator_/type/ScanOperatorType.cpp include/normal/plan/operator_/type/ScanOperatorType.h
        include/normal/plan/operator_/type/OperatorTypeId.h
        src/operator_/type/OperatorTypes.cpp include/normal/plan/operator_/type/OperatorTypes.h
        src/operator_/type/ProjectOperatorType.cpp include/normal/plan/operator_/type/ProjectOperatorType.h
        src/operator_/type/AggregateOperatorType.cpp include/normal/plan/operator_/type/AggregateOperatorType.h
        src/operator_/type/CollateOperatorType.cpp include/normal/plan/operator_/type/CollateOperatorType.h)

target_include_directories(normal-plan PUBLIC include)

target_link_libraries(normal-plan PRIVATE spdlog::spdlog)
target_link_libraries(normal-plan PRIVATE fmt::fmt)
target_link_libraries(normal-plan PUBLIC normal-pushdown)
target_link_libraries(normal-plan PUBLIC normal-connector)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
