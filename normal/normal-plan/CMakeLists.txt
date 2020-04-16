project(normal-plan VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-plan STATIC
        include/normal/plan/Globals.h
        src/ScanLogicalOperator.cpp include/normal/plan/ScanLogicalOperator.h
        src/LogicalOperator.cpp include/normal/plan/LogicalOperator.h
        src/CollateLogicalOperator.cpp include/normal/plan/CollateLogicalOperator.h
        src/AggregateLogicalOperator.cpp include/normal/plan/AggregateLogicalOperator.h
        src/AggregateLogicalFunction.cpp include/normal/plan/AggregateLogicalFunction.h
        src/SumLogicalFunction.cpp include/normal/plan/SumLogicalFunction.h
        src/FileScanLogicalOperator.cpp include/normal/plan/FileScanLogicalOperator.h
        src/S3SelectScanLogicalOperator.cpp include/normal/plan/S3SelectScanLogicalOperator.h
        src/ProjectLogicalOperator.cpp include/normal/plan/ProjectLogicalOperator.h
        src/LogicalPlan.cpp include/normal/plan/LogicalPlan.h src/Planner.cpp include/normal/plan/Planner.h src/PhysicalPlan.cpp include/normal/plan/PhysicalPlan.h src/OperatorType.cpp include/normal/plan/OperatorType.h src/ScanOperatorType.cpp include/normal/plan/ScanOperatorType.h include/normal/plan/OperatorTypeId.h src/OperatorTypes.cpp include/normal/plan/OperatorTypes.h src/ProjectOperatorType.cpp include/normal/plan/ProjectOperatorType.h src/AggregateOperatorType.cpp include/normal/plan/AggregateOperatorType.h src/CollateOperatorType.cpp include/normal/plan/CollateOperatorType.h)

target_include_directories(normal-plan PUBLIC include)

target_link_libraries(normal-plan PUBLIC normal-core)
target_link_libraries(normal-plan PUBLIC normal-pushdown)
target_link_libraries(normal-plan PUBLIC normal-connector)
target_link_libraries(normal-plan PRIVATE spdlog::spdlog)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
