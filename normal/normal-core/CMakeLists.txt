project("normal-core" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-core STATIC
        src/message/StartMessage.cpp include/normal/core/message/StartMessage.h
        src/message/Envelope.cpp include/normal/core/message/Envelope.h
        include/normal/core/Globals.h
        src/message/CompleteMessage.cpp include/normal/core/message/CompleteMessage.h
        src/ATTIC/Normal.cpp include/normal/core/ATTIC/Normal.h
        src/OperatorActor.cpp include/normal/core/OperatorActor.h
        src/Operator.cpp include/normal/core/Operator.h
        src/OperatorContext.cpp include/normal/core/OperatorContext.h
        src/OperatorManager.cpp include/normal/core/OperatorManager.h
        src/message/Message.cpp include/normal/core/message/Message.h
        src/ATTIC/Cache.cpp include/normal/core/ATTIC/Cache.h
        src/ATTIC/Actors.cpp include/normal/core/ATTIC/Actors.h
        src/OperatorDirectory.cpp include/normal/core/OperatorDirectory.h
        src/OperatorDirectoryEntry.cpp include/normal/core/OperatorDirectoryEntry.h
        src/OperatorRelationshipType.cpp include/normal/core/OperatorRelationshipType.h
        src/LocalOperatorDirectory.cpp include/normal/core/LocalOperatorDirectory.h
        src/LocalOperatorDirectoryEntry.cpp include/normal/core/LocalOperatorDirectoryEntry.h
        include/normal/core/type/Type.h
        src/type/DecimalType.cpp include/normal/core/type/DecimalType.h
        src/type/Types.cpp include/normal/core/type/Types.h
        src/type/Float64Type.cpp include/normal/core/type/Float64Type.h
        src/type/Integer32Type.cpp include/normal/core/type/Integer32Type.h
        src/type/Integer64Type.cpp include/normal/core/type/Integer64Type.h
        src/cache/LoadRequestMessage.cpp include/normal/core/cache/LoadRequestMessage.h
        src/cache/StoreRequestMessage.cpp include/normal/core/cache/StoreRequestMessage.h
        src/cache/LoadResponseMessage.cpp include/normal/core/cache/LoadResponseMessage.h
        src/cache/SegmentCacheActor.cpp include/normal/core/cache/SegmentCacheActor.h
        src/graph/OperatorGraph.cpp include/normal/core/graph/OperatorGraph.h
        src/cache/WeightRequestMessage.cpp include/normal/core/cache/WeightRequestMessage.h
        include/normal/core/Forward.h
        src/OperatorActor2.cpp include/normal/core/OperatorActor2.h
        src/OperatorConnection.cpp include/normal/core/OperatorConnection.h
        src/message/ConnectMessage.cpp include/normal/core/message/ConnectMessage.h
        src/cache/CacheMetricsMessage.cpp include/normal/core/cache/CacheMetricsMessage.h
        src/message/TupleMessage.cpp include/normal/core/message/TupleMessage.h)


target_include_directories(normal-core PUBLIC include)

target_link_libraries(normal-core PRIVATE LLVM-filesystem)
target_link_libraries(normal-core PRIVATE spdlog::spdlog)
target_link_libraries(normal-core PRIVATE fmt::fmt)
target_link_libraries(normal-core PUBLIC caf::libcaf_core_static)
target_link_libraries(normal-core PUBLIC caf::libcaf_io_static)
target_link_libraries(normal-core PUBLIC arrow_static)
target_link_libraries(normal-core PUBLIC gandiva_static)
target_link_libraries(normal-core PUBLIC graphviz_gvc_shared)
target_link_libraries(normal-core PUBLIC graphviz_dot_layout_shared)
target_link_libraries(normal-core PUBLIC graphviz_neato_layout_shared)
target_link_libraries(normal-core PUBLIC expected)
target_link_libraries(normal-core PUBLIC normal-expression)
target_link_libraries(normal-core PUBLIC normal-cache)
target_link_libraries(normal-core PUBLIC normal-util)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-core-test
        test/Globals.h
        test/MainTest.cpp
        test/CacheTest.cpp
        test/SampleOperator.cpp test/SampleOperator.h
        test/OperatorTest.cpp)


target_include_directories(normal-core-test PRIVATE include)

target_link_libraries(normal-core-test spdlog::spdlog)
target_link_libraries(normal-core-test doctest::doctest)
target_link_libraries(normal-core-test Backward::Backward)
target_link_libraries(normal-core-test normal-core)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-core)
