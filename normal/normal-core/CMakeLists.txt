project("normal-core" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-core STATIC
        src/message/StartMessage.cpp include/normal/core/message/StartMessage.h
        src/message/Envelope.cpp include/normal/core/message/Envelope.h
        include/normal/core/Globals.h
        src/message/CompleteMessage.cpp include/normal/core/message/CompleteMessage.h
        src/Normal.cpp include/normal/core/Normal.h
        src/OperatorActor.cpp include/normal/core/OperatorActor.h
        src/Operator.cpp include/normal/core/Operator.h
        src/OperatorContext.cpp include/normal/core/OperatorContext.h
        src/OperatorManager.cpp include/normal/core/OperatorManager.h
        src/message/Message.cpp include/normal/core/message/Message.h
        src/message/TupleMessage.cpp include/normal/core/message/TupleMessage.h
        src/TupleSet.cpp include/normal/core/TupleSet.h
        src/Cache.cpp include/normal/core/Cache.h
        src/Actors.cpp include/normal/core/Actors.h
        src/OperatorDirectory.cpp include/normal/core/OperatorDirectory.h
        src/OperatorDirectoryEntry.cpp include/normal/core/OperatorDirectoryEntry.h
        src/OperatorRelationshipType.cpp include/normal/core/OperatorRelationshipType.h
        src/LocalOperatorDirectory.cpp include/normal/core/LocalOperatorDirectory.h
        src/LocalOperatorDirectoryEntry.cpp include/normal/core/LocalOperatorDirectoryEntry.h
        include/normal/core/expression/Expression.h
        include/normal/core/expression/Add.h
        include/normal/core/expression/Literal.h
        include/normal/core/expression/Multiply.h
        include/normal/core/expression/Subtract.h
        include/normal/core/expression/Divide.h
        include/normal/core/expression/Column.h
        src/arrow/Arrays.cpp include/normal/core/arrow/Arrays.h
        include/normal/core/expression/Cast.h
        include/normal/core/type/Type.h
        include/normal/core/type/DecimalType.h
        src/type/Types.cpp include/normal/core/type/Types.h
        include/normal/core/type/Float64Type.h
        src/expression/Expressions.cpp
        include/normal/core/expression/Expressions.h src/Globals.h)


target_include_directories(normal-core PUBLIC include)

target_link_libraries(normal-core PRIVATE LLVM-filesystem)
target_link_libraries(normal-core PRIVATE spdlog::spdlog)
target_link_libraries(normal-core PUBLIC caf::libcaf_core_shared)
target_link_libraries(normal-core PUBLIC caf::libcaf_io_shared)
target_link_libraries(normal-core PUBLIC arrow_static)
target_link_libraries(normal-core PUBLIC gandiva_static)
target_link_libraries(normal-core PUBLIC graphviz_gvc_shared)
target_link_libraries(normal-core PUBLIC graphviz_dot_layout_shared)
target_link_libraries(normal-core PUBLIC graphviz_neato_layout_shared)
target_link_libraries(normal-core PUBLIC expected)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps(normal-core)
