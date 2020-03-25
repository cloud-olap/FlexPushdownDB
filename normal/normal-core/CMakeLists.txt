project("normal-core" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


setDefaults()


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-core STATIC
        src/StartMessage.cpp include/normal/core/StartMessage.h
        src/Envelope.cpp include/normal/core/Envelope.h
        include/normal/core/Globals.h
        src/CompleteMessage.cpp include/normal/core/CompleteMessage.h
        src/Normal.cpp include/normal/core/Normal.h
        src/OperatorActor.cpp include/normal/core/OperatorActor.h
        src/Operator.cpp include/normal/core/Operator.h
        src/OperatorContext.cpp include/normal/core/OperatorContext.h
        src/OperatorManager.cpp include/normal/core/OperatorManager.h
        src/Message.cpp include/normal/core/Message.h
        src/TupleMessage.cpp include/normal/core/TupleMessage.h
        src/TupleSet.cpp include/normal/core/TupleSet.h
        src/Cache.cpp include/normal/core/Cache.h
        src/Actors.cpp include/normal/core/Actors.h
        src/OperatorDirectory.cpp include/normal/core/OperatorDirectory.h
        src/OperatorDirectoryEntry.cpp include/normal/core/OperatorDirectoryEntry.h
        src/OperatorRelationshipType.cpp include/normal/core/OperatorRelationshipType.h
        src/LocalOperatorDirectory.cpp include/normal/core/LocalOperatorDirectory.h
        src/LocalOperatorDirectoryEntry.cpp include/normal/core/LocalOperatorDirectoryEntry.h)


target_compile_features(normal-core PRIVATE cxx_std_17)
target_compile_options(normal-core PRIVATE -Wall -Wextra -pedantic)
set_target_properties(normal-core PROPERTIES LINKER_LANGUAGE CXX)


target_include_directories(normal-core PUBLIC include)

target_link_libraries(normal-core PRIVATE spdlog::spdlog)
target_link_libraries(normal-core PUBLIC caf::libcaf_core_shared)
target_link_libraries(normal-core PUBLIC caf::libcaf_io_shared)
target_link_libraries(normal-core PRIVATE arrow_static)
target_link_libraries(normal-core PRIVATE arrow_dataset_static)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-core)
