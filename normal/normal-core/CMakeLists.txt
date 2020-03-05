project("normal-core" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

setDefaults()

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library("normal-core" STATIC src/StartMessage.cpp include/normal/core/StartMessage.h src/OperatorMeta.cpp include/normal/core/OperatorMeta.h src/Envelope.cpp include/normal/core/Envelope.h include/normal/core/Globals.h src/CompleteMessage.cpp include/normal/core/CompleteMessage.h)

target_sources("normal-core" PRIVATE
        src/Normal.cpp include/normal/core/Normal.h
        src/OperatorActor.cpp include/normal/core/OperatorActor.h
        src/Operator.cpp include/normal/core/Operator.h
        src/OperatorContext.cpp include/normal/core/OperatorContext.h
        src/OperatorManager.cpp include/normal/core/OperatorManager.h
        src/Message.cpp include/normal/core/Message.h
        src/TupleMessage.cpp include/normal/core/TupleMessage.h
        src/TupleSet.cpp include/normal/core/TupleSet.h)

target_compile_features("normal-core" PRIVATE cxx_std_17)
target_compile_options("normal-core" PRIVATE -Wall -Wextra -pedantic)
set_target_properties("normal-core" PROPERTIES LINKER_LANGUAGE CXX)


target_include_directories("normal-core" PUBLIC include)

target_link_libraries("normal-core" PRIVATE spdlog)
target_link_libraries("normal-core" PRIVATE arrow)
target_link_libraries("normal-core" PRIVATE arrow-dataset)
target_link_libraries("normal-core" PUBLIC caf-core)
target_link_libraries("normal-core" PUBLIC caf-io)


#-----------------------------------------------------------------------------------------------------------------------
# Dependency graph
#-----------------------------------------------------------------------------------------------------------------------

add_dependencies("normal-core" "spdlog")
add_dependencies("normal-core" "arrow")
add_dependencies("normal-core" "arrow-dataset")
add_dependencies("normal-core" "caf-core")
add_dependencies("normal-core" "caf-io")


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps("normal-core")
