cmake_policy(SET CMP0048 NEW)

project("normal-htap" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-htap STATIC
        ../normal-htap/src/antijoin/HashAntiJoinProbe.cpp ../normal-htap/include/antijoin/HashAntiJoinProbe.h
        ../normal-htap/src/antijoin/HashAntiJoinProbeKernel.cpp ../normal-htap/include/antijoin/HashAntiJoinProbeKernel.h
        ../normal-htap/src/antijoin/RecordBatchAntiJoiner.cpp ../normal-htap/include/antijoin/RecordBatchAntiJoiner.h
        ../normal-htap/src/antijoin/AntiJoinPredicate.cpp ../normal-htap/include/antijoin/AntiJoinPredicate.h
        ../normal-htap/src/deltamerge/DeltaMerge.cpp ../normal-htap/include/deltamerge/DeltaMerge.h)

target_include_directories(normal-htap PUBLIC include)