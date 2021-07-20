project(normal-connector VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-connector STATIC
        include/normal/connector/Globals.h
        src/Connector.cpp include/normal/connector/Connector.h
        src/CatalogueEntry.cpp include/normal/connector/CatalogueEntry.h
        src/Catalogue.cpp include/normal/connector/Catalogue.h
        src/s3/S3SelectConnector.cpp include/normal/connector/s3/S3SelectConnector.h
        src/s3/S3SelectCatalogueEntry.cpp include/normal/connector/s3/S3SelectCatalogueEntry.h
        src/local-fs/LocalFileSystemCatalogueEntry.cpp include/normal/connector/local-fs/LocalFileSystemCatalogueEntry.h
        src/local-fs/LocalFileSystemConnector.cpp include/normal/connector/local-fs/LocalFileSystemConnector.h
        src/local-fs/LocalFilePartitioningScheme.cpp include/normal/connector/local-fs/LocalFilePartitioningScheme.h
        src/local-fs/LocalFileExplicitPartitioningScheme.cpp include/normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h
        src/local-fs/LocalFilePartition.cpp include/normal/connector/local-fs/LocalFilePartition.h
        src/partition/PartitioningScheme.cpp include/normal/connector/partition/PartitioningScheme.h
        src/partition/Partition.cpp include/normal/connector/partition/Partition.h
        src/s3/S3SelectPartition.cpp include/normal/connector/s3/S3SelectPartition.h
        src/s3/S3SelectPartitioningScheme.cpp include/normal/connector/s3/S3SelectPartitioningScheme.h
        src/s3/S3SelectExplicitPartitioningScheme.cpp include/normal/connector/s3/S3SelectExplicitPartitioningScheme.h
        src/s3/S3Util.cpp include/normal/connector/s3/S3Util.h
        src/MiniCatalogue.cpp include/normal/connector/MiniCatalogue.h)

target_include_directories(normal-connector PUBLIC include)

target_link_libraries(normal-connector PRIVATE spdlog::spdlog)
target_link_libraries(normal-connector PRIVATE fmt::fmt)
target_link_libraries(normal-connector PUBLIC normal-plan)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
