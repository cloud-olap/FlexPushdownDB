project(fpdb-catalogue VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-catalogue STATIC
        include/fpdb/catalogue/Globals.h
        include/fpdb/catalogue/CatalogueEntryType.h
        src/Catalogue.cpp include/fpdb/catalogue/Catalogue.h
        src/CatalogueEntry.cpp include/fpdb/catalogue/CatalogueEntry.h
        src/Table.cpp include/fpdb/catalogue/Table.h
        src/Partition.cpp include/fpdb/catalogue/Partition.h
        src/local-fs/LocalFSCatalogueEntry.cpp include/fpdb/catalogue/local-fs/LocalFSCatalogueEntry.h
        src/local-fs/LocalFSTable.cpp include/fpdb/catalogue/local-fs/LocalFSTable.h
        src/local-fs/LocalFSPartition.cpp include/fpdb/catalogue/local-fs/LocalFSPartition.h
        include/fpdb/catalogue/obj-store/ObjStoreType.h
        src/obj-store/ObjStoreCatalogueEntry.cpp include/fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h
        src/obj-store/ObjStoreTable.cpp include/fpdb/catalogue/obj-store/ObjStoreTable.h
        src/obj-store/ObjStorePartition.cpp include/fpdb/catalogue/obj-store/ObjStorePartition.h
        src/obj-store/ObjStoreCatalogueEntryReader.cpp include/fpdb/catalogue/obj-store/ObjStoreCatalogueEntryReader.h
        src/obj-store/ObjStoreConnector.cpp include/fpdb/catalogue/obj-store/ObjStoreConnector.h
        src/obj-store/s3/S3Connector.cpp include/fpdb/catalogue/obj-store/s3/S3Connector.h
        src/obj-store/fpdb-store/FPDBStoreConnector.cpp include/fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h
        include/fpdb/catalogue/caf-serialization/CAFPartitionSerializer.h
        include/fpdb/catalogue/caf-serialization/CAFTableSerializer.h
        include/fpdb/catalogue/caf-serialization/CAFObjStoreConnectorSerializer.h)

target_include_directories(fpdb-catalogue PUBLIC include)

set_target_properties(fpdb-catalogue PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(fpdb-catalogue PUBLIC fpdb-tuple)
target_link_libraries(fpdb-catalogue PUBLIC fpdb-aws)
target_link_libraries(fpdb-catalogue PUBLIC fpdb-util)
target_link_libraries(fpdb-catalogue PRIVATE fpdb-store-server-file)
target_link_libraries(fpdb-catalogue PUBLIC nlohmann_json)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(fpdb-catalogue)
