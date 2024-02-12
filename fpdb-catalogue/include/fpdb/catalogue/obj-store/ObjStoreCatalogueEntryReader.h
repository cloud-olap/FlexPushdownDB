//
// Created by Yifei Yang on 11/9/21.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECATALOGUEENTRYREADER_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECATALOGUEENTRYREADER_H

#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <fpdb/catalogue/Catalogue.h>
#include <fpdb/tuple/FileFormat.h>
#include <fpdb/tuple/Scalar.h>
#include <nlohmann/json.hpp>
#include <aws/s3/S3Client.h>

using namespace fpdb::tuple;
using namespace Aws::S3;
using namespace std;
using json = nlohmann::json;

namespace fpdb::catalogue::obj_store {

class ObjStoreCatalogueEntryReader {

public:
  static shared_ptr<ObjStoreCatalogueEntry> readCatalogueEntry(const shared_ptr<Catalogue> &catalogue,
                                                               const string &bucket,
                                                               const string &schemaName,
                                                               const shared_ptr<ObjStoreConnector> &objStoreConnector);

private:
  static shared_ptr<ObjStoreCatalogueEntry> readCatalogueEntryNoPartitionSize(ObjStoreType storeType,
                                                                              const shared_ptr<Catalogue> &catalogue,
                                                                              const string &bucket,
                                                                              const string &schemaName);

  static void readSchema(const json &schemaJObj,
                         const string &bucket,
                         const string &schemaName,
                         unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
                         unordered_map<string, shared_ptr<FileFormat>> &formatMap,
                         unordered_map<string, vector<shared_ptr<ObjStorePartition>>> &partitionMap);

  static void readStats(const json &statsJObj,
                        unordered_map<string, unordered_map<string, int>> &apxColumnLengthMapMap,
                        unordered_map<string, int> &apxRowLengthMap);

  static void readZoneMap(const json &zoneMapJObj,
                          const unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
                          unordered_map<string, vector<shared_ptr<ObjStorePartition>>> &partitionMap,
                          unordered_map<string, unordered_set<string>> &zoneMapColumnNamesMap);

  static void readS3PartitionSize(const shared_ptr<ObjStoreCatalogueEntry> &catalogueEntry,
                                  const shared_ptr<S3Client> &s3Client);

  static void readFPDBStoreObjectMap(const shared_ptr<ObjStoreCatalogueEntry> &catalogueEntry,
                                     const string &schemaName);

  static void readFPDBStorePartitionSize(const shared_ptr<ObjStoreCatalogueEntry> &catalogueEntry,
                                         const shared_ptr<FPDBStoreConnector> &connector);

  static shared_ptr<arrow::DataType> strToDataType(const string &str);

  static pair<shared_ptr<Scalar>, shared_ptr<Scalar>>
    jsonToMinMaxLiterals(const json &jObj, const shared_ptr<arrow::DataType> &datatype);
};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECATALOGUEENTRYREADER_H
