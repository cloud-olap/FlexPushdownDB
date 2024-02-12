//
// Created by Yifei Yang on 11/9/21.
//

#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntryReader.h>
#include <fpdb/catalogue/obj-store/s3/S3Connector.h>
#include <fpdb/store/server/file/RemoteFileReaderBuilder.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/parquet/ParquetFormat.h>
#include <fpdb/tuple/ColumnName.h>
#include <fpdb/aws/S3Util.h>
#include <fpdb/util/Util.h>
#include <nlohmann/json.hpp>

using namespace fpdb::store::server::file;
using namespace fpdb::aws;
using namespace fpdb::tuple;
using namespace fpdb::util;

namespace fpdb::catalogue::obj_store {

shared_ptr<ObjStoreCatalogueEntry>
ObjStoreCatalogueEntryReader::readCatalogueEntry(const shared_ptr<Catalogue> &catalogue,
                                                 const string &bucket,
                                                 const string &schemaName,
                                                 const shared_ptr<ObjStoreConnector> &objStoreConnector) {
  // read catalogue entry
  auto objStoreType = objStoreConnector->getStoreType();
  auto catalogueEntry = readCatalogueEntryNoPartitionSize(objStoreType, catalogue, bucket, schemaName);

  // read partition size
  switch (objStoreConnector->getStoreType()) {
    case ObjStoreType::S3: {
      auto s3Connector = static_pointer_cast<S3Connector>(objStoreConnector);
      readS3PartitionSize(catalogueEntry, s3Connector->getAwsClient()->getS3Client());
      break;
    }
    case ObjStoreType::FPDB_STORE: {
      auto fpdbStoreConnector = static_pointer_cast<FPDBStoreConnector>(objStoreConnector);
      readFPDBStoreObjectMap(catalogueEntry, schemaName);
      readFPDBStorePartitionSize(catalogueEntry, fpdbStoreConnector);
      break;
    }
    default: {
      throw runtime_error("Unknown object store type");
    }
  }

  return catalogueEntry;
}

shared_ptr<ObjStoreCatalogueEntry>
ObjStoreCatalogueEntryReader::readCatalogueEntryNoPartitionSize(ObjStoreType storeType,
                                                                const shared_ptr<Catalogue> &catalogue,
                                                                const string &bucket,
                                                                const string &schemaName) {
  // read metadata files
  filesystem::path metadataPath = catalogue->getMetadataPath().append(schemaName);
  if (!filesystem::exists(metadataPath)) {
    throw runtime_error(fmt::format("Metadata not found for schema: {}", schemaName));
  }
  auto schemaJObj = json::parse(readFile(metadataPath.append("schema.json")));
  metadataPath = metadataPath.parent_path();
  auto statsJObj = json::parse(readFile(metadataPath.append("stats.json")));
  metadataPath = metadataPath.parent_path();
  auto zoneMapJObj = json::parse(readFile(metadataPath.append("zoneMap.json")));

  // get all table names
  unordered_set<string> tableNames;
  for (const auto &tableSchemaJObj: schemaJObj["tables"].get<vector<json>>()) {
    tableNames.emplace(tableSchemaJObj["name"].get<string>());
  }

  // member variables to make ObjStoreCatalogueEntry and S3Table
  unordered_map<string, shared_ptr<FileFormat>> formatMap;
  unordered_map<string, shared_ptr<arrow::Schema>> schemaMap;
  unordered_map<string, unordered_map<string, int>> apxColumnLengthMapMap;
  unordered_map<string, int> apxRowLengthMap;
  unordered_map<string, unordered_set<string>> zoneMapColumnNamesMap;
  unordered_map<string, vector<shared_ptr<ObjStorePartition>>> partitionsMap;

  // read schema
  readSchema(schemaJObj, bucket, schemaName, schemaMap, formatMap, partitionsMap);

  // read stats
  readStats(statsJObj, apxColumnLengthMapMap, apxRowLengthMap);

  // read zoneMap
  readZoneMap(zoneMapJObj, schemaMap, partitionsMap, zoneMapColumnNamesMap);

  // create an ObjStoreCatalogueEntry
  shared_ptr<ObjStoreCatalogueEntry> catalogueEntry = make_shared<ObjStoreCatalogueEntry>(storeType,
                                                                                          schemaName,
                                                                                          bucket,
                                                                                          catalogue);

  // make S3Tables
  for (const auto &tableName: tableNames) {
    shared_ptr<ObjStoreTable> table = make_shared<ObjStoreTable>(tableName,
                                                                 schemaMap.find(tableName)->second,
                                                                 formatMap.find(tableName)->second,
                                                                 apxColumnLengthMapMap.find(tableName)->second,
                                                                 apxRowLengthMap.find(tableName)->second,
                                                                 zoneMapColumnNamesMap.find(tableName)->second,
                                                                 partitionsMap.find(tableName)->second);
    catalogueEntry->addTable(table);
  }

  return catalogueEntry;
}

void ObjStoreCatalogueEntryReader::readSchema(
        const json &schemaJObj,
        const string &bucket,
        const string &schemaName,
        unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
        unordered_map<string, shared_ptr<FileFormat>> &formatMap,
        unordered_map<string, vector<shared_ptr<ObjStorePartition>>> &partitionsMap) {
  // read schemas, formats and ObjStorePartitions
  for (const auto &tableSchemasJObj: schemaJObj["tables"].get<vector<json>>()) {
    const string &tableName = tableSchemasJObj["name"].get<string>();

    // formats
    const auto &formatJObj = tableSchemasJObj["format"];
    const string &formatStr = formatJObj["name"].get<string>();
    string s3ObjectSuffix;
    if (formatStr == "csv") {
      char fieldDelimiter = formatJObj["fieldDelimiter"].get<string>().c_str()[0];
      formatMap.emplace(tableName, make_shared<csv::CSVFormat>(fieldDelimiter));
      s3ObjectSuffix = ".tbl";
    } else if (formatStr == "parquet") {
      formatMap.emplace(tableName, make_shared<parquet::ParquetFormat>());
      s3ObjectSuffix = ".parquet";
    } else {
      throw runtime_error(fmt::format("Unsupported data format: {}", formatStr));
    }

    // fields
    vector<shared_ptr<arrow::Field>> fields;
    for (const auto &fieldJObj: tableSchemasJObj["fields"].get<vector<json>>()) {
      const string &fieldName = ColumnName::canonicalize(fieldJObj["name"].get<string>());
      const string &fieldTypeStr = fieldJObj["type"].get<string>();
      const auto &fieldType = strToDataType(fieldTypeStr);
      auto field = make_shared<arrow::Field>(fieldName, fieldType);
      fields.emplace_back(field);
    }
    auto schema = make_shared<arrow::Schema>(fields);
    schemaMap.emplace(tableName, schema);

    // partitions
    vector<shared_ptr<ObjStorePartition>> ObjStorePartitions;
    int numPartitions = tableSchemasJObj["numPartitions"].get<int>();
    if (numPartitions == 1) {
      string s3Object = fmt::format("{}{}{}", schemaName, tableName, s3ObjectSuffix);
      ObjStorePartitions.emplace_back(make_shared<ObjStorePartition>(bucket, s3Object));
    } else {
      string s3ObjectDir = schemaName + tableName + "_sharded/";
      for (int i = 0; i < numPartitions; ++i) {
        string s3Object = fmt::format("{}{}{}.{}", s3ObjectDir, tableName, s3ObjectSuffix, to_string(i));
        ObjStorePartitions.emplace_back(make_shared<ObjStorePartition>(bucket, s3Object));
      }
    }
    partitionsMap.emplace(tableName, ObjStorePartitions);
  }
}

void ObjStoreCatalogueEntryReader::readStats(const json &statsJObj,
                                             unordered_map<string, unordered_map<string, int>> &apxColumnLengthMapMap,
                                             unordered_map<string, int> &apxRowLengthMap) {
  for (const auto &tableStatsJObj: statsJObj["tables"].get<vector<json>>()) {
    const string &tableName = tableStatsJObj["name"].get<string>();
    unordered_map<string, int> apxColumnLengthMap;
    int apxRowLength = 0;
    for (const auto &apxColumnLengthIt: tableStatsJObj["stats"]["apxColumnLength"].get<unordered_map<string, int>>()) {
      string columnName = ColumnName::canonicalize(apxColumnLengthIt.first);
      int apxColumnLength = apxColumnLengthIt.second;
      apxColumnLengthMap.emplace(columnName, apxColumnLength);
      apxRowLength += apxColumnLength;
    }
    apxColumnLengthMapMap.emplace(tableName, apxColumnLengthMap);
    apxRowLengthMap.emplace(tableName, apxRowLength);
  }
}

void ObjStoreCatalogueEntryReader::readZoneMap(const json &zoneMapJObj,
                                               const unordered_map<string, shared_ptr<arrow::Schema>> &schemaMap,
                                               unordered_map<string, vector<shared_ptr<ObjStorePartition>>> &partitionsMap,
                                               unordered_map<string, unordered_set<string>> &zoneMapColumnNamesMap
        ) {
  // read zoneMaps
  for (const auto &tableZoneMapsJObj: zoneMapJObj["tables"].get<vector<json>>()) {
    const string &tableName = tableZoneMapsJObj["name"].get<string>();
    const shared_ptr<arrow::Schema> &schema = schemaMap.find(tableName)->second;
    const vector<shared_ptr<ObjStorePartition>> &partitions = partitionsMap.find(tableName)->second;

    unordered_set<string> zoneMapColumnNames;
    for (const auto &tableZoneMapJObj: tableZoneMapsJObj["zoneMap"].get<vector<json>>()) {
      const string &fieldName = ColumnName::canonicalize(tableZoneMapJObj["field"].get<string>());
      zoneMapColumnNames.emplace(fieldName);
      const shared_ptr<arrow::DataType> fieldType = schema->GetFieldByName(fieldName)->type();
      const auto valuePairsJArr = tableZoneMapJObj["valuePairs"].get<vector<json>>();

      for (size_t i = 0; i < partitions.size(); ++i) {
        const auto &partition = partitions[i];
        const auto valuePairJObj = valuePairsJArr[i];
        partition->addMinMax(fieldName, jsonToMinMaxLiterals(valuePairJObj, fieldType));
      }
    }
    zoneMapColumnNamesMap.emplace(tableName, zoneMapColumnNames);
  }

  // fill zoneMapColumnNamesMap for no-zoneMap tables
  for (const auto &it: schemaMap) {
    const string &tableName = it.first;
    if (zoneMapColumnNamesMap.find(tableName) == zoneMapColumnNamesMap.end()) {
      zoneMapColumnNamesMap.emplace(tableName, unordered_set<string>{});
    }
  }
}

void ObjStoreCatalogueEntryReader::readS3PartitionSize(const shared_ptr<ObjStoreCatalogueEntry> &catalogueEntry,
                                                       const shared_ptr<S3Client> &s3Client) {
  // collect a map of s3Object -> partition
  vector<shared_ptr<ObjStorePartition>> allPartitions;
  for (const auto &table: catalogueEntry->getTables()) {
    auto partitions = table->getObjStorePartitions();
    allPartitions.insert(allPartitions.end(), partitions.begin(), partitions.end());
  }

  vector<string> allS3Objects;
  unordered_map<string, shared_ptr<ObjStorePartition>> s3ObjectPartitionMap;
  for (const auto &partition: allPartitions) {
    const auto &s3Object = partition->getObject();
    allS3Objects.emplace_back(s3Object);
    s3ObjectPartitionMap.emplace(s3Object, partition);
  }

  // list objects
  auto s3ObjectSizeMap = S3Util::listObjects(catalogueEntry->getBucket(),
                                             catalogueEntry->getSchemaName(),
                                             allS3Objects,
                                             s3Client);

  // set partition size
  for (const auto &it: s3ObjectSizeMap) {
    const auto &s3Object = it.first;
    long objectSize = it.second;
    const auto &partition = s3ObjectPartitionMap.find(s3Object)->second;
    partition->setNumBytes(objectSize);
  }
}

void ObjStoreCatalogueEntryReader::readFPDBStoreObjectMap(const shared_ptr<ObjStoreCatalogueEntry> &catalogueEntry,
                                                          const string &schemaName) {
  // read object map
  auto metadataPath = catalogueEntry->getCatalogue().lock()->getMetadataPath().append(schemaName);
  if (!filesystem::exists(metadataPath)) {
    throw runtime_error(fmt::format("Metadata not found for schema: {}", schemaName));
  }
  unordered_map<string, int> objectMap;
  auto objectMapJObj = json::parse(readFile(metadataPath.append("fpdb_store_object_map.json")));
  for (const auto &objectEntryJObj: objectMapJObj["objectMap"].get<vector<json>>()) {
    auto object = schemaName + objectEntryJObj["object"].get<string>();
    auto nodeId = objectEntryJObj["nodeId"].get<int>();
    objectMap.emplace(object, nodeId);
  }

  // set node id for each object
  for (const auto &table: catalogueEntry->getTables()) {
    for (const auto &partition: table->getObjStorePartitions()) {
      partition->setNodeId(objectMap.find(partition->getObject())->second);
    }
  }
}

void ObjStoreCatalogueEntryReader::readFPDBStorePartitionSize(const shared_ptr<ObjStoreCatalogueEntry> &catalogueEntry,
                                                              const shared_ptr<FPDBStoreConnector> &connector) {
  int port = connector->getFileServicePort();
  for (const auto &table: catalogueEntry->getTables()) {
    for (const auto &partition: table->getObjStorePartitions()) {
      auto nodeId = partition->getNodeId();
      if (!nodeId.has_value()) {
        throw runtime_error(fmt::format("Node id not set for FPDB store object: {}", partition->getObject()));
      }
      auto reader = RemoteFileReaderBuilder::make(table->getFormat(),
                                                  table->getSchema(),
                                                  partition->getBucket(),
                                                  partition->getObject(),
                                                  connector->getHost(*nodeId),
                                                  port);
      auto expPartitionSize = reader->getFileSize();
      if (!expPartitionSize.has_value()) {
        throw runtime_error(fmt::format("Error when reading FPDB store catalogue entry: {}", expPartitionSize.error()));
      }
      partition->setNumBytes(*expPartitionSize);
    }
  }
}

shared_ptr<arrow::DataType> ObjStoreCatalogueEntryReader::strToDataType(const string &str) {
  if (str == "int32" || str == "int") {
    return arrow::int32();
  } else if (str == "int64" || str == "long") {
    return arrow::int64();
  } else if (str == "float64" || str == "double") {
    return arrow::float64();
  } else if (str == "utf8" || str == "string") {
    return arrow::utf8();
  } else if (str == "boolean" || str == "bool") {
    return arrow::boolean();
  } else if (str == "date") {
    return arrow::date64();
  } else {
    throw runtime_error(fmt::format("Unsupported data type: {}", str));
  }
}

pair<shared_ptr<Scalar>, shared_ptr<Scalar>>
ObjStoreCatalogueEntryReader::jsonToMinMaxLiterals(const json &jObj, const shared_ptr<arrow::DataType> &datatype) {
  if (datatype->id() == arrow::int32()->id()) {
    return make_pair(Scalar::make(arrow::MakeScalar(arrow::int32(), jObj["min"].get<int>()).ValueOrDie()),
                     Scalar::make(arrow::MakeScalar(arrow::int32(), jObj["max"].get<int>()).ValueOrDie()));
  } else if (datatype->id() == arrow::int64()->id()) {
    return make_pair(Scalar::make(arrow::MakeScalar(arrow::int64(), jObj["min"].get<long>()).ValueOrDie()),
                     Scalar::make(arrow::MakeScalar(arrow::int64(), jObj["max"].get<long>()).ValueOrDie()));
  } else if (datatype->id() == arrow::float64()->id()) {
    return make_pair(Scalar::make(arrow::MakeScalar(arrow::float64(), jObj["min"].get<double>()).ValueOrDie()),
                     Scalar::make(arrow::MakeScalar(arrow::float64(), jObj["max"].get<double>()).ValueOrDie()));
  } else if (datatype->id() == arrow::utf8()->id()) {
    return make_pair(Scalar::make(arrow::MakeScalar(arrow::utf8(), jObj["min"].get<string>()).ValueOrDie()),
                     Scalar::make(arrow::MakeScalar(arrow::utf8(), jObj["max"].get<string>()).ValueOrDie()));
  } else {
    throw runtime_error(fmt::format("Unsupported data type: {}", datatype->name()));
  }
}

}
