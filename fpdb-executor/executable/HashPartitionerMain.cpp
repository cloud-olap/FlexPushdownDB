//
// Created by Yifei Yang on 10/2/22.
//

#include <fpdb/executor/physical/shuffle/ShuffleKernel2.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/aws/AWSClient.h>
#include <fpdb/util/Util.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <unordered_map>
#include <stdio.h>
#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace fpdb::executor::physical::shuffle;
using namespace fpdb::tuple;
using namespace fpdb::aws;
using namespace fpdb::util;

class HashPartitioner {

public:
  HashPartitioner(const std::string &schema, const std::string &newSchema, const std::string &table,
                  int numPartitions, int numNodes, int64_t numRowsPerPart,
                  bool recreateMetadataFiles, const std::optional<std::string> &key):
    schema_(schema), newSchema_(newSchema), table_(table),
    numPartitions_(numPartitions), numNodes_(numNodes), numRowsPerPart_(numRowsPerPart),
    recreateMetadataFiles_(recreateMetadataFiles), key_(key),
    availSlots_(std::thread::hardware_concurrency()) {
    // Extract 'schemaWoFormat_', format_' and 'suffix_' from "schema_"
    auto schemaWoLastSlash = schema_.substr(0, schema_.length() - 1);
    auto pos = schemaWoLastSlash.rfind('/');
    auto schemaWoFormat = schemaWoLastSlash.substr(0, pos);
    format_ = schemaWoLastSlash.substr(pos + 1);
    if (format_.substr(0, 7) == "parquet") {
      format_ = "parquet";
    } else {
      throw std::runtime_error("Currently only implemented for parquet");
    }
    suffix_ = "." + format_;
  }

  // Entry function
  void doPartition() {
    // Create log file
    log_ = fopen("fpdb-executor-hash-partitioner-exec.log", "a");
    fprintf(log_, "Partition table '%s'...\n", table_.c_str());
    fflush(log_);

    // Start s3 client
    initAwsClient();

    // Read existing objects and partition them
    readAndPartitionAll();

    // Combine and split to the desired size
    combinePartitions();

    // Update metadata (hash keys and object map)
    updateMetadata();

    // Write new partitions to S3
    writePartitions();

    // Stop s3 client
    stopAwsClient();

    // Close log file
    fprintf(log_, "Done\n\n");
    fclose(log_);
  }

private:
  void initAwsClient() {
    awsClient_ = std::make_shared<AWSClient>(std::make_shared<AWSConfig>());
    awsClient_->init();
  }

  void stopAwsClient() {
    awsClient_->shutdown();
  }

  void readAndPartitionAll() {
    fprintf(log_, "  Read original data from S3 and partition on the hash key...  ");
    fflush(log_);

    // Get all objects
    std::vector<std::string> objects;
    if (numPartitions_ == 1) {
      objects.emplace_back(schema_ + table_ + suffix_);
    } else {
      for (int i = 0; i < numPartitions_; ++i) {
        objects.emplace_back(schema_ + table_ + "_sharded/" + table_ + suffix_ + "." + std::to_string(i));
      }
    }

    // Partition in parallel
    std::vector<std::thread> thVec;
    for (const auto &object: objects) {
      std::thread th(&HashPartitioner::readAndPartition, this, std::ref(object));
      thVec.emplace_back(std::move(th));
    }
    for (auto &th: thVec) {
      th.join();
    }

    fprintf(log_, "Done\n");
    fflush(log_);
  }

  void readAndPartition(const std::string &object) {
    // limit concurrency
    {
      std::unique_lock lock(parallelMutex_);
      parallelCv_.wait(lock, [&] {
        return availSlots_ > 0;
      });
      --availSlots_;
    }

    // Read and partition
    std::vector<std::shared_ptr<TupleSet>> partitions;
    auto tupleSet = readObject(object);
    if (key_.has_value()) {
      partitions = hashPartition(tupleSet);
    } else {
      partitions = splitPartition(tupleSet);
    }

    // limit concurrency
    {
      std::unique_lock lock(parallelMutex_);
      ++availSlots_;
      parallelCv_.notify_all();
    }

    // Save
    std::unique_lock lock(mutex_);
    partitionsVec_.emplace_back(partitions);
  }

  std::shared_ptr<TupleSet> readObject(const std::string &object) {
    Aws::S3::Model::GetObjectRequest getObjectRequest;
    getObjectRequest.SetBucket("flexpushdowndb");
    getObjectRequest.SetKey(Aws::String(object));
    auto getObjectOutcome = awsClient_->getS3Client()->GetObject(getObjectRequest);
    if (!getObjectOutcome.IsSuccess()) {
      throw std::runtime_error(getObjectOutcome.GetError().GetMessage().c_str());
    }
    auto& retrievedFile = getObjectOutcome.GetResultWithOwnership().GetBody();
    std::string parquetFileString(std::istreambuf_iterator<char>(retrievedFile), {});
    auto bufferedReader = std::make_shared<arrow::io::BufferReader>(parquetFileString);

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    ::arrow::Status status = ::parquet::arrow::OpenFile(bufferedReader,
                                                        ::arrow::default_memory_pool(),
                                                        &parquetReader);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    std::shared_ptr<arrow::Table> table;
    status = parquetReader->ReadTable(&table);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    return TupleSet::make(table);
  }

  // Partition the table into 'numNodes_' pieces on the hash 'key_'
  std::vector<std::shared_ptr<TupleSet>> hashPartition(const std::shared_ptr<TupleSet> &tupleSet) {
    auto expPartitions = ShuffleKernel2::shuffle({*key_}, numNodes_, tupleSet);
    if (!expPartitions.has_value()) {
      throw std::runtime_error(expPartitions.error());
    }
    return *expPartitions;
  }

  // Partition the table into 'numNodes_' pieces evenly
  std::vector<std::shared_ptr<TupleSet>>
  splitPartition(const std::shared_ptr<TupleSet> &tupleSet) {
    // split evenly
    int64_t splitSize = tupleSet->numRows() / numNodes_;
    std::vector<arrow::ChunkedArrayVector> splitColumns{(size_t) numNodes_};
    for (const auto &inputColumn: tupleSet->table()->columns()) {
      for (int i = 0; i < numNodes_; ++i) {
        int64_t offset = i * splitSize;
        if (i < numNodes_ - 1) {
          splitColumns[i].emplace_back(inputColumn->Slice(offset, splitSize));
        } else {
          splitColumns[i].emplace_back(inputColumn->Slice(offset));
        }
      }
    }

    // make output tupleSets
    std::vector<std::shared_ptr<TupleSet>> outputTupleSets;
    for (int i = 0; i < numNodes_; ++i) {
      auto outputTupleSet = TupleSet::make(tupleSet->schema(), splitColumns[i]);
      auto res = outputTupleSet->combine();
      if (!res.has_value()) {
        throw std::runtime_error(res.error());
      }
      outputTupleSets.emplace_back(outputTupleSet);
    }
    return outputTupleSets;
  }

  // Combine all the partition results (originally we have multiple partitions, and each of them is just repartitioned),
  // then further split into pieces of 'num_rows_per_part' for each node.
  void combinePartitions() {
    fprintf(log_, "  Combine and generate final partitions... ");
    fflush(log_);

    if (partitionsVec_.empty()) {
      throw std::runtime_error("No partition obtained");
    }
    size_t num_nodes = partitionsVec_[0].size();

    // Gather partitions that belong to the same node
    std::vector<std::vector<std::shared_ptr<TupleSet>>> rotatedPartitionsVec{num_nodes};
    for (const auto &partitions: partitionsVec_) {
      if (partitions.size() != num_nodes) {
        throw std::runtime_error("Num partitions mismatch");
      }
      for (size_t i = 0; i < num_nodes; ++i) {
        rotatedPartitionsVec[i].emplace_back(partitions[i]);
      }
    }

    // Concatenate partitions that belong to the same node
    std::vector<std::shared_ptr<TupleSet>> concatenatedTupleSets;
    for (const auto &rotatedPartitions: rotatedPartitionsVec) {
      auto expConcatenatedTupleSet = TupleSet::concatenate(rotatedPartitions);
      if (!expConcatenatedTupleSet.has_value()) {
        throw std::runtime_error(expConcatenatedTupleSet.error());
      }
      auto concatenatedTupleSet = *expConcatenatedTupleSet;
      concatenatedTupleSets.emplace_back(concatenatedTupleSet);
    }

    // Split combined tupleSet into 'num_rows_per_part' for each node
    for (const auto &tupleSet: concatenatedTupleSets) {
      // split on 'num_rows_per_part'
      std::vector<arrow::ChunkedArrayVector> splitColumns;

      for (int c = 0; c < tupleSet->numColumns(); ++c) {
        auto inputColumn = tupleSet->table()->column(c);
        int64_t offset = 0;
        int num_part = 0;
        while (offset + numRowsPerPart_ < tupleSet->numRows()) {
          auto columnSlice = inputColumn->Slice(offset, numRowsPerPart_);
          if (c == 0) {
            splitColumns.emplace_back(arrow::ChunkedArrayVector{columnSlice});
          } else {
            splitColumns[num_part++].emplace_back(columnSlice);
          }
          offset += numRowsPerPart_;
        }
        if (offset < tupleSet->numRows()) {
          auto columnSlice = inputColumn->Slice(offset);
          if (c == 0) {
            splitColumns.emplace_back(arrow::ChunkedArrayVector{columnSlice});
          } else {
            splitColumns[num_part++].emplace_back(columnSlice);
          }
        }
      }

      // make output tupleSets
      std::vector<std::shared_ptr<TupleSet>> outputTupleSetsOneNode;
      for (const auto &columnVec: splitColumns) {
        auto outputTupleSet = TupleSet::make(tupleSet->schema(), columnVec);
        auto res = outputTupleSet->combine();
        if (!res.has_value()) {
          throw std::runtime_error(res.error());
        }
        outputTupleSetsOneNode.emplace_back(outputTupleSet);
      }
      outputTupleSets_.emplace_back(outputTupleSetsOneNode);
    }
    partitionsVec_.clear();

    fprintf(log_, "Done\n");
    fflush(log_);
  }

  void updateMetadata() {
    fprintf(log_, "  Update metadata files...  ");
    fflush(log_);

    // directory path
    std::filesystem::path schemaPath = std::filesystem::current_path()
            .parent_path()
            .parent_path()
            .append("resources/metadata")
            .append(newSchema_);
    if (!std::filesystem::exists(schemaPath)) {
      std::filesystem::create_directories(schemaPath);
    }

    // read or create json file of hash keys
    nlohmann::ordered_json hashKeysJObj;
    std::filesystem::path hashKeysFilePath = schemaPath.append("fpdb_store_hash_keys.json");
    if (!recreateMetadataFiles_ && std::filesystem::exists(hashKeysFilePath)) {
      hashKeysJObj = nlohmann::ordered_json::parse(readFile(hashKeysFilePath));
    } else {
      hashKeysJObj["schemaName"] = newSchema_;
    }

    // update hash keys if has
    if (key_.has_value()) {
      std::vector<nlohmann::ordered_json> hashKeysJArr;
      if (hashKeysJObj.contains("hashKeys")) {
        hashKeysJArr = hashKeysJObj["hashKeys"].get<std::vector<nlohmann::ordered_json>>();
      }
      nlohmann::ordered_json hashKeyPair;
      hashKeyPair["table"] = table_;
      hashKeyPair["key"] = *key_;
      hashKeysJArr.emplace_back(hashKeyPair);
      hashKeysJObj["hashKeys"] = hashKeysJArr;
    }

    // save json file
    std::ofstream hashKeysOs(hashKeysFilePath.string());
    hashKeysOs << hashKeysJObj.dump(2) << std::endl;
    schemaPath = schemaPath.parent_path();

    // read or create json file of object map
    nlohmann::ordered_json objectMapJObj;
    std::filesystem::path objectMapFilePath = schemaPath.append("fpdb_store_object_map.json");
    if (!recreateMetadataFiles_ && std::filesystem::exists(objectMapFilePath)) {
      objectMapJObj = nlohmann::ordered_json::parse(readFile(objectMapFilePath));
    } else {
      objectMapJObj["schemaName"] = newSchema_;
      objectMapJObj["numNodes"] = numNodes_;
    }

    // update object map
    std::vector<nlohmann::ordered_json> objectToNodeJArr;
    if (objectMapJObj.contains("objectMap")) {
      objectToNodeJArr = objectMapJObj["objectMap"].get<std::vector<nlohmann::ordered_json>>();
    }
    if (outputTupleSets_.size() == 1 && outputTupleSets_[0].size() == 1) {
      nlohmann::ordered_json objectToNodePair;
      objectToNodePair["object"] = table_ + suffix_;
      objectToNodePair["nodeId"] = 0;
      objectToNodeJArr.emplace_back(objectToNodePair);
    } else {
      size_t objectIdOffset = 0;
      for (int i = 0; i < numNodes_; ++i) {
        for (size_t j = 0; j < outputTupleSets_[i].size(); ++j) {
          nlohmann::ordered_json objectToNodePair;
          objectToNodePair["object"] = table_ + "_sharded/" + table_ + suffix_ + "." + std::to_string(objectIdOffset + j);
          objectToNodePair["nodeId"] = i;
          objectToNodeJArr.emplace_back(objectToNodePair);
        }
        objectIdOffset += outputTupleSets_[i].size();
      }
    }
    objectMapJObj["objectMap"] = objectToNodeJArr;

    // save json file
    std::ofstream objectMapOs(objectMapFilePath.string());
    objectMapOs << objectMapJObj.dump(2) << std::endl;

    fprintf(log_, "Done\n");
    fflush(log_);
  }

  void writePartitions() {
    fprintf(log_, "  Write final partitions to S3...  ");
    fflush(log_);

    if (outputTupleSets_.size() == 1 && outputTupleSets_[0].size() == 1) {
      writeObject(newSchema_ + table_ + suffix_,
                  outputTupleSets_[0][0]);
    } else {
      // Write in parallel
      std::vector<std::thread> thVec;
      int tupleSetId = 0;
      for (const auto &partitions: outputTupleSets_) {
        for (const auto &tupleSet: partitions) {
          std::thread th(&HashPartitioner::writeObject,
                         this,
                         newSchema_ + table_ + "_sharded/" + table_ + suffix_ + "." + std::to_string(tupleSetId++),
                         std::ref(tupleSet));
          thVec.emplace_back(std::move(th));
        }
      }
      for (auto &th: thVec) {
        th.join();
      }
    }

    fprintf(log_, "Done\n");
    fflush(log_);
  }

  void writeObject(const std::string &object, const std::shared_ptr<TupleSet> &tupleSet) {
    // limit concurrency
    {
      std::unique_lock lock(parallelMutex_);
      parallelCv_.wait(lock, [&] {
        return availSlots_ > 0;
      });
      --availSlots_;
    }

    // write table as parquet into arrow buffer
    auto expBufferOutputStream = arrow::io::BufferOutputStream::Create();
    if (!expBufferOutputStream.ok()) {
      throw std::runtime_error(expBufferOutputStream.status().message());
    }
    auto bufferOutputStream = *expBufferOutputStream;
    auto writerProperties = ::parquet::WriterProperties::Builder()
            .max_row_group_length(DefaultChunkSize)
            ->compression(::parquet::Compression::type::UNCOMPRESSED)
            ->build();
    auto arrowWriterProperties = ::parquet::ArrowWriterProperties::Builder()
            .build();
    auto st = ::parquet::arrow::WriteTable(*tupleSet->table(),
                                           arrow::default_memory_pool(),
                                           bufferOutputStream,
                                           DefaultChunkSize,
                                           writerProperties,
                                           arrowWriterProperties);
    if (!st.ok()) {
      throw std::runtime_error(st.message());
    }
    auto expBuffer = bufferOutputStream->Finish();
    if (!expBuffer.ok()) {
      throw std::runtime_error(expBuffer.status().message());
    }
    auto buffer = *expBuffer;

    // arrow buffer -> std stream
    auto ss = std::make_shared<std::stringstream>();
    ss->write(reinterpret_cast<const char *>(buffer->data()), buffer->size());

    // send request to s3
    Aws::S3::Model::PutObjectRequest putObjectRequest;
    putObjectRequest.SetBucket("flexpushdowndb");
    putObjectRequest.SetKey(Aws::String(object));
    putObjectRequest.SetBody(ss);
    Aws::S3::Model::PutObjectOutcome putObjectOutcome = awsClient_->getS3Client()->PutObject(putObjectRequest);
    if (!putObjectOutcome.IsSuccess()) {
      throw std::runtime_error(putObjectOutcome.GetError().GetMessage().c_str());
    }

    // limit concurrency
    {
      std::unique_lock lock(parallelMutex_);
      ++availSlots_;
      parallelCv_.notify_all();
    }
  }

  std::string schema_;
  std::string newSchema_;
  std::string table_;
  int numPartitions_;   // #partitions of the original data
  int numNodes_;
  int64_t numRowsPerPart_;
  bool recreateMetadataFiles_;
  std::optional<std::string> key_;

  std::string format_;
  std::string suffix_;
  std::shared_ptr<AWSClient> awsClient_;
  std::vector<std::vector<std::shared_ptr<TupleSet>>> partitionsVec_;   // intermediate partitions for each object
  std::vector<std::vector<std::shared_ptr<TupleSet>>> outputTupleSets_;  // final partitions for each node
  std::mutex mutex_;
  FILE *log_;

  int availSlots_;
  std::mutex parallelMutex_;
  std::condition_variable_any parallelCv_;
};

int main(int argc, char *argv[]) {
  if (argc < 8) {
    fprintf(stderr, "Usage: %s <schema> <new_schema> <table> <num_partitions> <num nodes> <num_rows_per_part> "
                    "<recreate_metadata_files> (<key>)",
            argv[0]);
    return 1;
  }

  // input parameters
  auto schema = std::string(argv[1]);
  auto newSchema = std::string(argv[2]);
  auto table = std::string(argv[3]);
  int numPartitions = std::stoi(argv[4]);
  int numNodes = std::stoi(argv[5]);
  int64_t numRowsPerPart = std::stoll(argv[6]);
  auto strArgv6 = std::string(argv[7]);
  bool recreateMetadataFiles = (strArgv6 == "true" || strArgv6 == "True" || strArgv6 == "TRUE") ? true : false;
  std::optional<std::string> key = std::nullopt;
  if (argc > 8) {
    key = std::string(argv[8]);
  }

  // run partitioner
  HashPartitioner partitioner(schema, newSchema, table, numPartitions, numNodes, numRowsPerPart,
                              recreateMetadataFiles, key);
  partitioner.doPartition();
}
