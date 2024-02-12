//
// Created by Yifei Yang on 11/18/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_RECORDBATCHHASHER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_RECORDBATCHHASHER_H

#include <arrow/compute/exec/key_hash.h>
#include <arrow/api.h>
#include <tl/expected.hpp>

namespace fpdb::tuple {

/**
 * Compute hash for record batches, using the code from arrow/compute/kernels/hash_aggregate.cc
 * that uses arrow::compute::Hashing::HashMultiColumn()
 */
class RecordBatchHasher {

public:
  RecordBatchHasher() = default;

  static tl::expected<std::shared_ptr<RecordBatchHasher>, std::string>
  make(const std::shared_ptr<arrow::Schema> &schema,
       const std::vector<std::string> &columnNames);

  void hash(const std::shared_ptr<arrow::RecordBatch> &recordBatch, uint32_t *hashes);

private:
  static constexpr int MiniBatchSize_ = 1 << 10;

  arrow::util::TempVectorStack tempStack_;
  arrow::compute::KeyEncoder::KeyEncoderContext encodeCtx_;
  arrow::compute::KeyEncoder encoder_;
  std::vector<arrow::compute::KeyEncoder::KeyColumnMetadata> colMetadata_;
  std::vector<arrow::compute::KeyEncoder::KeyColumnArray> cols_;
  arrow::compute::KeyEncoder::KeyRowArray rows_;
  std::vector<int> keyIndices_;
};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_RECORDBATCHHASHER_H
