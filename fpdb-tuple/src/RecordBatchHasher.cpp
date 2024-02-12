//
// Created by Yifei Yang on 11/18/22.
//

#include <fpdb/tuple/RecordBatchHasher.h>
#include <arrow/compute/exec.h>
#include <fmt/format.h>

namespace fpdb::tuple {

tl::expected<std::shared_ptr<RecordBatchHasher>, std::string>
RecordBatchHasher::make(const std::shared_ptr<arrow::Schema> &schema,
                        const std::vector<std::string> &columnNames) {
  auto hasher = std::make_shared<RecordBatchHasher>();

  // temp stack
  auto status = hasher->tempStack_.Init(arrow::default_memory_pool(), 64 * MiniBatchSize_);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  // encoder ctx
  hasher->encodeCtx_.hardware_flags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  hasher->encodeCtx_.stack = &hasher->tempStack_;

  // key indices
  size_t numKeys = columnNames.size();
  hasher->keyIndices_.resize(numKeys);
  for (size_t i = 0; i < numKeys; ++i) {
    int keyIndex = schema->GetFieldIndex(columnNames[i]);
    if (keyIndex == -1) {
      return tl::make_unexpected(fmt::format("Column '{}' not found when creating RecordBatchHasher", columnNames[i]));
    }
    hasher->keyIndices_[i] = keyIndex;
  }

  // col metadata
  hasher->colMetadata_.resize(numKeys);
  for (size_t i = 0; i < numKeys; ++i) {
    int icol = hasher->keyIndices_[i];
    const auto &type = schema->field(icol)->type();
    if (type->id() == arrow::Type::DICTIONARY) {
      auto bit_width = arrow::internal::checked_cast<const arrow::FixedWidthType&>(*type).bit_width();
              ARROW_DCHECK(bit_width % 8 == 0);
      hasher->colMetadata_[i] = arrow::compute::KeyEncoder::KeyColumnMetadata(true, bit_width / 8);
    } else if (type->id() == arrow::Type::BOOL) {
      hasher->colMetadata_[i] = arrow::compute::KeyEncoder::KeyColumnMetadata(true, 0);
    } else if (is_fixed_width(type->id())) {
      hasher->colMetadata_[i] = arrow::compute::KeyEncoder::KeyColumnMetadata(
              true, arrow::internal::checked_cast<const arrow::FixedWidthType&>(*type).bit_width() / 8);
    } else if (is_binary_like(type->id())) {
      hasher->colMetadata_[i] = arrow::compute::KeyEncoder::KeyColumnMetadata(false, sizeof(uint32_t));
    } else {
      return tl::make_unexpected(fmt::format("Type '{}' not implemented for RecordBatchHasher", type->name()));
    }
  }

  // encoder
  hasher->encoder_.Init(hasher->colMetadata_, &hasher->encodeCtx_,
          /* row_alignment = */ sizeof(uint64_t),
          /* string_alignment = */ sizeof(uint64_t));

  // cols
  hasher->cols_.resize(numKeys);

  return hasher;
}

void RecordBatchHasher::hash(const std::shared_ptr<arrow::RecordBatch> &recordBatch, uint32_t *hashes) {
  int64_t numRows = recordBatch->num_rows();
  arrow::compute::ExecBatch execBatch(*recordBatch);

  // make cols
  for (size_t i = 0; i < keyIndices_.size(); ++i) {
    int icol = keyIndices_[i];
    const uint8_t* nonNulls = nullptr;
    if (execBatch[icol].array()->buffers[0] != NULLPTR) {
      nonNulls = execBatch[icol].array()->buffers[0]->data();
    }
    const uint8_t* fixedLen = execBatch[icol].array()->buffers[1]->data();
    const uint8_t* varLen = nullptr;
    if (!colMetadata_[i].is_fixed_length) {
      varLen = execBatch[icol].array()->buffers[2]->data();
    }

    int64_t offset = execBatch[icol].array()->offset;
    auto colBase = arrow::compute::KeyEncoder::KeyColumnArray(
            colMetadata_[i], offset + numRows, nonNulls, fixedLen, varLen);
    cols_[i] = arrow::compute::KeyEncoder::KeyColumnArray(colBase, offset, numRows);
  }

  // split into smaller mini-batches
  for (int64_t startRow = 0; startRow < numRows;) {
    int64_t nextMiniBatchSize = std::min(static_cast<int64_t>(MiniBatchSize_), numRows - startRow);

    // encode
    encoder_.PrepareEncodeSelected(startRow, nextMiniBatchSize, cols_);

    // compute hash
    arrow::compute::Hashing::HashMultiColumn(encoder_.GetBatchColumns(), &encodeCtx_, hashes + startRow);

    startRow += MiniBatchSize_;
  }
}

}
