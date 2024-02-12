//
// Created by matt on 19/5/20.
//

#include <fpdb/cache/SegmentKey.h>
#include <fmt/format.h>

using namespace fpdb::cache;

SegmentKey::SegmentKey(std::shared_ptr<Partition> Partition,
                       std::string columnName,
                       SegmentRange Range)
        : partition_(std::move(Partition)),
          columnName_(std::move(columnName)),
          range_(Range) {}

SegmentKey::SegmentKey(std::shared_ptr<Partition> Partition,
                       std::string columnName,
                       SegmentRange Range,
                       std::shared_ptr<SegmentMetadata> metadata)
	: partition_(std::move(Partition)),
	  columnName_(std::move(columnName)),
	  range_(Range),
	  metadata_(std::move(metadata)) {}

std::shared_ptr<SegmentKey> SegmentKey::make(const std::shared_ptr<Partition> &Partition,
                                             const std::string& columnName,
                                             SegmentRange Range) {
  return std::make_shared<SegmentKey>(Partition,
                                      columnName,
                                      Range);
}

std::shared_ptr<SegmentKey> SegmentKey::make(const std::shared_ptr<Partition> &Partition,
                                             const std::string& columnName,
                                             SegmentRange Range,
                                             const std::shared_ptr<SegmentMetadata>& metadata) {
  return std::make_shared<SegmentKey>(Partition,
                                      columnName,
                                      Range,
                                      metadata);
}

const std::shared_ptr<Partition> &SegmentKey::getPartition() const {
  return partition_;
}

[[maybe_unused]] const SegmentRange &SegmentKey::getRange() const {
  return range_;
}

std::string SegmentKey::toString() {
  return fmt::format("{{ partition: {}, column: {}, range: {} }}", partition_->toString(), columnName_, range_.toString());
}

bool SegmentKey::operator==(const SegmentKey &other) const {
  return this->partition_->equalTo(other.partition_) &&
	  this->columnName_ == other.columnName_;// &&
	  // We have changed the files a few times and this has caused changes in the byte range. However we are not splitting
	  // up objects by byte range so removing this check for now is fine.
//	  this->range_ == other.range_;
}

bool SegmentKey::operator!=(const SegmentKey &other) const {
  return !(*this == other);
}

size_t SegmentKey::hash() {
  return partition_->hash();
  // We have changed the files a few times and this has caused changes in the byte range. However we are not splitting
  // up objects by byte range so removing this check for now is fine.
//	  this->range_ == other.range_;
//  return partition_->hash() + range_.hash();
}

const std::shared_ptr<SegmentMetadata> &SegmentKey::getMetadata() const {
  return metadata_;
}

const std::string &SegmentKey::getColumnName() const {
  return columnName_;
}

void SegmentKey::setMetadata(const std::shared_ptr<SegmentMetadata> &metadata) {
    metadata_ = metadata;
}
