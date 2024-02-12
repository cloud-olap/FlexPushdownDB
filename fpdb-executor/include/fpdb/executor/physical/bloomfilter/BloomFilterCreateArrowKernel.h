//
// Created by Yifei Yang on 11/24/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEARROWKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEARROWKERNEL_H

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateAbstractKernel.h>
#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>

namespace fpdb::executor::physical::bloomfilter {

class BloomFilterCreateArrowKernel: public BloomFilterCreateAbstractKernel {

public:
  BloomFilterCreateArrowKernel(const std::vector<std::string> &columnNames);
  BloomFilterCreateArrowKernel() = default;
  BloomFilterCreateArrowKernel(const BloomFilterCreateArrowKernel&) = default;
  BloomFilterCreateArrowKernel& operator=(const BloomFilterCreateArrowKernel&) = default;
  ~BloomFilterCreateArrowKernel() override = default;

  static std::shared_ptr<BloomFilterCreateArrowKernel> make(const std::vector<std::string> &columnNames);

  tl::expected<void, std::string> buildBloomFilter() override;
  std::optional<std::shared_ptr<BloomFilterBase>> getBloomFilter() const override;

  void clear() override;

private:
  std::optional<std::shared_ptr<ArrowBloomFilter>> bloomFilter_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterCreateArrowKernel& kernel) {
    return f.object(kernel).fields(f.field("columnNames", kernel.columnNames_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEARROWKERNEL_H
