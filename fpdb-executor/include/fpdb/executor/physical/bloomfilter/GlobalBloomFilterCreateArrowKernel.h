//
// Created by Yifei Yang on 10/24/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERCREATEARROWKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERCREATEARROWKERNEL_H

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateAbstractKernel.h>
#include <fpdb/executor/physical/bloomfilter/GlobalArrowBloomFilter.h>

namespace fpdb::executor::physical::bloomfilter {

class GlobalBloomFilterCreateArrowKernel: public BloomFilterCreateAbstractKernel {
public:
  GlobalBloomFilterCreateArrowKernel(const std::vector<std::string> &columnNames);
  GlobalBloomFilterCreateArrowKernel() = default;
  GlobalBloomFilterCreateArrowKernel(const GlobalBloomFilterCreateArrowKernel&) = default;
  GlobalBloomFilterCreateArrowKernel& operator=(const GlobalBloomFilterCreateArrowKernel&) = default;
  ~GlobalBloomFilterCreateArrowKernel() override = default;

  static std::shared_ptr<GlobalBloomFilterCreateArrowKernel> make(const std::vector<std::string> &columnNames);

  void setInitedGlobalArrowBloomFilter(int threadId,
                                       const std::shared_ptr<GlobalArrowBloomFilter> &bloomFilter);

  tl::expected<void, std::string> buildBloomFilter() override;
  std::optional<std::shared_ptr<BloomFilterBase>> getBloomFilter() const override;
  void clear() override;

private:
  // state set at runtime
  int threadId_;
  std::optional<std::shared_ptr<GlobalArrowBloomFilter>> bloomFilter_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GlobalBloomFilterCreateArrowKernel& kernel) {
    return f.object(kernel).fields(f.field("columnNames", kernel.columnNames_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERCREATEARROWKERNEL_H
