//
// Created by Yifei Yang on 4/10/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_CARDCACHE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_CARDCACHE_H

#include <unordered_map>
#include <shared_mutex>
#include <tl/expected.hpp>
#include <optional>
#include <sys/types.h>

namespace fpdb::executor::cache {

/**
 * Record cardinalities of pred-trans steps
 */
class PredTransCardCache {
public:
  struct PredTransCardKey {
    uint step_;
    bool forward_;
    bool left_;
    bool input_;    // whether the cardinality refers to the input or the reduced table, only applicable to right side

    PredTransCardKey(uint step, bool forward, bool left, bool input):
      step_(step), forward_(forward), left_(left), input_(input) {}
    PredTransCardKey() = default;
    PredTransCardKey(const PredTransCardKey&) = default;
    PredTransCardKey& operator=(const PredTransCardKey&) = default;
    ~PredTransCardKey() = default;

    size_t hash() const {
      return (step_ << 3) + (forward_ << 2) + (left_ << 1) + input_;
    };
    bool equalTo(const PredTransCardKey &other) const {
      return step_ == other.step_ && forward_ == other.forward_ && left_ == other.left_ && input_ == other.input_;
    }

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, PredTransCardKey& key) {
      return f.object(key).fields(f.field("step_", key.step_),
                                  f.field("forward_", key.forward_),
                                  f.field("left_", key.left_),
                                  f.field("input_", key.input_));
    }
  };

  struct PredTransCardKeyHash {
    inline size_t operator()(const PredTransCardKey &key) const {
      return key.hash();
    }
  };

  struct PredTransCardKeyPred {
    inline bool operator()(const PredTransCardKey &lhs, const PredTransCardKey &rhs) const {
      return lhs.equalTo(rhs);
    }
  };

  struct PredTransCardValue {
    int64_t card_;
    std::optional<double> joinKeyLen_;

    PredTransCardValue(int64_t card, std::optional<double> joinKeyLen): card_(card), joinKeyLen_(joinKeyLen) {}
    PredTransCardValue() = default;
    PredTransCardValue(const PredTransCardValue&) = default;
    PredTransCardValue& operator=(const PredTransCardValue&) = default;
    ~PredTransCardValue() = default;

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, PredTransCardValue& value) {
      return f.object(value).fields(f.field("card_", value.card_),
                                    f.field("joinKeyLen_", value.joinKeyLen_));
    }
  };

  // used to let ops record cardinalities
  struct PredTransCardInfo {
    bool collect_ = false;
    PredTransCardKey key_;

    PredTransCardInfo() = default;
    PredTransCardInfo(const PredTransCardInfo&) = default;
    PredTransCardInfo& operator=(const PredTransCardInfo&) = default;
    ~PredTransCardInfo() = default;

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, PredTransCardInfo& info) {
      return f.object(info).fields(f.field("collect_", info.collect_),
                                   f.field("key", info.key_));
    }
  };

  PredTransCardCache() = default;

  void produce(const PredTransCardKey &key, const PredTransCardValue &value);
  tl::expected<PredTransCardValue, std::string> consume(const PredTransCardKey &key);

private:
  std::unordered_map<PredTransCardKey, PredTransCardValue, PredTransCardKeyHash, PredTransCardKeyPred> cards_;
  std::shared_mutex mutex_;
};

/**
 * Record the output cardinality of finished ops
 */
class OutputCardCache {
public:
  struct OutputCardKey {
    uint prePOpId_;

    OutputCardKey(uint prePOpId): prePOpId_(prePOpId) {}
    OutputCardKey() = default;
    OutputCardKey(const OutputCardKey&) = default;
    OutputCardKey& operator=(const OutputCardKey&) = default;
    ~OutputCardKey() = default;

    size_t hash() const {
      return prePOpId_;
    }
    bool equalTo(const OutputCardKey &other) const {
      return prePOpId_ == other.prePOpId_;
    }

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, OutputCardKey& key) {
      return f.object(key).fields(f.field("prePOpId_", key.prePOpId_));
    }
  };
  
  struct OutputCardKeyHash {
    inline size_t operator()(const OutputCardKey &key) const {
      return key.hash();
    }
  };
  
  struct OutputCardKeyPred {
    inline bool operator()(const OutputCardKey &lhs, const OutputCardKey &rhs) const {
      return lhs.equalTo(rhs);
    }
  };

  // used to let ops record cardinalities
  struct OutputCardInfo {
    bool collect_ = false;
    OutputCardKey key_;

    OutputCardInfo() = default;
    OutputCardInfo(const OutputCardInfo&) = default;
    OutputCardInfo& operator=(const OutputCardInfo&) = default;
    ~OutputCardInfo() = default;

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, OutputCardInfo& info) {
      return f.object(info).fields(f.field("collect_", info.collect_),
                                   f.field("key", info.key_));
    }
  };

  OutputCardCache() = default;

  void produce(const OutputCardKey &key, int64_t card);
  tl::expected<int64_t, std::string> consume(const OutputCardKey &key);

private:
  std::unordered_map<OutputCardKey, int64_t, OutputCardKeyHash, OutputCardKeyPred> cards_;
  std::shared_mutex mutex_;
};

/**
 * Record cardinality metrics of finished ops
 */
class CardCache {
public:
  CardCache() = default;

  void produce(const PredTransCardCache::PredTransCardKey &key, const PredTransCardCache::PredTransCardValue &value);
  void produce(const OutputCardCache::OutputCardKey &key, int64_t card);

  tl::expected<PredTransCardCache::PredTransCardValue, std::string>
          consume(const PredTransCardCache::PredTransCardKey &key);
  tl::expected<int64_t, std::string> consume(const OutputCardCache::OutputCardKey &key);

private:
  PredTransCardCache predTransCardCache_;
  OutputCardCache outputCardCache_;
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_CARDCACHE_H
