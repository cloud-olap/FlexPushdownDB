//
// Created by Yifei Yang on 4/10/24.
//

#include <fpdb/executor/cache/CardCache.h>
#include <fmt/format.h>

namespace fpdb::executor::cache {

void PredTransCardCache::produce(const PredTransCardKey &key, const PredTransCardValue &value) {
  std::unique_lock lock(mutex_);

  auto it = cards_.find(key);
  if (it == cards_.end()) {
    cards_[key] = value;
  } else {
    it->second.card_ += value.card_;
    if (!it->second.joinKeyLen_.has_value()) {
      it->second.joinKeyLen_ = value.joinKeyLen_;
    }
  }
}

tl::expected<PredTransCardCache::PredTransCardValue, std::string>
  PredTransCardCache::consume(const PredTransCardKey &key) {
  std::unique_lock lock(mutex_);

  auto it = cards_.find(key);
  if (it == cards_.end()) {
    return tl::make_unexpected(fmt::format("PredTransCard with key '{}-{}-{}' not found",
                                           key.step_, key.forward_, key.left_));
  }
  const auto &card = it->second;
  cards_.erase(it);
  return card;
}

void OutputCardCache::produce(const OutputCardKey &key, int64_t card) {
  std::unique_lock lock(mutex_);

  cards_[key] += card;
}

tl::expected<int64_t, std::string> OutputCardCache::consume(const OutputCardKey &key) {
  std::unique_lock lock(mutex_);

  auto it = cards_.find(key);
  if (it == cards_.end()) {
    return tl::make_unexpected(fmt::format("OutputCard with key '{}' not found", key.prePOpId_));
  }
  int64_t card = it->second;
  cards_.erase(it);
  return card;
}

void CardCache::produce(const PredTransCardCache::PredTransCardKey &key,
                        const PredTransCardCache::PredTransCardValue &value) {
  predTransCardCache_.produce(key, value);
}

void CardCache::produce(const OutputCardCache::OutputCardKey &key, int64_t card) {
  outputCardCache_.produce(key, card);
}

tl::expected<PredTransCardCache::PredTransCardValue, std::string>
CardCache::consume(const PredTransCardCache::PredTransCardKey &key) {
  return predTransCardCache_.consume(key);
}

tl::expected<int64_t, std::string> CardCache::consume(const OutputCardCache::OutputCardKey &key) {
  return outputCardCache_.consume(key);
}

}
