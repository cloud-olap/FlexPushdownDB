//
// Created by Yifei Yang on 4/10/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_BITMAPCACHE_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_BITMAPCACHE_HPP

#include <unordered_map>
#include <vector>
#include <string>
#include <shared_mutex>
#include <tl/expected.hpp>

namespace fpdb::store::server::flight {

/**
 * A cache for constructed bitmaps during bitmap pushdown.
 * Due to that flight can only return batches of the same schema in one call, compute side needs another call for bitmap.
 */
class BitmapCache {

public:
  BitmapCache() = default;

  static std::string generateBitmapKey(long queryId, const std::string &op);

  tl::expected<std::optional<std::vector<int64_t>>, std::string> consumeBitmap(const std::string &key);
  void produceBitmap(const std::string &key, const std::vector<int64_t> &bitmap, bool valid);

private:
  std::unordered_map<std::string, std::optional<std::vector<int64_t>>> bitmaps_;
  std::shared_mutex mutex_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_BITMAPCACHE_HPP
