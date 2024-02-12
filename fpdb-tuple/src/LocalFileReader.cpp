//
// Created by Yifei Yang on 2/18/22.
//

#include <fpdb/tuple/LocalFileReader.h>
#include <fpdb/util/Util.h>
#include <filesystem>

namespace fpdb::tuple {

LocalFileReader::LocalFileReader(const std::string &path):
  path_(path) {}

tl::expected<int64_t, std::string> LocalFileReader::getFileSize() const {
  // check if file exists
  if (!std::filesystem::exists(std::filesystem::path(path_))) {
    return tl::make_unexpected(fmt::format("File {} not exist.", path_));
  }

  return fpdb::util::getFileSize(path_);
}

}
