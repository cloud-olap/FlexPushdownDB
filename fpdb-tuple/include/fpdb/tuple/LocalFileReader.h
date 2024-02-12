//
// Created by Yifei Yang on 2/18/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_LOCALFILEREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_LOCALFILEREADER_H

#include <fpdb/tuple/FileReader.h>

namespace fpdb::tuple {

class LocalFileReader : virtual public FileReader{

public:
  LocalFileReader(const std::string &path);
  virtual ~LocalFileReader() = default;

  tl::expected<int64_t, std::string> getFileSize() const override;

protected:
  std::string path_;
};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_LOCALFILEREADER_H
