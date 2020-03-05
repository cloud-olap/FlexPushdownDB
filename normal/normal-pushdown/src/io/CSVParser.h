//
// Created by matt on 19/12/19.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_IO_CSVPARSER_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_IO_CSVPARSER_H

namespace normal::pushdown {

class CSVParser {

  static arrow::util::string_view asStringView(const arrow::Buffer &buffer);
  static nonstd::string_view asStringView(const arrow::Buffer &&buffer);
  static nonstd::string_view asStringView(const std::shared_ptr<arrow::Buffer> &buffer);
  static nonstd::string_view asStringView(const arrow::Buffer *buffer);
public:
  static std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
  readFields(const std::shared_ptr<arrow::io::ReadableFile> &inputStream);
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_IO_CSVPARSER_H
