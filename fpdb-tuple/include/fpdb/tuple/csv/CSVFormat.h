//
// Created by Yifei Yang on 2/13/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_CSVFORMAT_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_CSVFORMAT_H

#include <fpdb/tuple/FileFormat.h>

namespace fpdb::tuple::csv {

class CSVFormat: public FileFormat {

public:
  CSVFormat(char fieldDelimiter,
            int skipRows = 1);
  CSVFormat() = default;
  CSVFormat(const CSVFormat&) = default;
  CSVFormat& operator=(const CSVFormat&) = default;
  ~CSVFormat() = default;

  char getFieldDelimiter() const;
  int getSkipRows() const;

  bool isColumnar() const override;

  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<CSVFormat>, std::string> fromJson(const nlohmann::json &jObj);

private:
  char fieldDelimiter_;
  int skipRows_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CSVFormat& format) {
    return f.object(format).fields(f.field("type", format.type_),
                                   f.field("fieldDelimiter", format.fieldDelimiter_),
                                   f.field("skipRows", format.skipRows_));
  }
};

}

#endif // FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_CSVFORMAT_H
