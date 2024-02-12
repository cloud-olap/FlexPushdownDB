//
// Created by matt on 16/6/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTCSVPARSEOPTIONS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTCSVPARSEOPTIONS_H

#include <string>

namespace fpdb::executor::physical::s3 {

class S3SelectCSVParseOptions {
public:
  S3SelectCSVParseOptions(std::string FieldDelimiter, std::string RecordDelimiter);
private:
  std::string fieldDelimiter_;
	std::string recordDelimiter_;
public:
  [[nodiscard]] const std::string &getFieldDelimiter() const;
  [[nodiscard]] const std::string &getRecordDelimiter() const;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTCSVPARSEOPTIONS_H
