//
// Created by matt on 16/6/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTCSVPARSEOPTIONS_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTCSVPARSEOPTIONS_H

#include <string>

namespace normal::pushdown::s3 {

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

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTCSVPARSEOPTIONS_H
