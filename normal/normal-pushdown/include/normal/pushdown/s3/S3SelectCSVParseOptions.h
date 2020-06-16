//
// Created by matt on 16/6/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTCSVPARSEOPTIONS_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTCSVPARSEOPTIONS_H

#include <string>

namespace normal::pushdown {

class S3SelectCSVParseOptions {
public:
  S3SelectCSVParseOptions(const std::string &FieldDelimiter, const std::string &RecordDelimiter);
private:
  std::string fieldDelimiter_;
	std::string recordDelimiter_;
public:
  const std::string &getFieldDelimiter() const;
  const std::string &getRecordDelimiter() const;
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTCSVPARSEOPTIONS_H
