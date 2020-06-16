//
// Created by matt on 16/6/20.
//

#include "normal/pushdown/s3/S3SelectCSVParseOptions.h"

using namespace normal::pushdown;

S3SelectCSVParseOptions::S3SelectCSVParseOptions(const std::string &FieldDelimiter,
												 const std::string &RecordDelimiter)
	: fieldDelimiter_(FieldDelimiter), recordDelimiter_(RecordDelimiter) {}

const std::string &S3SelectCSVParseOptions::getFieldDelimiter() const {
  return fieldDelimiter_;
}

const std::string &S3SelectCSVParseOptions::getRecordDelimiter() const {
  return recordDelimiter_;
}
