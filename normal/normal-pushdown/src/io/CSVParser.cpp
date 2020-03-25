//
// Created by matt on 19/12/19.
//

#include "normal/core/Operator.h"      // for Operator
#include "normal/core/message/Message.h"       // for Message
#include <iostream>
#include <sstream>
#include <arrow/csv/parser.h>
#include <normal/core/TupleSet.h>
#include <normal/core/message/TupleMessage.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/status.h>              // for Status
#include <arrow/result.h>              // for Result
#include <arrow/io/memory.h>           // for BufferReader
#include <arrow/io/file.h>             // for ReadableFile
#include <arrow/csv/reader.h>          // for TableReader
#include <arrow/csv/options.h>         // for ReadOptions, ConvertOptions
#include <utility>
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <cstdlib>                    // for abort
#include "normal/pushdown/FileScan.h"
#include "CSVParser.h"

namespace normal::pushdown {

arrow::util::string_view CSVParser::asStringView(const arrow::Buffer &buffer) {
  const char *s = reinterpret_cast<const char *>(buffer.data());
  int64_t length = buffer.size();
  arrow::util::string_view view(s, length);
  return view;
}

arrow::util::string_view CSVParser::asStringView(const arrow::Buffer *buffer) {
  return asStringView(*buffer);
}

arrow::util::string_view CSVParser::asStringView(const std::shared_ptr<arrow::Buffer> &buffer) {
  return asStringView(*buffer);
}

std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
CSVParser::readFields(const std::shared_ptr<arrow::io::ReadableFile> &inputStream) {

  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> fields;
  arrow::Status st;
  auto parseOptions = arrow::csv::ParseOptions::Defaults();

  bool firstRowFound = false;
  long numBytesToRead = 16 * 1024;
  std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>(nullptr, 0);
  arrow::BufferBuilder bufferBuilder;

  while (!firstRowFound) {
    auto b = inputStream->Read(numBytesToRead).ValueOrDie();

    st = bufferBuilder.Append(buffer->data(), buffer->size());
    st = bufferBuilder.Append(b->data(), b->size());
    st = bufferBuilder.Finish(&buffer);

    auto bufferView = asStringView(buffer);

    arrow::csv::BlockParser p{parseOptions, -1, 1};
    uint32_t outSize;
    arrow::Status status = p.Parse(bufferView, &outSize);
    if (!status.ok() && !status.IsInvalid())
      abort();

    if (status.IsInvalid()) {
      numBytesToRead *= 2;
    } else {
      int numFields = p.num_cols();
      for (int i = 0; i < numFields; ++i) {

        st = p.VisitColumn(i, [&fields](const uint8_t *data, uint32_t size, bool quoted) -> arrow::Status {
          std::string fieldName{reinterpret_cast<const char *>(data), size};
          if (quoted) {
            fieldName.erase(0);
            fieldName.erase(fieldName.length());
          }
          fields[fieldName] = arrow::utf8();
          return arrow::Status::OK();
        });

//        std::stringstream ss;
//        ss << "f" << i;
//        fields[ss.str()] = arrow::utf8();
      }

      firstRowFound = true;
    }
  }

  return fields;
}

}