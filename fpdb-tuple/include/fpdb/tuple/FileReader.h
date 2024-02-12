//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEREADER_H

#include <fpdb/tuple/FileFormat.h>
#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <memory>

namespace fpdb::tuple {

class FileReader {
  
public:
  FileReader(const std::shared_ptr<FileFormat> &format,
             const std::shared_ptr<::arrow::Schema> schema);
//  FileReader() = default;
  virtual ~FileReader() = default;

  const std::shared_ptr<FileFormat> &getFormat() const;

  int64_t getBytesReadLocal() const;
  int64_t getBytesReadRemote() const;

  /**
   * Read the whole file
   * @return
   */
  tl::expected<std::shared_ptr<TupleSet>, std::string> read();

  /**
   * Read specific columns of the file
   * @param inputSchema schema of the table to read
   * @param outputSchema output schema
   * @return
   */
  virtual tl::expected<std::shared_ptr<TupleSet>, std::string> read(const std::vector<std::string> &columnNames) = 0;

  /**
   * Read specific byte range of the file
   * @return
   */
  tl::expected<std::shared_ptr<TupleSet>, std::string> readRange(int64_t startPos, int64_t finishPos);

  /**
   * Read specific columns inside specific byte range of the file
   * @param columnNames
   * @param startPos
   * @param finishPos
   * @return
   */
  virtual tl::expected<std::shared_ptr<TupleSet>, std::string>
  readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) = 0;

  virtual tl::expected<int64_t, std::string> getFileSize() const = 0;

protected:
  // close the input stream, in case getting exception or read finished
  void close(const std::shared_ptr<arrow::io::InputStream> &inputStream);

  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<::arrow::Schema> schema_;

  int64_t bytesReadLocal_ = 0;
  int64_t bytesReadRemote_ = 0;

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEREADER_H
