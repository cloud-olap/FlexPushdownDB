//
// Created by Yifei Yang on 2/18/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_CSVREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_CSVREADER_H

#include <fpdb/tuple/FileReader.h>
#include <fpdb/tuple/TupleSet.h>

namespace fpdb::tuple::csv {

class CSVReader : virtual public FileReader {

public:
  CSVReader() = default;
  virtual ~CSVReader() = default;

  tl::expected<std::shared_ptr<TupleSet>, std::string> read(const std::vector<std::string> &columnNames) override;

protected:
#ifdef __AVX2__
  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingSimdParserImpl(const std::vector<std::string> &columnNames,
                          std::basic_istream<char, std::char_traits<char>> &inputStream);
#endif

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingArrowApiImpl(const std::vector<std::string> &columnNames,
                        const std::shared_ptr<::arrow::io::InputStream> &inputStream);

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readRangeImpl(const std::vector<std::string> &columnNames,
                int64_t startPos,
                int64_t finishPos,
                const std::shared_ptr<::arrow::io::RandomAccessFile> &inputStream);

private:
  virtual tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingSimdParser(const std::vector<std::string> &columnNames) = 0;

  virtual tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingArrowApi(const std::vector<std::string> &columnNames) = 0;
};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_CSVREADER_H
