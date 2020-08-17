//
// Created by matt on 12/8/20.
//

#include "normal/tuple/CSVReader.h"

#include <experimental/filesystem>

#include <normal/tuple/csv/CSVParser.h>

using namespace normal::tuple;
using namespace normal::tuple::csv;

CSVReader::CSVReader(std::string path) : path_(std::move(path)) {}

tl::expected<std::shared_ptr<CSVReader>, std::string> CSVReader::make(const std::string &path) {
  auto absolutePath = std::experimental::filesystem::absolute(path);
  return std::make_shared<CSVReader>(absolutePath);
}

tl::expected<std::shared_ptr<TupleSet2>, std::string>
CSVReader::read(const std::vector<std::string> &columnNames, unsigned long startPos, unsigned long finishPos) {
  CSVParser parser(path_, columnNames, startPos, finishPos);
  return parser.parse();
}
