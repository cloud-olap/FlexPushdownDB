//
// Created by matt on 11/8/20.
//

#include <fpdb/tuple/Globals.h>
#include <fpdb/tuple/Converter.h>
#include <fpdb/tuple/LocalFileReaderBuilder.h>

#include <filesystem>

#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>


using namespace fpdb::tuple;

tl::expected<void, std::string> Converter::csvToParquet(const std::string &inFile,
                                                        const std::string &outFile,
                                                        const std::shared_ptr<csv::CSVFormat> &csvFormat,
                                                        const std::shared_ptr<::arrow::Schema> &schema,
                                                        int rowGroupSize,
                                                        ::parquet::Compression::type compressionType) {
  // create directory if not exist
  auto dirPath = std::filesystem::absolute(outFile).remove_filename();
  if (!std::filesystem::exists(dirPath)) {
    std::filesystem::create_directories(dirPath);
  }

  // read csv into arrow table
  auto reader = LocalFileReaderBuilder::make(csvFormat, schema, inFile);
  auto expTupleSet = reader->read();
  if (!expTupleSet.has_value()) {
    return tl::make_unexpected(expTupleSet.error());
  }
  auto table = (*expTupleSet)->table();

  // write arrow table into parquet
  auto absoluteOutFile = std::filesystem::absolute(outFile);
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  auto expectedOutputStream = arrow::io::FileOutputStream::Open(absoluteOutFile);
  if (!expectedOutputStream.ok())
    return tl::make_unexpected(expectedOutputStream.status().message());
  auto outputStream = *expectedOutputStream;

  auto writerProperties = ::parquet::WriterProperties::Builder()
                            .max_row_group_length(rowGroupSize)
                            ->compression(compressionType)
                            ->build();

  auto arrowWriterProperties = ::parquet::ArrowWriterProperties::Builder()
                                 .build();

  auto result = ::parquet::arrow::WriteTable(*table,
                                             arrow::default_memory_pool(),
                                             outputStream,
                                             DefaultChunkSize,
                                             writerProperties,
                                             arrowWriterProperties);

  if (!result.ok())
    return tl::make_unexpected(result.message());

  return {};
}