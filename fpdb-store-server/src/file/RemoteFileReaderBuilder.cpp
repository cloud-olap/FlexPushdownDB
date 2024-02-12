//
// Created by Yifei Yang on 2/18/22.
//

#include <fpdb/store/server/file/RemoteFileReaderBuilder.h>
#include <fpdb/store/server/file/RemoteCSVReader.h>
#include <fpdb/store/server/file/RemoteParquetReader.h>

namespace fpdb::store::server::file {

std::shared_ptr<RemoteFileReader> RemoteFileReaderBuilder::make(const std::shared_ptr<FileFormat> &format,
                                                                const std::shared_ptr<::arrow::Schema> &schema,
                                                                const std::string &bucket,
                                                                const std::string &object,
                                                                const std::string &host,
                                                                int port) {
  switch (format->getType()) {
    case FileFormatType::CSV:
      return RemoteCSVReader::make(format, schema, bucket, object, host, port);
    case FileFormatType::PARQUET:
      return RemoteParquetReader::make(format, schema, bucket, object, host, port);
  }
}

}
