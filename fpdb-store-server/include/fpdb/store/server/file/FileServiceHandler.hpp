//
// Created by Yifei Yang on 2/17/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_FILESERVICEHANDLER_H
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_FILESERVICEHANDLER_H

#include <FileService.grpc.pb.h>
#include <mutex>

using namespace grpc;
using namespace google::protobuf;

namespace fpdb::store::server::file {

class FileServiceHandler: public FileService::Service {

public:
  FileServiceHandler(const std::string &storeRootPathPrefix,
                     int numDrives);
  ~FileServiceHandler() = default;

  Status ReadFile(ServerContext* context,
                  const ReadFileRequest* request,
                  ServerWriter<ReadFileResponse>* writer) override;

  Status GetFileSize(ServerContext* context,
                     const GetFileSizeRequest* request,
                     GetFileSizeResponse* response) override;

private:
  static Status checkFileExist(const std::string &storeRootPath, 
                               const std::string &bucket, 
                               const std::string &object);
  static void writeReadFileResponse(ServerWriter<ReadFileResponse>* writer, char *data, int64_t bytesRead);
  std::string getStoreRootPath(int driveId);
  int updateReadFileDriveId();
  int updateGetFileSizeDriveId();

  static constexpr int DefaultReadChunkSize = 16 * 1024 * 1024;   // 16 MB
  std::string storeRootPathPrefix_;
  int numDrives_;
  int readFileDriveId_ = 0, getFileSizeDriveId_ = 0;    // updated in round-robin
  std::mutex updateReadFileDriveIdMutex_, updateGetFileSizeDriveIdMutex_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_FILESERVICEHANDLER_H
