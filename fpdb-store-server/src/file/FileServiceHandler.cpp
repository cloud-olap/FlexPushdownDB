//
// Created by Yifei Yang on 2/17/22.
//

#include <fpdb/store/server/file/FileServiceHandler.hpp>
#include <fpdb/util/Util.h>
#include <fmt/format.h>
#include <filesystem>
#include "fstream"

namespace fpdb::store::server::file {

FileServiceHandler::FileServiceHandler(const std::string &storeRootPathPrefix,
                                       int numDrives):
  storeRootPathPrefix_(storeRootPathPrefix),
  numDrives_(numDrives) {}

Status FileServiceHandler::ReadFile(ServerContext*,
                                    const ReadFileRequest* request,
                                    ServerWriter<ReadFileResponse>* writer) {
  auto bucket = request->bucket();
  auto object = request->object();
  if (!request->has_option()) {
    return Status(StatusCode::INVALID_ARGUMENT, "Option not set");
  }
  auto option = request->option();

  switch (option.type()) {
    case ReadOption_ReadType_FULL:
    case ReadOption_ReadType_RANGE: {
      // increment num reqs and get store root path
      auto storeRootPath = getStoreRootPath(updateReadFileDriveId());
      
      // check if file exists
      auto status = checkFileExist(storeRootPath, bucket, object);
      if (!status.ok()) {
        return status;
      }

      // set file position and length
      auto objectPath = fmt::format("{}/{}/{}", storeRootPath, bucket, object);
      int64_t position, length;
      if (option.type() == ReadOption_ReadType_FULL) {
        position = 0;
        length = fpdb::util::getFileSize(objectPath);
      } else {
        position = option.position();
        length = option.length();
      }

      // read file into chunks
      int64_t totalBytesRead = 0;
      std::ifstream is(objectPath, std::ifstream::binary);
      is.seekg(position, is.beg);
      char *buffer = new char[DefaultReadChunkSize];
      while (totalBytesRead < length && !is.eof()) {
        int64_t readSize = (totalBytesRead + DefaultReadChunkSize <= length) ?
                DefaultReadChunkSize : (length - totalBytesRead);
        is.read(buffer, readSize);
        int64_t bytesRead = is.gcount();
        writeReadFileResponse(writer, buffer, bytesRead);
        totalBytesRead += bytesRead;
      }

      delete []buffer;
      return Status::OK;
    }

    default: {
      return Status(StatusCode::INVALID_ARGUMENT, "Unknown read option");
    }
  }
}

Status FileServiceHandler::GetFileSize(ServerContext*,
                                       const GetFileSizeRequest* request,
                                       GetFileSizeResponse* response) {
  auto bucket = request->bucket();
  auto object = request->object();

  // increment num reqs and get store root path
  auto storeRootPath = getStoreRootPath(updateGetFileSizeDriveId());

  // check if file exists
  auto status = checkFileExist(storeRootPath, bucket, object);
  if (!status.ok()) {
    return status;
  }

  // get file size
  auto objectPath = fmt::format("{}/{}/{}", storeRootPath, bucket, object);
  int64_t fileSize = fpdb::util::getFileSize(objectPath);
  response->set_size(fileSize);
  return Status::OK;
}

Status FileServiceHandler::checkFileExist(const std::string &storeRootPath, 
                                          const std::string &bucket, 
                                          const std::string &object) {
  auto bucketPath = fmt::format("{}/{}", storeRootPath, bucket);
  if (!std::filesystem::exists(std::filesystem::path(bucketPath))) {
    return Status(StatusCode::NOT_FOUND, fmt::format("Bucket '{}' not exist", bucket));
  }
  auto objectPath = fmt::format("{}/{}", bucketPath, object);
  if (!std::filesystem::exists(std::filesystem::path(objectPath))) {
    return Status(StatusCode::NOT_FOUND, fmt::format("Object '{}' not exist in bucket '{}'",
                                                     object, bucket));
  }
  return Status::OK;
}

void FileServiceHandler::writeReadFileResponse(ServerWriter<ReadFileResponse>* writer, char *data, int64_t bytesRead) {
  ReadFileResponse resp;
  resp.set_data(data, bytesRead);
  resp.set_bytes_read(bytesRead);
  writer->Write(resp);
}

std::string FileServiceHandler::getStoreRootPath(int driveId) {
  return fmt::format("{}-{}", storeRootPathPrefix_, driveId);
}

int FileServiceHandler::updateReadFileDriveId() {
  std::unique_lock lock(updateReadFileDriveIdMutex_);
  if (++readFileDriveId_ >= numDrives_) {
    readFileDriveId_ -= numDrives_;
  }
  return readFileDriveId_;
}

int FileServiceHandler::updateGetFileSizeDriveId() {
  std::unique_lock lock(updateGetFileSizeDriveIdMutex_);
  if (++getFileSizeDriveId_ >= numDrives_) {
    getFileSizeDriveId_ -= numDrives_;
  }
  return getFileSizeDriveId_;
}

}
