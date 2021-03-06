//
// Created by Yifei Yang on 3/18/21.
//

#include <doctest/doctest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/plan/Globals.h>
#include <spdlog/spdlog.h>

using namespace normal::pushdown;

#define SKIP_SUITE true

void listObjects(std::shared_ptr<Aws::S3::S3Client>& s3Client) {
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.WithBucket("pushdowndb");
  listObjectsRequest.WithPrefix("ssb-sf100-sortlineorder/parquet_150MB/");

  bool done = false;
  std::vector<std::string> objectNames;
  while (!done) {
    auto res = s3Client->ListObjects(listObjectsRequest);
    if (res.IsSuccess()) {
      Aws::Vector<Aws::S3::Model::Object> objectList = res.GetResult().GetContents();
      for (auto const &object: objectList) {
        objectNames.emplace_back(static_cast<std::string>(object.GetKey()));
      }
    } else {
      throw std::runtime_error(res.GetError().GetMessage().c_str());
    }
    done = !res.GetResult().GetIsTruncated();
    if (!done) {
      listObjectsRequest.SetMarker(res.GetResult().GetContents().back().GetKey());
    }
  }

  SPDLOG_INFO("Total {} objects, list first 20:", objectNames.size());
  int cnt = 1;
  for (auto const& objectName: objectNames) {
    SPDLOG_INFO("{}: {}", cnt++, objectName);
    if (cnt >= 20)
      break;
  }
}

TEST_SUITE ("connection" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("connection-s3" * doctest::skip(false || SKIP_SUITE)) {
  normal::plan::s3ClientType = normal::plan::S3;
  auto s3Client = AWSClient::defaultS3Client();
  listObjects(s3Client);
}

TEST_CASE ("connection-minio" * doctest::skip(false || SKIP_SUITE)) {
  normal::plan::s3ClientType = normal::plan::Minio;
  auto s3Client = AWSClient::defaultS3Client();
  listObjects(s3Client);
}

TEST_CASE ("connection-airmettle" * doctest::skip(false || SKIP_SUITE)) {
  normal::plan::s3ClientType = normal::plan::Airmettle;
  auto s3Client = AWSClient::defaultS3Client();
  listObjects(s3Client);
}

}
