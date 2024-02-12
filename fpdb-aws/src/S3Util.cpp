//
// Created by matt on 16/6/20.
//

#include <fpdb/aws/S3Util.h>
#include <aws/s3/model/ListObjectsRequest.h>

namespace fpdb::aws {

std::unordered_map<std::string, long>
S3Util::listObjects(const std::string& s3Bucket,
					const std::string& prefix,
					const std::vector<std::string>& s3Objects,
					const std::shared_ptr<S3Client>& s3Client) {

  // Create a map of objects to object sizes
  std::unordered_map<std::string, long> partitionMap;
  for (auto &s3Object : s3Objects) {
	partitionMap.emplace(s3Object, 0);
  }

  // Invoke list object operation on the s3 objects
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.WithBucket(s3Bucket.c_str());
  listObjectsRequest.WithPrefix(prefix.c_str());
  bool done = false;

  while (!done) {
    auto res = s3Client->ListObjects(listObjectsRequest);
    if (res.IsSuccess()) {
      Aws::Vector<Aws::S3::Model::Object> objectList = res.GetResult().GetContents();
      for (auto const &object: objectList) {
        auto partitionEntry = partitionMap.find(static_cast<std::string>(object.GetKey()));
        if (partitionEntry != partitionMap.end()) {
          partitionEntry->second = object.GetSize();
        }
      }
    } else {
      throw std::runtime_error(res.GetError().GetMessage().c_str());
    }
    done = !res.GetResult().GetIsTruncated();
    if (!done) {
      listObjectsRequest.SetMarker(res.GetResult().GetContents().back().GetKey());
    }
  }

  return partitionMap;
}

}
