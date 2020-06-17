//
// Created by matt on 16/6/20.
//

#include "normal/connector/s3/S3Util.h"

#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>

using namespace normal::connector::s3;

std::unordered_map<std::string, long>
S3Util::listObjects(std::string s3Bucket,
					std::vector<std::string> s3Objects,
					std::shared_ptr<S3Client> s3Client) {

  // Create a map of objects to object sizes
  std::unordered_map<std::string, long> partitionMap;
  for (auto &s3Object : s3Objects) {
	partitionMap.emplace(s3Object, 0);
  }

  // Invoke list object operations on the s3 objects
  std::vector<Aws::S3::Model::ListObjectsV2OutcomeCallable> partitionFutures;
  for (const auto &partition : partitionMap) {
	Aws::S3::Model::ListObjectsV2Request listObjectsRequest;
	listObjectsRequest.WithBucket(s3Bucket.c_str());
	listObjectsRequest.WithPrefix(partition.first.c_str());
	partitionFutures.push_back(s3Client->ListObjectsV2Callable(listObjectsRequest));
  }

  // Get the results from the list object futures
  for (auto &future : partitionFutures) {
	auto res = future.get();
	if (res.IsSuccess()) {
	  Aws::Vector<Aws::S3::Model::Object> objectList = res.GetResult().GetContents();
	  for (auto const &object : objectList) {
		partitionMap.find(object.GetKey().c_str())->second = object.GetSize();
	  }
	}
  }

  return partitionMap;
}
