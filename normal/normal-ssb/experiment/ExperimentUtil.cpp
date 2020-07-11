//
// Created by Yifei Yang on 7/9/20.
//

#include "ExperimentUtil.h"
#include <aws/s3/model/ListObjectsRequest.h>

using namespace normal::ssb;

std::string ExperimentUtil::read_file(std::string filename) {
  std::ifstream ifile(filename);
  std::ostringstream buf;
  char ch;
  while (buf && ifile.get(ch)) {
    buf.put(ch);
  }
  return buf.str();
}

std::vector<std::string> ExperimentUtil::list_objects(normal::pushdown::AWSClient client, std::string bucket_name, std::string dir_prefix) {
  client.init();
  Aws::S3::S3Client s3Client = *client.defaultS3Client();
  Aws::String aws_bucket_name(bucket_name);
  Aws::String aws_dir_prefix(dir_prefix);

  Aws::S3::Model::ListObjectsRequest objects_request;
  objects_request.WithBucket(aws_bucket_name);
  objects_request.WithPrefix(aws_dir_prefix);
  auto list_objects_outcome = s3Client.ListObjects(objects_request);

  std::vector<std::string> object_keys;

  if (list_objects_outcome.IsSuccess())
  {
    Aws::Vector<Aws::S3::Model::Object> object_list =
            list_objects_outcome.GetResult().GetContents();

    for (auto const &s3_object : object_list)
    {
      object_keys.push_back(s3_object.GetKey().c_str());
    }

  }
  else
  {
    std::cout << "ListObjects error: " <<
              list_objects_outcome.GetError().GetExceptionName() << " " <<
              list_objects_outcome.GetError().GetMessage() << std::endl;

  }

  return object_keys;
}