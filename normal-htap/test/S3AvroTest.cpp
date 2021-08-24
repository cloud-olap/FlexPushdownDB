//
// Created by ZhangOscar on 8/23/21.
//

/*
 *  This code is intended to test the download and parsing of the avro files from S3.
 */

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <normal/pushdown/s3/S3Get.h>


bool GetObject(const Aws::String& objectKey, const Aws::String& fromBucket, const Aws::String& region) {
    Aws::Client::ClientConfiguration config;

    if (!region.empty()) {
        config.region = region;
    }

    Aws::S3::S3Client s3_client(config);

    Aws::S3::Model::GetObjectRequest object_request;
    object_request.SetBucket(fromBucket);
    object_request.SetKey(objectKey);

    Aws::S3::Model::GetObjectOutcome get_object_outcome =
            s3_client.GetObject(object_request);

    if (get_object_outcome.IsSuccess()) {
        auto& retrieved_file = get_object_outcome.GetResultWithOwnership().
                GetBody();

        // TODO: check whether this tuple is correctly parsed
        auto tupleResult = normal::pushdown::s3::S3Get::readAvroFile(retrieved_file, "../../normal-avro/schemas/delta/lineorder_d.json");
        return true;
    } else {
        auto err = get_object_outcome.GetError();
        std::cout << "Error: GetObject: " <<
        err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        return false;
    }
}

int main() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        const Aws::String bucket_name = "pushdowndb-htap";
        const Aws::String object_name = "lineorder.avro";
        const Aws::String region = "us-west-1";

        if (!GetObject(object_name, bucket_name, region)) {
            return 1;
        }
    }
    Aws::ShutdownAPI(options);

    return 0;
}

