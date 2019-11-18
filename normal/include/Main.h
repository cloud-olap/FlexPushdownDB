#ifndef NORMAL_MAIN_H
#define NORMAL_MAIN_H

#include <memory>
#include <aws/s3/S3Client.h>

#include <aws/s3/model/SelectObjectContentRequest.h>

using namespace Aws::S3;

class Main {
public:
    int run();
};


#endif //NORMAL_MAIN_H
