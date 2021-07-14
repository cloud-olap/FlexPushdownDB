//
// Created by matt on 27/3/20.
//

#include <normal/pushdown/AWSClient.h>
#include <normal/connector/s3/S3SelectConnector.h>

normal::connector::s3::S3SelectConnector::S3SelectConnector(const std::string &name) :
    Connector(name){
}
