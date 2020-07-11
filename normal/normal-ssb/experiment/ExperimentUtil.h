//
// Created by Yifei Yang on 7/9/20.
//

#ifndef NORMAL_EXPERIMENTUTIL_H
#define NORMAL_EXPERIMENTUTIL_H

#include <normal/ssb/Globals.h>
#include <normal/pushdown/AWSClient.h>
#include <fstream>
#include <sstream>

class ExperimentUtil {

public:

  static std::string read_file(std::string filename);

  static std::vector<std::string> list_objects(normal::pushdown::AWSClient, std::string bucket_name, std::string dir_prefix);

};


#endif //NORMAL_EXPERIMENTUTIL_H
