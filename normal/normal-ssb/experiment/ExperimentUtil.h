//
// Created by Yifei Yang on 7/9/20.
//

#ifndef NORMAL_EXPERIMENTUTIL_H
#define NORMAL_EXPERIMENTUTIL_H

#include <normal/ssb/Globals.h>
#include <normal/pushdown/AWSClient.h>
#include <fstream>
#include <sstream>

namespace normal::ssb {

class ExperimentUtil {

public:

  static std::string read_file(const std::string& filename);

  [[maybe_unused]] static std::vector<std::string> list_objects(normal::pushdown::AWSClient, const std::string& bucket_name, const std::string& dir_prefix);

};
}

#endif //NORMAL_EXPERIMENTUTIL_H
