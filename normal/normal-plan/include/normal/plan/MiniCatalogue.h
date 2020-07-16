//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_MINICATALOGUE_H
#define NORMAL_MINICATALOGUE_H


#include <unordered_map>
#include <normal/tuple/Schema.h>

namespace normal::plan {

/**
 * A hardcoded catalogue used for generating the logical plan. May be integrated with normal-connector::Catalogue?
 */
class MiniCatalogue {

public:
    static void init();

private:
    // default star join order, "lineorder" is center, order rest from small size to large size
    static std::shared_ptr<std::unordered_map<std::string, int>> defaultJoinOrder;

    static std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> schemas;
};

}


#endif //NORMAL_MINICATALOGUE_H
