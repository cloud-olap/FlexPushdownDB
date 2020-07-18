//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_MINICATALOGUE_H
#define NORMAL_MINICATALOGUE_H


#include <unordered_map>
#include <vector>

namespace normal::connector {

/**
 * A hardcoded catalogue used for generating the logical plan. May be integrated with normal-connector::Catalogue?
 */
class MiniCatalogue {

public:

    MiniCatalogue(const std::shared_ptr<std::vector<std::string>> &tables,
                  const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> &schemas,
                  const std::shared_ptr<std::vector<std::string>> &defaultJoinOrder);

    static std::shared_ptr<MiniCatalogue> defaultMiniCatalogue();

    std::shared_ptr<std::vector<std::string>> tables();

    std::shared_ptr<std::vector<std::string>> defaultJoinOrder();

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> schemas();

    std::string findTableOfColumn(std::string columnName);

private:

    std::shared_ptr<std::vector<std::string>> tables_;

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> schemas_;

    // default star join order, "lineorder" is center, order rest from small size to large size
    std::shared_ptr<std::vector<std::string>> defaultJoinOrder_;
};

}


#endif //NORMAL_MINICATALOGUE_H
