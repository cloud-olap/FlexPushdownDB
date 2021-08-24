//
// Created by Han Cao on 8/15/21.
//

#ifndef NORMAL_AVROTUPLE_H
#define NORMAL_AVROTUPLE_H


#include <string>
#include <vector>

namespace normal::avro_tuple {

class avroTuple {
public:
    avroTuple(bool isDelta);

private:
    std::vector<std::string> avroVector;
    std::string tableName_;
    bool isDelta_;
};

}

#endif //NORMAL_AVROTUPLE_H
