//
// Created by Jialing Pei on 5/6/20.
//

#ifndef NORMAL_CACHESCAN_H
#define NORMAL_CACHESCAN_H

#include <string>

#include <normal/core/Operator.h>
#include <normal/core/ATTIC/Cache.h>
namespace normal::pushdown {

    class cacheScan : public normal::core::Operator {

    private:

        std::string cacheID_;
        std::shared_ptr<Cache> cache_;
        void onStart();
        void onReceive(const normal::core::message::Envelope &message) override;


    public:
        cacheScan(std::string name, std::string cacheID, std::shared_ptr<Cache> cache);
        ~cacheScan() override;
    };

}
#endif //NORMAL_CACHESCAN_H
