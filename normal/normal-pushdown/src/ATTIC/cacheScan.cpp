//
// Created by Jialing Pei on 5/6/20.
//

#include <normal/pushdown/TupleMessage.h>
#include "normal/pushdown/ATTIC/cacheScan.h"
namespace normal::pushdown {
    cacheScan::cacheScan(std::string name,  std::string cacheID,std::shared_ptr<Cache> cache) :
    Operator(std::move(name), "cacheScan"),
    cacheID_(std::move(cacheID)),
    cache_(cache){}
    void cacheScan::onStart() {
        std::unordered_map<std::string, std::shared_ptr<TupleSet>> cacheMap = cache_->m_cacheData;
        //no found
        if (cacheMap.empty() || cacheMap.find(cacheID_)==cacheMap.end()) {
            ctx()->notifyComplete();
        }
        else {
            std::shared_ptr<TupleSet> tupleSet = cacheMap[cacheID_];
            std::shared_ptr<normal::core::message::Message> message = std::make_shared<normal::core::message::TupleMessage>(tupleSet, this->name());
            ctx()->tell(message);
            ctx()->notifyComplete();
        }
    }
    void cacheScan::onReceive(const normal::core::message::Envelope &message) {
        if (message.message().type() == "StartMessage") {
            this->onStart();
        } else {
            throw;
        }
    }

}