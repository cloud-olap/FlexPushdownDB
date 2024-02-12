//
// Created by matt on 5/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPCONTEXT_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPCONTEXT_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/physical/LocalPOpDirectory.h>
#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/physical/Forward.h>
#include <fpdb/caf/CAFUtil.h>
#include <memory>
#include <string>

namespace fpdb::executor::physical {

/**
 * The API physical operators use to interact with their environment, e.g. sending messages
 */
class POpContext {

public:
  POpContext(::caf::actor rootActor, ::caf::actor segmentCacheActor);
  POpContext() = default;
  POpContext(const POpContext&) = default;
  POpContext& operator=(const POpContext&) = default;

  POpActor* operatorActor();
  void operatorActor(POpActor *operatorActor);

  LocalPOpDirectory &operatorMap();

  void tell(const std::shared_ptr<message::Message> &msg,
            const std::optional<std::set<std::string>> &consumers = std::nullopt);    // default for all consumers
  void send(const std::shared_ptr<message::Message> &msg, const std::string &consumer);
  void notifyComplete();
  void notifyError(const std::string &content);
  void notifyRoot(const std::shared_ptr<message::Message> &msg,
                  const std::optional<std::string> &embeddedBloomFilterConsumer = std::nullopt);

  void destroyActorHandles();
  [[nodiscard]] bool isComplete() const;

private:
  void send_regular(const std::shared_ptr<message::Message> &msg, const std::string &consumer);
  // this only applies on TupleSetMessage/TupleSetBufferMessage with bloom filter set,
  // others retain the original message
  std::shared_ptr<message::Message> applyEmbeddedBloomFilter(const std::shared_ptr<message::Message> &msg,
                                                             const std::string &consumer);

  POpActor* operatorActor_;
  LocalPOpDirectory operatorMap_;
  ::caf::actor rootActor_;
  ::caf::actor segmentCacheActor_;
  bool complete_ = false;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, POpContext& ctx) {
    return f.object(ctx).fields(f.field("operatorMap", ctx.operatorMap_),
                                f.field("rootActor", ctx.rootActor_),
                                f.field("segmentCacheActor", ctx.segmentCacheActor_),
                                f.field("complete", ctx.complete_));
  }
};

}

using POpContextPtr = std::shared_ptr<fpdb::executor::physical::POpContext>;

CAF_BEGIN_TYPE_ID_BLOCK(POpContext, fpdb::caf::CAFUtil::POpContext_first_custom_type_id)
CAF_ADD_TYPE_ID(POpContext, (fpdb::executor::physical::POpContext))
CAF_END_TYPE_ID_BLOCK(POpContext)

namespace caf {
template <>
struct inspector_access<POpContextPtr> : variant_inspector_access<POpContextPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPCONTEXT_H
