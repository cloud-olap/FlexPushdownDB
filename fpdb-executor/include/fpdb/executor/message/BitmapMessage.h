//
// Created by Yifei Yang on 4/4/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BITMAPMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BITMAPMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <caf/all.hpp>

namespace fpdb::executor::message {

/**
 * Message sent for filter bitmap pushdown
 */
class BitmapMessage: public Message {

public:
  explicit BitmapMessage(const std::vector<int64_t> &bitmap,
                         const std::string &sender);
  BitmapMessage() = default;
  BitmapMessage(const BitmapMessage&) = default;
  BitmapMessage& operator=(const BitmapMessage&) = default;

  std::string getTypeString() const override;

  const std::vector<int64_t> &getBitmap() const;

private:
  // the bitmap
  std::vector<int64_t> bitmap_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BitmapMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("bitmap", msg.bitmap_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BITMAPMESSAGE_H
