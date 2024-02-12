//
// Created by Yifei Yang on 4/4/22.
//

#include <fpdb/executor/message/BitmapMessage.h>

namespace fpdb::executor::message {

BitmapMessage::BitmapMessage(const std::vector<int64_t> &bitmap,
                             const std::string &sender) :
  Message(BITMAP, sender),
  bitmap_(bitmap) {}

std::string BitmapMessage::getTypeString() const {
  return "BitmapMessage";
}

const std::vector<int64_t> &BitmapMessage::getBitmap() const {
  return bitmap_;
}

}
