//
// Created by matt on 14/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY_H

#include <vector>
#include <memory>

#include <normal/tuple/Scalar.h>

namespace normal::pushdown::group {

class GroupKey {

public:
  GroupKey();
  explicit GroupKey(std::vector<std::shared_ptr<normal::tuple::Scalar>> Attributes);

  static std::shared_ptr<GroupKey> make();

  void append(const std::shared_ptr<normal::tuple::Scalar>& attribute);
  size_t hash();
  bool operator==(const GroupKey &other);
  std::string toString();

private:
  std::vector<std::shared_ptr<normal::tuple::Scalar>> attributes_;

};

struct GroupKeyPointerHash {
  inline size_t operator()(const std::shared_ptr<GroupKey> &key) const {
	return key->hash();
  }
};

struct GroupKeyPointerPredicate {
  inline bool operator()(const std::shared_ptr<GroupKey>& lhs, const std::shared_ptr<GroupKey>& rhs) const {
	return *lhs == *rhs;
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY_H
