//
// Created by matt on 14/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY_H

#include <vector>
#include <memory>

#include <normal/tuple/Scalar.h>
#include <normal/tuple/Schema.h>

using namespace normal::tuple;

namespace normal::pushdown::group {

class GroupKey {

public:
  explicit GroupKey(std::shared_ptr<Schema> Schema, std::vector<std::shared_ptr<normal::tuple::Scalar>> Attributes);

  static std::shared_ptr<GroupKey> make();

  void append(std::string name, const std::shared_ptr<normal::tuple::Scalar>& attribute);
  size_t hash();
  bool operator==(const GroupKey &other);
  [[nodiscard]] std::string toString() const;
  [[nodiscard]] const std::shared_ptr<Schema> &getSchema() const;

  [[maybe_unused]] [[nodiscard]] const std::vector<std::shared_ptr<normal::tuple::Scalar>> &getAttributes() const;
  [[nodiscard]] std::shared_ptr<normal::tuple::Scalar> getAttributeValueByName(const std::string& name) const;

private:
  std::shared_ptr<Schema> schema_;
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
