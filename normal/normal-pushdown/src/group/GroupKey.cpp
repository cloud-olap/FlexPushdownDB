//
// Created by matt on 14/5/20.
//

#include <normal/pushdown/group/GroupKey.h>

#include <utility>

using namespace normal::pushdown::group;

GroupKey::GroupKey(std::vector<std::shared_ptr<normal::tuple::Scalar>> Attributes)
	: attributes_(std::move(Attributes)) {}

std::string GroupKey::toString() {

  std::string s;

  s+= "(";
  for(size_t i=0;i<attributes_.size();++i){
    s += attributes_[i]->toString();
    if(i < attributes_.size() - 1){
      s += ",";
    }
  }
  s+= ")";

  return s;
}

size_t GroupKey::hash() {
  size_t hash = 0;
  for(const auto &attribute: attributes_)
	hash += attribute->hash();
  return hash;
}

bool GroupKey::operator==(const GroupKey &other) {
  for (const auto &attribute: attributes_) {
	for (const auto &otherAttribute: other.attributes_) {
	  if (*attribute != *otherAttribute)
		return false;
	}
  }
  return true;
}

GroupKey::GroupKey(): attributes_({}) {
}

std::shared_ptr<GroupKey> GroupKey::make() {
  return std::make_shared<GroupKey>();
}

void GroupKey::append(const std::shared_ptr<normal::tuple::Scalar>& attribute) {
	attributes_.emplace_back(attribute);
}
