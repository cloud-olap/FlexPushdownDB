//
// Created by matt on 14/5/20.
//

#include <normal/pushdown/group/GroupKey.h>

#include <utility>

using namespace normal::pushdown::group;

GroupKey::GroupKey(std::shared_ptr<Schema> Schema, std::vector<std::shared_ptr<normal::tuple::Scalar>> Attributes)
	: schema_(std::move(Schema)), attributes_(std::move(Attributes)) {}

std::string GroupKey::toString() const {

  std::string s;

  s += "(";
  for (size_t i = 0; i < attributes_.size(); ++i) {
	s += attributes_[i]->toString();
	if (i < attributes_.size() - 1) {
	  s += ",";
	}
  }
  s += ")";

  return s;
}

size_t GroupKey::hash() {
  size_t hash = 0;
  for (const auto &attribute: attributes_)
	hash += attribute->hash();
  return hash;
}

bool GroupKey::operator==(const GroupKey &other) {
  for (size_t attr_id = 0; attr_id < attributes_.size(); attr_id++) {
    if (*attributes_.at(attr_id) != *other.attributes_.at(attr_id)) {
      return false;
    }
  }
  return true;
}

std::shared_ptr<GroupKey> GroupKey::make() {
  std::vector<std::shared_ptr<::arrow::Field>> fields = {};
  auto arrowSchema = std::make_shared<::arrow::Schema>(fields);
  auto schema = std::make_shared<Schema>(arrowSchema);
  std::vector<std::shared_ptr<normal::tuple::Scalar>> attributes = {};
  return std::make_shared<GroupKey>(schema, attributes);
}

void GroupKey::append(std::string name, const std::shared_ptr<normal::tuple::Scalar> &attribute) {

  // FIXME: This seems wacky
  auto schema = schema_->getSchema();
  auto expectedSchema = schema->AddField(schema->fields().size(), ::arrow::field(name, attribute->type()));
  if(expectedSchema.ok())
	schema_ = std::make_shared<Schema>(*expectedSchema);
  else
    throw std::runtime_error(expectedSchema.status().message());

  attributes_.emplace_back(attribute);
}

[[maybe_unused]] [[maybe_unused]] const std::vector<std::shared_ptr<normal::tuple::Scalar>> &GroupKey::getAttributes() const {
  return attributes_;
}

std::shared_ptr<normal::tuple::Scalar> GroupKey::getAttributeValueByName(const std::string& name) const {
  int attributeIndex = schema_->getFieldIndexByName(name);
  return attributes_.at(attributeIndex);
}

const std::shared_ptr<Schema> &GroupKey::getSchema() const {
  return schema_;
}
