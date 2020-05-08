//
// Created by matt on 8/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNBUILDER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNBUILDER_H

#include <memory>
#include <utility>

#include <arrow/array/builder_base.h>
#include <arrow/builder.h>

#include "Column.h"

namespace normal::tuple {

class ColumnBuilder {

public:
  ColumnBuilder(std::string  name, const std::shared_ptr<::arrow::DataType>& type):name_(std::move(name)) {
	auto arrowStatus = ::arrow::MakeBuilder(::arrow::default_memory_pool(), type, &arrowBuilder_);
  }

  static std::shared_ptr<ColumnBuilder> make(const std::string& name, const std::shared_ptr<::arrow::DataType>& type){
	return std::make_unique<ColumnBuilder>(name, type);
  }

  void append(const std::shared_ptr<Scalar>& scalar){
    if(scalar->type()->id() == ::arrow::Int64Type::type_id){
      auto rawBuilderPtr = arrowBuilder_.get();
      auto typedArrowBuilder = dynamic_cast<::arrow::Int64Builder*>(rawBuilderPtr);
	  auto status = typedArrowBuilder->Append(scalar->value<long>());
    }
    else{
	  throw std::runtime_error(
		  "Builder for type '" + scalar->type()->ToString() + "' not implemented yet");
    }
  }

  std::shared_ptr<Column> finalize(){
	auto status = arrowBuilder_->Finish(&array_);
	auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(array_);
	return std::make_shared<Column>(name_, chunkedArray);
  }

private:
  std::string name_;
  std::unique_ptr<::arrow::ArrayBuilder> arrowBuilder_;
  std::shared_ptr<::arrow::Array> array_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNBUILDER_H
