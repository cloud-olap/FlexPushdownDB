//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_OR_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_OR_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"

namespace normal::expression::gandiva {

class Or : public Expression {

public:
    Or(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

    void compile(std::shared_ptr<arrow::Schema> schema) override;
    std::string alias() override;

private:
    std::shared_ptr<Expression> left_;
    std::shared_ptr<Expression> right_;

};

std::shared_ptr<Expression> or_(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

}


#endif //NORMAL_OR_H
