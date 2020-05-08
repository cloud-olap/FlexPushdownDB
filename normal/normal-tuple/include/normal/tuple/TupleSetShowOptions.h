//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETSHOWOPTIONS_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETSHOWOPTIONS_H

namespace normal::tuple {

enum TupleSetShowOrientation {
  ColumnOriented,
  RowOriented
};

class TupleSetShowOptions {

public:

  explicit TupleSetShowOptions(TupleSetShowOrientation  Orientation);
  [[nodiscard]] TupleSetShowOrientation getOrientation() const;

private:
  TupleSetShowOrientation orientation_;
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETSHOWOPTIONS_H
