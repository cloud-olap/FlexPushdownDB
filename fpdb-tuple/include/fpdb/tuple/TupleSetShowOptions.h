//
// Created by matt on 6/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETSHOWOPTIONS_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETSHOWOPTIONS_H

namespace fpdb::tuple {

enum TupleSetShowOrientation {
  ColumnOriented,
  RowOriented
};

class TupleSetShowOptions {

public:

  explicit TupleSetShowOptions(TupleSetShowOrientation  Orientation, int maxNumRows = 10);
  [[nodiscard]] TupleSetShowOrientation getOrientation() const;
  [[nodiscard]] int getMaxNumRows() const;
private:
  TupleSetShowOrientation orientation_;
  int maxNumRows_;
};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETSHOWOPTIONS_H
