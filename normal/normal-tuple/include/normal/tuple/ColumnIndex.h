//
// Created by matt on 4/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNINDEX_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNINDEX_H

namespace normal::tuple {

/**
 * An index into an arrow chunked array
 */
class ColumnIndex {

public:
  ColumnIndex(int chunk, long chunkIndex);

  void setChunk(int chunk);

  void setChunkIndex(long chunkIndex);

  [[nodiscard]] int getChunk() const;

  [[nodiscard]] long getChunkIndex() const;

private:
  int chunk_;
  long chunkIndex_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNINDEX_H
