//
// Created by matt on 4/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNINDEX_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNINDEX_H

namespace fpdb::tuple {

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

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNINDEX_H
