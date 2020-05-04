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
  ColumnIndex(int chunk, long chunkIndex) :
	  chunk_(chunk), chunkIndex_(chunkIndex) {}

  void setChunk(int chunk) {
	chunk_ = chunk;
  }

  void setChunkIndex(long chunkIndex) {
	chunkIndex_ = chunkIndex;
  }

  [[nodiscard]] int getChunk() const {
	return chunk_;
  }

  [[nodiscard]] long getChunkIndex() const {
	return chunkIndex_;
  }

private:
  int chunk_;
  long chunkIndex_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNINDEX_H
