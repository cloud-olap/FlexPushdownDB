//
// Created by Matt Woicik on 2/15/21.
//

#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARROWGZIPCSVINPUTSTREAM_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARROWGZIPCSVINPUTSTREAM_H

#include <arrow/io/interfaces.h>
#include <zlib.h>
#include <zconf.h>

class [[maybe_unused]] ArrowGzipInputStream : public arrow::io::InputStream {
public:
  [[maybe_unused]] explicit ArrowGzipInputStream(std::basic_istream<char, std::char_traits<char>> &file);
  ~ArrowGzipInputStream() override;

  void resetZStream(int64_t bytesToRead);

  /// \brief Read data from current file position.
  ///
  /// Read at most `nbytes` from the current file position into `out`.
  /// The number of bytes read is returned.
  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;

  /// \brief Read data from current file position.
  ///
  /// Read at most `nbytes` from the current file position. Less bytes may
  /// be read if EOF is reached. This method updates the current file position.
  ///
  /// In some cases (e.g. a memory-mapped file), this method may avoid a
  /// memory copy.
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

  arrow::Status Close() override;

  /// \brief Return the position in this stream, when complete this is the total number of bytes read from
  /// the passed in file
  [[nodiscard]] arrow::Result<int64_t> Tell() const override;

  /// \brief Return whether the stream is closed
  [[nodiscard]] bool closed() const override;

  [[maybe_unused]] int64_t getDecompressionTimeNS() const;


protected:
  std::basic_istream<char, std::char_traits<char>>& underlyingFile_;
  int64_t processedCompressedBytes = 0;
  int64_t returnedUncompressedBytes = 0;
  int64_t decompressionTimeNS_ = 0;
  bool underlyingFileEmpty_ = false; // set to true once all bytes have been read from underlying file
  std::vector<char*> allocations_;
  z_stream currentZStream_{};
};


#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARROWGZIPCSVINPUTSTREAM_H
