//
// Created by Matt Woicik on 2/22/21.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARROWGZIPCSVINPUTSTREAM3_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARROWGZIPCSVINPUTSTREAM3_H

#include <arrow/io/interfaces.h>
#include <libdeflate.h>

class ArrowAWSGZIPInputStream3 : public arrow::io::InputStream {
  public:
    explicit ArrowAWSGZIPInputStream3(std::basic_iostream<char, std::char_traits<char>> &file, int64_t inputSize);
    ~ArrowAWSGZIPInputStream3();

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

    /// \brief Return the position in this stream
    arrow::Result<int64_t> Tell() const override;

    /// \brief Return whether the stream is closed
    bool closed() const override;

    int64_t getDecompressionTimeNS();


  protected:
    int64_t processedCompressedBytes_ = 0;
    int64_t returnedUncompressedBytes_ = 0;
    int64_t decompressionTimeNS_ = 0;
    std::vector<char*> allocations_;
    char* outputBytes_;
    int64_t outputBytesLocation_;
    int64_t outputBytesRemaining_;
};


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARROWGZIPCSVINPUTSTREAM2_H