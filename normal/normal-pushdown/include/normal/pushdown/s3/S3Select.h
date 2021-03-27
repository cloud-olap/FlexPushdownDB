//
// Created by Matt Woicik on 1/19/21.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECT_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECT_H

#include "normal/pushdown/s3/S3SelectScan.h"
#ifdef __AVX2__
#include "normal/tuple/arrow/CSVToArrowSIMDChunkParser.h"
#endif

namespace normal::pushdown {
class S3Select: public S3SelectScan {
  public:
    S3Select(std::string name,
           std::string s3Bucket,
           std::string s3Object,
           std::string filterSql,
           std::vector<std::string> returnedS3ColumnNames,
           std::vector<std::string> neededColumnNames,
           int64_t startOffset,
           int64_t finishOffset,
           std::shared_ptr<arrow::Schema> schema,
           std::shared_ptr<Aws::S3::S3Client> s3Client,
           bool scanOnStart,
           bool toCache,
           long queryId,
           std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys);

    static std::shared_ptr<S3Select> make(const std::string& name,
                        const std::string& s3Bucket,
                        const std::string& s3Object,
                        const std::string& filterSql,
                        const std::vector<std::string>& returnedS3ColumnNames,
                        const std::vector<std::string>& neededColumnNames,
                        int64_t startOffset,
                        int64_t finishOffset,
                        const std::shared_ptr<arrow::Schema>& schema,
                        const std::shared_ptr<Aws::S3::S3Client>& s3Client,
                        bool scanOnStart = true,
                        bool toCache = false,
                        long queryId = 0,
                        const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>& weightedSegmentKeys = nullptr);


  private:
    std::string filterSql_;   // "where ...."
    std::shared_ptr<S3CSVParser> parser_;

    // Used for collecting all results for split requests that are run in parallel, and for having a
    // locks on shared variables when requests are split.
    std::mutex splitReqLock_;
    std::map<int, std::shared_ptr<arrow::Table>> splitReqNumToTable_;

#ifdef __AVX2__
    std::shared_ptr<CSVToArrowSIMDChunkParser> generateSIMDParser();
#endif
    std::shared_ptr<S3CSVParser> generateParser();
    Aws::Vector<unsigned char> s3Result_{};

    // Scan range not supported in AWS for GZIP and BZIP2 CSV. We also don't support this for parquet yet either
    // as that is more complicated due to involving parquet metadata and we haven't had a chance to implement this yet
    bool scanRangeSupported();
    Aws::S3::Model::InputSerialization getInputSerialization();
    std::shared_ptr<TupleSet2> s3Select(int64_t startOffset, int64_t endOffset);
    std::shared_ptr<TupleSet2> s3SelectParallelReqs();
    // Wrapper function to encapsulate a thread spawned when making parallel requests
    void s3SelectIndividualReq(int reqNum, int64_t startOffset, int64_t endOffset);

    void processScanMessage(const scan::ScanMessage &message) override;

    std::shared_ptr<TupleSet2> readTuples() override;
    int getPredicateNum() override;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECT_H
