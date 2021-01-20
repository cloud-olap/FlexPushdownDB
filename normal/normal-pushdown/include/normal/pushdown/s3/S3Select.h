//
// Created by Matt Woicik on 1/19/21.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECT_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECT_H

#include "normal/pushdown/s3/S3SelectScan.h"

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
    std::shared_ptr<S3SelectParser> parser_;

    [[nodiscard]] tl::expected<void, std::string> s3Select();

    void processScanMessage(const scan::ScanMessage &message) override;

    std::shared_ptr<TupleSet2> readTuples() override;
    int getPredicateNum() override;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECT_H
