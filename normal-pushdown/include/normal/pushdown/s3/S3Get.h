//
// Created by Matt Woicik on 1/19/21.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3GET_H
#define NORMAL_NORMAL_CORE_SRC_S3GET_H

#include "normal/pushdown/s3/S3SelectScan.h"
#include <aws/s3/model/GetObjectResult.h>

#ifdef __AVX2__
#include "normal/tuple/arrow/CSVToArrowSIMDChunkParser.h"
#endif

namespace normal::pushdown::s3 {

// This is for controlling the maximum number of GET requests converting data at the same time
// as per maxConcurrentGetConversions in normal-pushdown/include/normal/pushdown/Globals.h
extern std::mutex GetConvertLock;
extern int activeGetConversions;

class S3Get : public S3SelectScan {
  public:
    S3Get(std::string name,
             std::string s3Bucket,
             std::string s3Object,
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

    static std::shared_ptr<S3Get> make(const std::string& name,
                                          const std::string& s3Bucket,
                                          const std::string& s3Object,
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
    std::shared_ptr<TupleSet2> readCSVFile(std::shared_ptr<arrow::io::InputStream> &arrowInputStream);
    std::shared_ptr<avro::avroTuple> S3Get::readAvroFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile, const char* schemaName);
    std::shared_ptr<TupleSet2> readParquetFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile);
    std::shared_ptr<TupleSet2> s3GetFullRequest();
    Aws::S3::Model::GetObjectResult s3GetRequestOnly(const std::string &s3Object, uint64_t startOffset, uint64_t endOffset);

    // Whether we can process different portions of the response in parallel
    // For now we only support this for uncompressed CSV, but eventually we work
    // with parquet more we should be able to turn on a flag to have arrow do this as well,
    // the methods will just be a bit different
    bool parallelTuplesetCreationSupported();

#ifdef __AVX2__
    void s3GetIndividualReq(int reqNum, const std::string &s3Object, uint64_t startOffset, uint64_t endOffset);
    std::shared_ptr<TupleSet2> s3GetParallelReqs(bool tempFixForAirmettleCSV150MB);
#endif

    // Used for collecting all results for split requests that are run in parallel, and for having a
    // locks on shared variables when requests are split.
    std::mutex splitReqLock_;
    std::map<int, std::shared_ptr<arrow::Table>> splitReqNumToTable_;
    std::unordered_map<int, std::vector<char>> reqNumToAdditionalOutput_;
    #ifdef __AVX2__
    std::unordered_map<int, std::shared_ptr<CSVToArrowSIMDChunkParser>> reqNumToParser_;
    #endif


    void processScanMessage(const scan::ScanMessage &message) override;

    std::shared_ptr<TupleSet2> readTuples() override;
    int getPredicateNum() override;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3GET_H
