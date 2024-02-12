//
// Created by Yifei Yang on 3/2/21.
//

#ifndef NORMAL_NORMAL_SSB_EXPERIMENT_MATHMODELTEST_H
#define NORMAL_NORMAL_SSB_EXPERIMENT_MATHMODELTEST_H
//
//#include <iostream>
//#include <normal/plan/Mode.h>
//#include <vector>
//#include <map>
//
//namespace normal::ssb {
//
///**
// * Metrics of multiple runs
// */
//struct MetricsVec {
//  std::vector<double> executionTimeVec;
//  std::vector<size_t> processedBytesVec;
//  std::vector<size_t> returnedBytesVec;
//  std::vector<size_t> getTransferNSVec;
//  std::vector<size_t> getConvertNSVec;
//  std::vector<size_t> selectTransferNSVec;
//  std::vector<size_t> selectConvertNSVec;
//  std::vector<double> hitRatioVec;
//};
//
//class MathModelTest {
//
//public:
//  MathModelTest(size_t networkLimit, size_t chunkSize, int numRuns);
//
//    /**
//     * Run two consecutive queries in all modes for math model evaluation
//     */
//  void runTest();
//
//private:
//  void runTestSingleMode(const std::shared_ptr<normal::plan::operator_::mode::Mode> &mode,
//                         bool saveMetrics);
//
//  const size_t cacheSize_ = 64L * 1024 * 1024 * 1024;   // make it large enough
//  const std::string bucketName_ = "pushdowndb";
//  const std::string dirPrefix_ = "ssb-sf100-sortlineorder/csv/";
//
//  size_t networkLimit_;
//  size_t chunkSize_;
//  int numRuns_;
//
//  // metrics for calculating average
//  std::map<std::string, MetricsVec> metricsMap_;
//  std::vector<double> localSpeedVec_;
//};
//
//}
//

#endif //NORMAL_NORMAL_SSB_EXPERIMENT_MATHMODELTEST_H
