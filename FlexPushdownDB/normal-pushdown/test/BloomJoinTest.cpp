//
// Created by matt on 5/8/20.
//


#include <memory>
#include <filesystem>

#include <doctest/doctest.h>

#include <normal/tuple/Sample.h>
#include <normal/pushdown/bloomjoin/BloomCreateKernel.h>
#include <normal/pushdown/bloomjoin/FileScanBloomUseKernel.h>

using namespace normal::tuple;

#define SKIP_SUITE true

TEST_SUITE ("bloom-join" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bloom-join-create" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = Sample::sampleCxRInt<int, ::arrow::Int32Type>(5, 1000, std::uniform_int_distribution<int>(0,1000000));

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 10)));

  auto createKernel = BloomCreateKernel::make("c_0",
											  0.1,
											  {"select * from s3Object where"});
	  CHECK(createKernel->addTupleSet(inputTupleSet));
	  CHECK(createKernel->buildBloomFilter());
  auto expectedBloomFilter = createKernel->getBloomFilter();
	  CHECK(expectedBloomFilter.has_value());
  auto bloomFilter = expectedBloomFilter.value();

  for (int r = 0; r < inputTupleSet->numRows(); ++r) {
	auto expectedValue = inputTupleSet->toTupleSetV1()->value<::arrow::Int32Type>("c_0", r);
		CHECK(expectedValue.has_value());
		DOCTEST_CHECK_MESSAGE(bloomFilter->contains(expectedValue.value()),
					  fmt::format("Bloom filter does not contain {}", expectedValue.value()));
  }
}

TEST_CASE ("bloom-join-create-use" * doctest::skip(false || SKIP_SUITE)) {

  auto leftInputTupleSet = Sample::sampleCxRInt<int, ::arrow::Int32Type>(5, 10);

  SPDLOG_DEBUG("Left Input:\n{}",
			   leftInputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 10)));

  auto rightFile = std::filesystem::absolute("data/bloomjoin/right.csv");
  auto numBytesRightFile = std::filesystem::file_size(rightFile);

  auto createKernel = BloomCreateKernel::make("c_0",
											  0.3,
											  {"select * from s3Object where"});
	  CHECK(createKernel->addTupleSet(leftInputTupleSet));
	  CHECK(createKernel->buildBloomFilter());
  auto expectedBloomFilter = createKernel->getBloomFilter();
	  CHECK(expectedBloomFilter.has_value());
  auto bloomFilter = expectedBloomFilter.value();

  auto useKernel = FileScanBloomUseKernel::make(rightFile,
												{"r_0", "r_1", "r_2"},
												0,
												numBytesRightFile,
												"r_0");
	  CHECK(useKernel->setBloomFilter(bloomFilter));
	  CHECK(useKernel->scan({"r_0", "r_1", "r_2"}));

  auto expectedRightInputTupleSet = useKernel->getTupleSet();
	  CHECK(expectedRightInputTupleSet.has_value());
  auto rightInputTupleSet = expectedRightInputTupleSet.value();
  SPDLOG_DEBUG("Right Input:\n{}",
			   rightInputTupleSet->showString(
				   TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 20)));

	  CHECK(useKernel->filter());

  auto expectedOutputTupleSet = useKernel->getTupleSet();
	  CHECK(expectedOutputTupleSet.has_value());
  auto outputTupleSet = expectedOutputTupleSet.value();
  SPDLOG_DEBUG("Output:\n{}",
			   outputTupleSet->showString(
				   TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 20)));

  for (int lr = 0; lr < leftInputTupleSet->numRows(); ++lr) {
	auto leftValue = leftInputTupleSet->toTupleSetV1()->value<::arrow::Int32Type>("c_0", lr);

	for (int rr = 0; rr < rightInputTupleSet->numRows(); ++rr) {
	  auto rightValue1 = rightInputTupleSet->toTupleSetV1()->getString("r_0", rr);
	  auto rightValue2 = rightInputTupleSet->toTupleSetV1()->getString("r_1", rr);
	  auto rightValue3 = rightInputTupleSet->toTupleSetV1()->getString("r_2", rr);
	  if (rightValue1 == std::to_string(leftValue.value())) {
		bool rowFoundInOutput = false;
		for (int or_ = 0; or_ < outputTupleSet->numRows(); ++or_) {
		  auto outputValue1 = outputTupleSet->toTupleSetV1()->getString("r_0", or_);
		  auto outputValue2 = outputTupleSet->toTupleSetV1()->getString("r_1", or_);
		  auto outputValue3 = outputTupleSet->toTupleSetV1()->getString("r_2", or_);
		  if (outputValue1 == rightValue1 && outputValue2 == rightValue2 && outputValue3 == rightValue3) {
			rowFoundInOutput = true;
			break;
		  }
		}
			CHECK_MESSAGE(rowFoundInOutput, fmt::format("Matching right row {} not found in output", rr));
	  }
	}
  }
}

}