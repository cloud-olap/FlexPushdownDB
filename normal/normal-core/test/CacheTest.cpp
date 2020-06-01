//
// Created by matt on 20/5/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/connector/local-fs/LocalFilePartition.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentRange.h>
#include <normal/cache/SegmentData.h>

#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/core/OperatorManager.h>
#include <normal/tuple/Sample.h>

using namespace normal::tuple;
using namespace normal::core::cache;

namespace normal::core::test {

#define SKIP_SUITE false

TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("load" * doctest::skip(false || SKIP_SUITE)) {

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto partition1 = std::make_shared<LocalFilePartition>("data/a.csv");
  auto segmentKey1 = SegmentKey::make(partition1, "a", SegmentRange::make(0, 1023));
  auto segment1TupleSet1 = Sample::sample3String();
  auto segment1Data1 = SegmentData::make(segment1TupleSet1);
  auto segmentKeys = {segmentKey1};

  mgr->send(StoreRequestMessage::make(segmentKey1, segment1Data1, "root"), "SegmentCache")
	  .map_error([](auto err) { throw std::runtime_error(err); });
  mgr->send(LoadRequestMessage::make(segmentKeys, "root"), "SegmentCache")
	  .map_error([](auto err) { throw std::runtime_error(err); });

  auto msg = mgr->receive();

  auto loadResponseMessage = std::static_pointer_cast<LoadResponseMessage>(msg);
	  CHECK_GT(loadResponseMessage->getSegments().size(), 0);

  auto segmentData = loadResponseMessage->getSegments().at(0);
	  CHECK(segmentData->getColumn()->numRows() == segment1Data1->getColumn()->numRows());

  // TODO: Check cell by cell

  mgr->stop();
}

}

}