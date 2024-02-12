//
// Created by matt on 19/5/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <fpdb/catalogue/local-fs/LocalFSPartition.h>

#include <fpdb/cache/SegmentCache.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/cache/SegmentData.h>
#include <fpdb/cache/SegmentRange.h>
#include <fpdb/cache/policy/LRUCachingPolicy.h>

using namespace fpdb::cache;
using namespace fpdb::tuple;
using namespace fpdb::catalogue::local_fs;

namespace fpdb::cache::test {

#define SKIP_SUITE false

TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

/**
 * Tests the equality and hash functions work for segment keys
 */
TEST_CASE ("segmentkey-equality" * doctest::skip(false || SKIP_SUITE)) {

  auto partition1 = std::make_shared<LocalFSPartition>("data/a.csv");
  auto segmentKey1 = SegmentKey::make(partition1, "a", SegmentRange::make(0, 1023));

  // An equal segment key
  auto partition2 = std::make_shared<LocalFSPartition>("data/a.csv");
  auto segmentKey2 = SegmentKey::make(partition2, "a",SegmentRange::make(0, 1023));
	  CHECK_EQ(*segmentKey1, *segmentKey2);
	  CHECK_EQ(segmentKey1->hash(), segmentKey2->hash());

  // A non equal segment key (different partition)
  auto partition3 = std::make_shared<LocalFSPartition>("data/b.csv");
  auto segmentKey3 = SegmentKey::make(partition3, "a",SegmentRange::make(0, 1023));
	  CHECK_NE(*segmentKey1, *segmentKey3);
	  CHECK_NE(segmentKey1->hash(), segmentKey3->hash());

  // A non equal segment key (different range)
  auto partition4 = std::make_shared<LocalFSPartition>("data/a.csv");
  auto segmentKey4 = SegmentKey::make(partition4, "a",SegmentRange::make(1024, 2047));
	  CHECK_NE(*segmentKey1, *segmentKey4);
	  CHECK_NE(segmentKey1->hash(), segmentKey4->hash());
}

/**
 * Tests we can store and retrieve and entry in the cache
 */
TEST_CASE ("cache-hit" * doctest::skip(false || SKIP_SUITE)) {

//  auto cache = SegmentCache::make(
//          std::make_shared<LRUCachingPolicy>(100,
//                                             std::make_shared<Mode>(ModeId::PULL_UP),
//                                             nullptr));
//
//  auto segment1Partition1 = std::make_shared<LocalFSPartition>("data/a.csv");
//  auto segment1Key1 = SegmentKey::make(segment1Partition1, "a",SegmentRange::make(0, 1023));
//
//  auto segment1Column1 = Column::make("a", ::arrow::utf8());
//  auto segment1Data1 = SegmentData::make(segment1Column1);
//
//  cache->store(segment1Key1, segment1Data1);
//
//	  CHECK_EQ(cache->getSize(), 1);
//
//  // An equal segment key
//  auto segment1Partition2 = std::make_shared<LocalFSPartition>("data/a.csv");
//  auto segment1Key2 = SegmentKey::make(segment1Partition2, "a", SegmentRange::make(0, 1023));
//
//  auto expectedSegment1CacheEntry = cache->load(segment1Key2);
//
//  // Check its a hit
//  if (expectedSegment1CacheEntry.has_value()) {
//	auto segment1CacheEntry = expectedSegment1CacheEntry.value();
//		CHECK_EQ(segment1Data1, segment1CacheEntry);
//  }
//  else{
//    FAIL(expectedSegment1CacheEntry.error());
//  }
}

/**
 * Tests we can store en entry and loading a different entry results in a miss
 */
TEST_CASE ("cache-miss" * doctest::skip(false || SKIP_SUITE)) {

//  auto cache = SegmentCache::make(
//          std::make_shared<LRUCachingPolicy>(100,
//                                             std::make_shared<Mode>(ModeId::PULL_UP),
//                                             nullptr));
//
//  auto segment1Partition1 = std::make_shared<LocalFSPartition>("data/a.csv");
//  auto segment1Key1 = SegmentKey::make(segment1Partition1, "a", SegmentRange::make(0, 1023));
//
//  auto segment1Column1 = Column::make("a", ::arrow::utf8());
//  auto segment1Data1 = SegmentData::make(segment1Column1);
//
//  cache->store(segment1Key1, segment1Data1);
//
//	  CHECK_EQ(cache->getSize(), 1);
//
//  // A non equal segment key (different partition)
//  auto segment2Partition = std::make_shared<LocalFSPartition>("data/b.csv");
//  auto segment2Key = SegmentKey::make(segment2Partition, "a", SegmentRange::make(0, 1023));
//
//  // Check its a miss
//  auto expectedSegment2Data = cache->load(segment2Key);
//	  CHECK_FALSE(expectedSegment2Data.has_value());
}

/**
 * Tests storing two entries in a 1 entry cache results in the first being evicted
 */
TEST_CASE ("cache-eviction" * doctest::skip(false || SKIP_SUITE)) {

//  auto cache = SegmentCache::make(
//          std::make_shared<LRUCachingPolicy>(100,
//                                             std::make_shared<Mode>(ModeId::PULL_UP),
//                                             nullptr));
//
//  auto segment1Partition1 = std::make_shared<LocalFSPartition>("data/a.csv");
//  auto segment1Key1 = SegmentKey::make(segment1Partition1, "a", SegmentRange::make(0, 1023));
//
//  auto segment1Column1 = Column::make("a", ::arrow::utf8());
//  auto segment1Data1 = SegmentData::make(segment1Column1);
//
//  cache->store(segment1Key1, segment1Data1);
//
//	  CHECK_EQ(cache->getSize(), 1);
//
//  // A non equal segment key (different partition)
//  auto segment2Partition = std::make_shared<LocalFSPartition>("data/b.csv");
//  auto segment2Key = SegmentKey::make(segment2Partition, "a", SegmentRange::make(0, 1023));
//
//  auto segment2Column1 = Column::make("b", ::arrow::utf8());
//  auto segment2Data1 = SegmentData::make(segment1Column1);
//
//  cache->store(segment2Key, segment2Data1);
//
//	  CHECK_EQ(cache->getSize(), 1);
//
//  // Check its a miss on the first key
//  auto expectedSegment1Data = cache->load(segment1Key1);
//	  CHECK_FALSE(expectedSegment1Data.has_value());
//
//  // Check its a hit on the second key
//  auto expectedSegment2Data = cache->load(segment2Key);
//	  CHECK(expectedSegment2Data.has_value());
}

/**
 * Test storing and removing entries
 */
TEST_CASE ("cache-remove" * doctest::skip(true || SKIP_SUITE)) {

//  auto cache = SegmentCache::make(
//          std::make_shared<LRUCachingPolicy>(100,
//                                             std::make_shared<Mode>(ModeId::PULL_UP),
//                                             nullptr));
//
//  auto segment1Partition1 = std::make_shared<LocalFSPartition>("data/a.csv");
//  auto segment1Key1 = SegmentKey::make(segment1Partition1, "a", SegmentRange::make(0, 1023));
//
//  auto segment1Column1 = Column::make("a", ::arrow::utf8());
//  auto segment1Data1 = SegmentData::make(segment1Column1);
//
//  cache->store(segment1Key1, segment1Data1);
//
//  auto segment2Partition1 = std::make_shared<LocalFSPartition>("data/b.csv");
//  auto segment2Key1 = SegmentKey::make(segment2Partition1, "a", SegmentRange::make(0, 1023));
//
//  auto segment2Column1 = Column::make("a", ::arrow::utf8());
//  auto segment2Data1 = SegmentData::make(segment2Column1);
//
//  cache->store(segment2Key1, segment2Data1);
//
//  auto segment3Partition1 = std::make_shared<LocalFSPartition>("data/c.csv");
//  auto segment3Key1 = SegmentKey::make(segment3Partition1, "a", SegmentRange::make(0, 1023));
//
//  auto segment3Column1 = Column::make("a", ::arrow::utf8());
//  auto segment3Data1 = SegmentData::make(segment3Column1);
//
//  cache->store(segment3Key1, segment3Data1);
//
//  // Erase segment 1 using segment key
//  auto segment1NumErased = cache->remove(segment1Key1);
//	  CHECK_EQ(segment1NumErased, 1);
//
//  auto expectedSegment1Data = cache->load(segment1Key1);
//	  CHECK_FALSE(expectedSegment1Data.has_value());
//
//  // Erase segment 2 using a partition predicate
//  auto segment2NumErased = cache->remove([](const SegmentKey &key) {
//	auto typedPartition = std::static_pointer_cast<LocalFSPartition>(key.getPartition());
//	auto path = typedPartition->getPath();
//	return path == "data/b.csv";
//  });
//
//	  CHECK_EQ(segment2NumErased, 1);
//
//  auto expectedSegment2Data = cache->load(segment2Key1);
//	  CHECK_FALSE(expectedSegment1Data.has_value());
//
//  auto expectedSegment3Data = cache->load(segment3Key1);
//	  CHECK(expectedSegment3Data.has_value());

}

}

}