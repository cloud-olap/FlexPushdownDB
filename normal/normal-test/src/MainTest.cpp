//
// Created by matt on 4/12/19.
//

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>
#include <spdlog/spdlog.h>
#include "normal/pushdown/S3SelectScan.h"
#include "normal/pushdown/Collate.h"
#include "normal/core/OperatorContext.h"
#include <normal/core/OperatorManager.h>
#include <string>
#include <memory>
#include <vector>

TEST_CASE ("Testing Operator lifecycle") {
  auto s3selectScan = std::make_shared<S3SelectScan>("s3selectScan");
  auto ctx = std::make_shared<OperatorContext>(s3selectScan);

  s3selectScan->start(ctx);
      CHECK(s3selectScan->running());

  s3selectScan->stop();
      CHECK(!s3selectScan->running());
}

TEST_CASE ("Testing OperatorContext lifecycle") {

  auto s3selectScan1 = std::make_shared<S3SelectScan>("s3selectScan1");
  auto s3selectScan2 = std::make_shared<S3SelectScan>("s3selectScan2");
  auto s3selectScan3 = std::make_shared<S3SelectScan>("s3selectScan3");
  auto mgr = std::make_shared<OperatorManager>();

  mgr->put(s3selectScan1);
  mgr->put(s3selectScan2);
  mgr->put(s3selectScan3);

  mgr->start();
  mgr->stop();

}

TEST_CASE ("Testing OperatorContext execution") {

  auto s3selectScan1 = std::make_shared<S3SelectScan>("s3selectScan1");
  auto s3selectScan2 = std::make_shared<S3SelectScan>("s3selectScan2");
  auto s3selectScan3 = std::make_shared<S3SelectScan>("s3selectScan3");
  auto mgr = std::make_shared<OperatorManager>();

  mgr->put(s3selectScan1);
  mgr->put(s3selectScan2);
  mgr->put(s3selectScan3);

  mgr->start();
  mgr->stop();
}

TEST_CASE ("Testing Operator graph") {

  spdlog::info("Test");

  auto s3selectScan1 = std::make_shared<S3SelectScan>("s3selectScan1");
  auto s3selectScan2 = std::make_shared<S3SelectScan>("s3selectScan2");
  auto s3selectScan3 = std::make_shared<S3SelectScan>("s3selectScan3");
  auto collate = std::make_shared<Collate>("collate");

  s3selectScan1->produce(s3selectScan2);
  s3selectScan2->consume(s3selectScan1);

  s3selectScan2->produce(s3selectScan3);
  s3selectScan3->consume(s3selectScan2);

  s3selectScan3->produce(collate);
  collate->consume(s3selectScan3);

  auto mgr = std::make_shared<OperatorManager>();

  mgr->put(s3selectScan1);
  mgr->put(s3selectScan2);
  mgr->put(s3selectScan3);

  mgr->start();
  mgr->stop();

  collate->show();
}