//
// Created by matt on 5/3/20.
//


#include <doctest/doctest.h>
#include <Interpreter.h>
#include <connector/s3/S3SelectConnector.h>
#include <connector/Catalogue.h>
#include <connector/s3/S3SelectCatalogueEntry.h>
#include <connector/local-fs/LocalFileSystemConnector.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include "Globals.h"

TEST_CASE ("SQL (S3SelectScan -> Sum -> Collate)"
               * doctest::skip(true)) {

  Interpreter i;

  auto conn =  std::make_shared<S3SelectConnector>("s3-select");

  auto cat = std::make_shared<Catalogue>("s3-select-catalogue");
  cat->put(std::make_shared<S3SelectCatalogueEntry>("customer", "s3Filter", "tpch-sf1/customer.csv"));
  i.put(cat);

  i.parse("select * from customer");
}

TEST_CASE ("SQL (FileScan -> Sum -> Collate)"
               * doctest::skip(false)) {

  SPDLOG_DEBUG("Started");

  Interpreter i;

  auto conn = std::make_shared<LocalFileSystemConnector>("local_fs");

  auto cat = std::make_shared<Catalogue>("local_fs");
  cat->put(std::make_shared<LocalFileSystemCatalogueEntry>("test", "data/data-file-simple/test.csv"));
  i.put(cat);

  i.parse("select * from local_fs.test");

  SPDLOG_DEBUG("Finished");
}
