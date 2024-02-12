//
// Created by matt on 18/6/20.
//


#include "normal/ssb/SQLite3.h"

#include <experimental/filesystem>

#include <sqlite3.h>
#include <tl/expected.hpp>
#include <fmt/format.h>
#include <unistd.h>

#include <normal/ssb/Globals.h>

using namespace normal::ssb;
using namespace std::experimental;

extern "C" {
  int sqlite3_csv_init(
	  sqlite3 *db,
	  char **pzErrMsg,
	  const sqlite3_api_routines *pApi
  );
}

tl::expected<std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>>,
			 std::string> SQLite3::execute(const std::string& sql, const std::vector<std::string>& files) {

  SPDLOG_DEBUG("SQL  |\n{}", sql);

  sqlite3 *db;
  int rc;
  char *errorMessage;

  auto template_ = fmt::format("{}/normal-XXXXXX", filesystem::temp_directory_path().generic_string());
  auto dbDir = mkdtemp(template_.data());
  filesystem::create_directories(dbDir);
  auto dbFile = fmt::format("{}/{}", dbDir, "sqlite3.db");

  rc = sqlite3_open(dbFile.c_str(), &db);
  if (rc) {
	sqlite3_close(db);
	filesystem::remove(dbFile);
	return tl::unexpected(fmt::format("Failed to create database. {}", sqlite3_errmsg(db)));
  }

  sqlite3_csv_init(db, &errorMessage, nullptr);

  /**
   * Sample code to load csv extension dynamically
   */
//  sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL);

//  rc = sqlite3_load_extension(db, "libcsv", 0, &errorMessage);
//  if (rc) {
//	sqlite3_close(db);
//	filesystem::remove(dbFile);
//	return tl::unexpected(fmt::format("Failed to load csv extension. {}", errorMessage));
//  }

  for (const auto &file: files) {
	auto filename = filesystem::path(file).filename().replace_extension();
	rc = sqlite3_exec(db,
					  fmt::format("CREATE VIRTUAL TABLE temp.{} USING csv(header=true,filename='{}');",
								  filename.generic_string(),
								  file).c_str(),
					  [](void *, int , char **, char **) {
						return 0;
					  },
					  nullptr,
					  &errorMessage);
	if (rc) {
	  sqlite3_close(db);
	  filesystem::remove(dbFile);
	  return tl::unexpected(fmt::format("Failed to create virtual table for file '{}'. {}", file, errorMessage));
	}
  }

  auto results = std::make_shared<std::vector<std::vector<std::pair<std::string, std::string>>>>();

  rc = sqlite3_exec(db,
					sql.c_str(),
					[](void *results, int argc, char **argv, char **azColName) {
					  auto typedResults = static_cast<std::vector<std::vector<std::pair<std::string, std::string>>> *>(results);
					  std::vector<std::pair<std::string, std::string>> row;
					  int i;
					  for (i = 0; i < argc; i++) {
						row.push_back(std::pair{azColName[i], argv[i] ? argv[i] : "NULL"});
					  }
					  typedResults->push_back(row);
					  return 0;
					},
					results.get(),
					&errorMessage);

  sqlite3_close(db);
  filesystem::remove(dbFile);

  if (rc) {
	return tl::unexpected(fmt::format("Failed execute SQL. {}", errorMessage));
  }

  return results;
}
