//
// Created by Yifei Yang on 8/16/20.
//


#include <filesystem>
#include <fstream>
#include <normal/ssb/SqlGenerator.h>

using namespace normal::ssb;

void writeFile(std::string content, std::filesystem::path &filePath) {
  std::ofstream file(filePath.string());
  file << content;
}

int main() {
  const int batchSize = 10;
  SqlGenerator sqlGenerator;
  auto sqls = sqlGenerator.generateSqlBatchSkew(batchSize);

  auto sql_file_dir_path = std::filesystem::current_path().append("sql/generated");
  if (!std::filesystem::exists(sql_file_dir_path)) {
    std::filesystem::create_directory(sql_file_dir_path);
  }

  int index = 1;
  for (auto const &sql: sqls) {
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index++));
    writeFile(sql, sql_file_path);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  return 0;
}
