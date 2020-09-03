//
// Created by Yifei Yang on 8/16/20.
//


#include <experimental/filesystem>
#include <fstream>
#include <normal/ssb/SqlGenerator.h>

using namespace normal::ssb;

void writeFile(std::string content, std::experimental::filesystem::path &filePath) {
  std::ofstream file(filePath.string());
  file << content;
}

int main() {
  const int batchSize = 100, recurringTimes = 1;
  SqlGenerator sqlGenerator;
  auto sqls = sqlGenerator.generateSqlBatchSkew(batchSize);

  auto sql_file_dir_path = std::experimental::filesystem::current_path().append("sql/generated");
  if (!std::experimental::filesystem::exists(sql_file_dir_path)) {
    std::experimental::filesystem::create_directory(sql_file_dir_path);
  }

  for (int index = 0; index < batchSize; index++) {
    auto sql = sqls[index];
    for (int j = 0; j < recurringTimes; j++) {
      auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", (index + 1) + j * batchSize));
      writeFile(sql, sql_file_path);
      sql_file_dir_path = sql_file_dir_path.parent_path();
    }
  }

  return 0;
}
