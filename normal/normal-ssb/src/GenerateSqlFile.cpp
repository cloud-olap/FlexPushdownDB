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

int main(int argc, char **argv) {
  // Directory
  auto sql_file_dir_path = std::experimental::filesystem::current_path().append("sql/generated");
  if (!std::experimental::filesystem::exists(sql_file_dir_path)) {
    std::experimental::filesystem::create_directory(sql_file_dir_path);
  }

  // Parameters
  auto type = atoi(argv[1]);
  auto size = atoi(argv[2]);
  auto skewness = atof(argv[3]);
  SqlGenerator sqlGenerator;
  std::random_device rd;
  std::mt19937 g(rd());

  // Workload
  switch (type) {

    // Basic workload
    case 1: {
      // make warm-up batch and execution batch the same, but random order respectively
      auto warmBatch = sqlGenerator.generateSqlBatchSkew(skewness, size / 2);
      std::vector<std::string> executionBatch(warmBatch);
      std::shuffle(warmBatch.begin(), warmBatch.end(), g);
      std::shuffle(executionBatch.begin(), executionBatch.end(), g);

      for (int index = 0; index < size; index++) {
        auto sql = (index < warmBatch.size()) ? warmBatch[index] : executionBatch[index - warmBatch.size()];
        auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", (index + 1)));
        writeFile(sql, sql_file_path);
        sql_file_dir_path = sql_file_dir_path.parent_path();
      }
      break;
    }

    // Weighted workload
    case 2: {
      // make warm-up batch and execution batch the same, but random order respectively
      auto warmBatch = sqlGenerator.generateSqlBatchSkewWeight(skewness, size / 2);
      std::vector<std::string> executionBatch(warmBatch);
      std::shuffle(warmBatch.begin(), warmBatch.end(), g);
      std::shuffle(executionBatch.begin(), executionBatch.end(), g);

      for (int index = 0; index < size; index++) {
        auto sql = (index < warmBatch.size()) ? warmBatch[index] : executionBatch[index - warmBatch.size()];
        auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", (index + 1)));
        writeFile(sql, sql_file_path);
        sql_file_dir_path = sql_file_dir_path.parent_path();
      }
      break;
    }

    // Recurring workload
    case 3: {
      auto recurSize = atoi(argv[4]);
      auto recurBatch = sqlGenerator.generateSqlBatchSkew(skewness, recurSize);
      for (int index = 0; index < size; index++) {
        auto sql = recurBatch[index % recurSize];
        auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", (index + 1)));
        writeFile(sql, sql_file_path);
        sql_file_dir_path = sql_file_dir_path.parent_path();
      }
      break;
    }

    // One-hot workload
    case 4: {
      // TODO
      break;
    }

    default:
      throw std::runtime_error("Workload tyoe not found, type: " + std::to_string(type));
  }

  return 0;
}
