//
// Created by Yifei Yang on 3/29/21.
//

#include <normal/ssb/SqlTransformer.h>
#include <vector>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <string>
#include <sstream>

using namespace normal::ssb;

std::vector<size_t> findAllSubstringPositions(const std::string &str, const std::string &sub) {
  std::vector<size_t> positions;
  size_t pos = str.find(sub, 0);
  while(pos != std::string::npos)
  {
    positions.push_back(pos);
    pos = str.find(sub,pos+1);
  }
  return positions;
}

std::string normal::ssb::transformSqlForPrestoCSV(std::vector<std::string> &sqlLines) {
  std::stringstream ss;
  std::vector<std::string> newSqlLines;
  size_t numJoins;

  for (size_t i = 0; i < sqlLines.size(); ++i) {
    auto &line = sqlLines[i];
    if (line.substr(0, 4) == "from" || line.substr(0, 4) == "FROM") {
      // Number of joins
      numJoins = findAllSubstringPositions(line, ",").size();
      // Replace "date" with "dates"
      for (auto pos: findAllSubstringPositions(line, "date")) {
        line.insert(pos + 4, "s");
      }
      newSqlLines.emplace_back(line);
    } else if (line.substr(0, 5) == "where") {
      // Skip join predicates
      for (size_t j = 0; j < numJoins; ++j) {
        newSqlLines.emplace_back(sqlLines[i + j]);
      }
      i += (numJoins - 1);
    } else if (line.substr(0, 8) == "order by" || line.substr(0, 8) == "ORDER BY") {
      // Noop
    } else if (line.find("lo_orderdate between") != std::string::npos) {
      int year = std::stoi(line.substr(line.find("lo_orderdate between") + 21, 4));
      newSqlLines.emplace_back(fmt::format("  and (cast(year as int) = {})", year));
    } else {
      // Type transformations using "Cast"
      for (auto const &it: typeTransformations) {
        auto &field = it.first, &type = it.second;
        for (auto pos: findAllSubstringPositions(line, field)) {
          // have to process specially because of some other field is its prefix
          auto prefixIt = prefixFields.find(field);
          if (prefixIt != prefixFields.end()) {
            auto &tmpField = prefixIt->second;
            line.replace(pos, field.length(),
                        fmt::format("cast({} as {})", tmpField, type));
          } else {
            line.replace(pos, field.length(),
                        fmt::format("cast({} as {})", field, type));
          }
        }
      }
      newSqlLines.emplace_back(line);
    }
  }

  // Recover from special processing
  for (size_t i = 0; i < newSqlLines.size(); ++i) {
    auto &line = newSqlLines[i];
    for (auto const &prefixIt: prefixFields) {
      auto &field = prefixIt.first, &tmpField = prefixIt.second;
      for (auto pos: findAllSubstringPositions(line, tmpField)) {
        line.replace(pos, tmpField.length(), field);
      }
    }
    if (i < newSqlLines.size() - 1) {
      ss << line << "\n";
    } else {
      ss << line << ";\n";
    }
  }

  return ss.str();
}
