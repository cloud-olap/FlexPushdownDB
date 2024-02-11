//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/util/Util.h>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <iostream>
#include <vector>
#include <filesystem>

using namespace normal::util;

std::string normal::util::readFile(const std::string& filePath) {
  std::ifstream inFile(filePath);
  std::ostringstream buf;
  char ch;
  while (buf && inFile.get(ch)) {
    buf.put(ch);
  }
  return buf.str();
}

std::vector<std::string> normal::util::readFileByLine(const std::string &filePath) {
  std::ifstream inFile(filePath);
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(inFile, line)) {
    lines.emplace_back(line);
  }
  return lines;
}

std::unordered_map<std::string, std::string> normal::util::readConfig() {
  std::unordered_map<std::string, std::string> configMap;
  std::string configPath = std::filesystem::current_path()
          .parent_path()
          .append("resources/config/exec.conf")
          .string();
  std::vector<std::string> lines = readFileByLine(configPath);
  for(auto const &line: lines) {
    auto pos = line.find('=');
    if (pos == std::string::npos) {
      continue;
    }
    auto key = line.substr(0, pos);
    auto value = line.substr(pos + 1);
    configMap[key] = value;
  }
  return configMap;
}

bool normal::util::isInteger(const std::string& str) {
  try {
    int parsedInt = std::stoi(str);
  } catch (const std::logic_error& err) {
    return false;
  }
  return true;
}

std::string normal::util::getLocalIp() {
  char hostBuffer[256];
  int hostName = gethostname(hostBuffer, sizeof(hostBuffer));
  struct hostent *host_entry = gethostbyname(hostBuffer);
  if (host_entry == NULL) {
    std::cerr << "Cannot get local ip" << std::endl;
    return "";
  }
  char *IPBuffer = inet_ntoa(*((struct in_addr*)host_entry->h_addr_list[0]));
  return std::string(IPBuffer);
}
