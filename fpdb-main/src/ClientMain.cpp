//
// Created by Yifei Yang on 2/9/21.
//

#include <fpdb/main/Client.h>
#include <fpdb/util/Util.h>

#define CMD_BEGIN(isNewLine) { std::cout << "FlexPushdownDB> "; if(!(isNewLine)) std::cout << "  ";}
#define RESULT(content) std::cout << (content) << std::endl;
#define ERROR(content) std::cout << "[ERROR] " << (content) << std::endl;

using namespace fpdb::main;
using namespace fpdb::util;

std::string setConfig(const std::string& command, const std::shared_ptr<Client>& client) {
  size_t whiteIndex = command.find(' ');
  std::string output;
  if (whiteIndex != std::string::npos) {
    auto word1 = command.substr(0, whiteIndex);
    auto word2 = command.substr(whiteIndex + 1);
    if (word1 == "distributed") {
      if (word2 == "True" || word2 == "true" || word2 == "TRUE") {
        output = "Distributed mode not implemented";
      } else if (word2 == "False" || word2 == "false" || word2 == "FALSE") {
        output = "Set to single mode";
      } else {
        output = "Bad set command";
      }
    } else {
      output = "Bad set command";
    }
  } else {
    output = "Bad set command";
  }
  return output;
}

bool execute(const std::string& command, const std::shared_ptr<Client>& client) {
  if (command == "quit" || command == "exit") {
    return true;
  }
  size_t whiteIndex = command.find(' ');
  if (whiteIndex != std::string::npos) {
    auto word1 = command.substr(0, whiteIndex);
    auto word2 = command.substr(whiteIndex + 1);
    try {
      if (word1 == "file") {
        auto content = client->executeQueryFile(word2);
        RESULT(content)
        return false;
      } else if (word1 == "sql") {
        auto content = client->executeQuery(word2);
        RESULT(content)
        return false;
      } else if (word1 == "set") {
        RESULT(setConfig(word2, client));
        return false;
      } else if (word1 == "local_ip") {
        auto expLocalIp = getLocalIp();
        if (expLocalIp.has_value()) {
          RESULT(*expLocalIp);
        } else {
          ERROR(expLocalIp.error())
        }
        return false;
      }
    }
    catch (const std::runtime_error& error) {
      ERROR(error.what())
      return false;
    }
    catch (const std::out_of_range& error) {
      ERROR(error.what())
      return false;
    }
    catch (...) {
      ERROR("Unrecognized error")
      return false;
    }
  }
  RESULT("Bad command")
  return false;
}

std::shared_ptr<Client> client;

int main() {
  // handle exit signals
  auto exitAct = [](int) {
    client->stop();
    client.reset();
    exit(0);
  };
  signal(SIGTERM, exitAct);
  signal(SIGINT, exitAct);
  signal(SIGABRT, exitAct);

  std::string line, appendLine;
  CMD_BEGIN(true)
  client = std::make_shared<Client>();
  client->start();

  while (std::getline(std::cin, appendLine)) {
    if (line.length() > 0) {
      line.append(" ");
    }
    line += appendLine;
    size_t endIndex = line.find(';');
    if (endIndex != std::string::npos) {
      std::string command = line.substr(0, endIndex);
      if (execute(command, client)) {
        client->stop();
        client.reset();
        break;
      }
      line = "";
      CMD_BEGIN(true)
    } else {
      CMD_BEGIN((line.length() == 0))
    }
  }
  return 0;
}