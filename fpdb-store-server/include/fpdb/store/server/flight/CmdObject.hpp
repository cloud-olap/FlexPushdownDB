//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CMDOBJECT_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CMDOBJECT_HPP

#include <memory>
#include "tl/expected.hpp"
#include "nlohmann/json.hpp"
#include "CmdType.hpp"

namespace fpdb::store::server::flight {

/**
 * Flight descriptor cmd
 *
 */
class CmdObject {
public:
  explicit CmdObject(std::shared_ptr<CmdType> type);

  virtual ~CmdObject() = default;

  virtual tl::expected<std::string, std::string> serialize(bool pretty) = 0;

  static tl::expected<std::shared_ptr<CmdObject>, std::string> deserialize(const std::string& cmd_string);

  [[nodiscard]] const std::shared_ptr<CmdType>& type() const;

private:
  std::shared_ptr<CmdType> type_;
};



} // namespace fpdb::store::server::flight

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_CMDOBJECT_HPP
