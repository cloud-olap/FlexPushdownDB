//
// Created by Yifei Yang on 10/31/21.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICALOP_PREPHYSICALOP_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICALOP_PREPHYSICALOP_H

#include <fpdb/plan/prephysical/PrePOpType.h>
#include <vector>
#include <set>
#include <string>
#include <memory>
#include <sys/types.h>

using namespace std;

namespace fpdb::plan::prephysical {

class PrePhysicalOp {
public:
  PrePhysicalOp(uint id, PrePOpType type, double rowCount);
  virtual ~PrePhysicalOp() = default;

  static bool equals(const std::shared_ptr<PrePhysicalOp> &p1,
                     const std::shared_ptr<PrePhysicalOp> &p2);
  static bool equals(const std::vector<std::shared_ptr<PrePhysicalOp>> &p1,
                     const std::vector<std::shared_ptr<PrePhysicalOp>> &p2);

  uint getId() const;
  PrePOpType getType() const;
  virtual string getTypeString() = 0;
  const vector<shared_ptr<PrePhysicalOp>> &getProducers() const;
  const set<string> &getProjectColumnNames() const;
  virtual set<string> getUsedColumnNames() = 0;
  double getRowCount() const;

  void setProducers(const vector<shared_ptr<PrePhysicalOp>> &producers);
  virtual void setProjectColumnNames(const set<string> &projectColumnNames);
  void setRowCount(double rowCount);

protected:
  uint id_;
  PrePOpType type_;
  double rowCount_;   // estimated row count
  vector<shared_ptr<PrePhysicalOp>> producers_;
  set<string> projectColumnNames_;

private:
  virtual bool equalTo(const std::shared_ptr<PrePhysicalOp> &other) const = 0;
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICALOP_PREPHYSICALOP_H
