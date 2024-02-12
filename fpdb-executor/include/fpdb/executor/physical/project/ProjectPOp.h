//
// Created by matt on 14/4/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PROJECT_PROJECTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PROJECT_PROJECTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/expression/gandiva/Expression.h>
#include <fpdb/expression/Expression.h>
#include <fpdb/expression/Projector.h>

using namespace fpdb::executor::message;

namespace fpdb::executor::physical::project {

class ProjectPOp : public PhysicalOp {

public:

  /**
   * Constructor
   * @param Name Descriptive name
   * @param Expressions Expressions to evaluate to produce the attributes in the projection
   */
  ProjectPOp(std::string name,
          std::vector<std::string> projectColumnNames,
          int nodeId,
          std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>> exprs,
          std::vector<std::string> exprNames,
          std::vector<std::pair<std::string, std::string>> projectColumnNamePairs);
  ProjectPOp() = default;
  ProjectPOp(const ProjectPOp&) = default;
  ProjectPOp& operator=(const ProjectPOp&) = default;

  /**
   * Default destructor
   */
  ~ProjectPOp() override = default;

  /**
   * Operators message handler
   * @param msg
   */
  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

  const std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>> &getExprs() const;
  const std::vector<std::string> &getExprNames() const;
  const std::vector<std::pair<std::string, std::string>> &getProjectColumnNamePairs() const;

private:
  /**
   * Start message handler
   */
  void onStart();

  /**
   * Completion message handler
   */
  void onComplete(const CompleteMessage &);

  /**
   * Tuples message handler
   * @param message
   */
  void onTupleSet(const TupleSetMessage &message);

  /**
   * Build the projector from the input schema
   * @param inputSchema
   */
  void buildProjector(const TupleSetMessage &message);

  /**
   * Adds the tuples in the tuple message to the internal buffer
   * @param message
   */
  void bufferTuples(const TupleSetMessage &message);

  /**
   * Sends the given projected tuples to consumers
   * @param projected
   */
  void sendTuples(std::shared_ptr<TupleSet> &projected);

  /**
   * Projects the tuples and sends them to consumers
   */
  void projectAndSendTuples();

  /**
   * Used in hybrid execution, keep only those projections that are applicable to input tupleSet
   * @param tupleSet
   */
  void discardInapplicableProjections(const std::shared_ptr<TupleSet> &tupleSet);

  /**
   * The project expressions and the attribute names
   */
  std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>> exprs_;
  std::vector<std::string> exprNames_;

  /**
   * The project columns (pair of input name and output name)
   */
  std::vector<std::pair<std::string, std::string>> projectColumnNamePairs_;

  /**
   * A buffer of received tuples that are not projected until enough tuples have been received
   */
  std::shared_ptr<TupleSet> tuples_;

  /**
   * The expression projector, created when input schema is extracted from first tuple received
   */
  std::optional<std::shared_ptr<fpdb::expression::Projector>> projector_;

  /**
   * Whether discardInapplicableProjections() has been invoked
   */
  bool inapplicableProjectionsDiscarded_ = false;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ProjectPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_),
                               f.field("exprs", op.exprs_),
                               f.field("exprNames", op.exprNames_),
                               f.field("projectColumnNamePairs", op.projectColumnNamePairs_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PROJECT_PROJECTPOP_H
