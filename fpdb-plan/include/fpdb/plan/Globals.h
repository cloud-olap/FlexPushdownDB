//
// Created by Yifei Yang on 4/12/23.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_GLOBALS_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_GLOBALS_H

namespace fpdb::plan {

/**
 * Pushdown parameters used by FPDB store (others are set in fpdb-executor)
 * Set by "pushdown.conf"
 */
inline bool ENABLE_CO_LOCATED_JOIN_PUSHDOWN;

/**
 * For predicate transfer.
 */
inline bool ENABLE_PRED_TRANS = false;
// expand "local filters" (filter but not attached to scan / limit sort / group) before joins,
// this may not always be benificial, which depends on whether it's better to run BF or this op on unfiltered data first
// currently set them empiricially, i.e. "filter/limit sort" almost can always reduce cardinality but it may not be the
// case for "group"
inline bool ENABLE_JOIN_ORIGIN_LOCAL_FILTER_EXPANSION_FILTER = true;
inline bool ENABLE_JOIN_ORIGIN_LOCAL_FILTER_EXPANSION_LIMIT_SORT = true;
inline bool ENABLE_JOIN_ORIGIN_LOCAL_FILTER_EXPANSION_GROUP = false;

}

#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_GLOBALS_H
