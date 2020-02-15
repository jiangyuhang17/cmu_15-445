//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {}

  void Init() override {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    table_iter_ = static_cast<std::unique_ptr<TableIterator>>(new TableIterator{table_info_->table_->Begin(exec_ctx_->GetTransaction())});
  }

  bool Next(Tuple *tuple) override {
    while (*table_iter_ != table_info_->table_->End()) {
      *tuple = **table_iter_;
      ++(*table_iter_);
      if (!plan_->GetPredicate() || plan_->GetPredicate()->Evaluate(tuple, &table_info_->schema_).GetAs<bool>()) return true;
    }
    return false;
  }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  const TableMetadata* table_info_;
  std::unique_ptr<TableIterator> table_iter_;
};
}  // namespace bustub
