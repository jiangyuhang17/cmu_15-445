//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor)
      : AbstractExecutor(exec_ctx), plan_{plan} {}

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    if (!plan_->IsRawInsert()) {
      child_executor_ = ExecutorFactory::CreateExecutor(GetExecutorContext(), plan_->GetChildPlan());
      child_executor_->Init();
    }
  }

  // Note that Insert does not make use of the tuple pointer being passed in.
  // We return false if the insert failed for any reason, and return true if all inserts succeeded.
  bool Next([[maybe_unused]] Tuple *tuple) override {
    if (plan_->IsRawInsert()) {
      int size = plan_->RawValues().size();
      for (int i=0; i < size; i++) {
        Tuple tuple1 {plan_->RawValuesAt(i), &table_info_->schema_};
        RID rid;
        if (!table_info_->table_->InsertTuple(tuple1, &rid, exec_ctx_->GetTransaction()))
          return false;
      }
    } else {
      Tuple tuple1;
      RID rid;
      while (child_executor_->Next(&tuple1)) {
        if (!table_info_->table_->InsertTuple(tuple1, &rid, exec_ctx_->GetTransaction()))
          return false;
      }
    }
    return true;
  }

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;
  const TableMetadata *table_info_;
  std::unique_ptr<AbstractExecutor> child_executor_;
};
}  // namespace bustub
