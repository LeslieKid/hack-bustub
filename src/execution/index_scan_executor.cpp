//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "catalog/catalog.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_oid_t table_id = plan_->table_oid_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);

  index_oid_t index_id = plan_->index_oid_;
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_id);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  std::vector<Value> values{};
  assert(plan_->pred_key_ != nullptr);
  values.push_back(plan_->pred_key_->val_);
  Tuple key_tuple = Tuple(values, &index_info_->key_schema_);
  htable_->ScanKey(key_tuple, &rids_, exec_ctx_->GetTransaction());
  index_num_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &table_heap = table_info_->table_;
  while (true) {
    if (index_num_ >= rids_.size()) {
      return false;
    }
    *rid = rids_[index_num_++];
    *tuple = table_heap->GetTuple(*rid).second;
    auto tuple_meta = table_heap->GetTupleMeta(*rid);
    if (!tuple_meta.is_deleted_) {
      if (plan_->filter_predicate_) {
        auto &filter_expr = plan_->filter_predicate_;
        Value value = filter_expr->Evaluate(tuple, GetOutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          return true;
        }
      } else {
        return true;
      }
    }
  }
}

}  // namespace bustub
