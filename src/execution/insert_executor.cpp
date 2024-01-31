//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include "catalog/catalog.h"
#include "execution/plans/values_plan.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  row_amount_ = 0;
  row_value_ = Value(INTEGER, row_amount_);
  child_executor_->Init();
  table_oid_t table_id = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_id);
  index_array_ = catalog->GetTableIndexes(table_info_->name_);
  is_end_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple child_tuple{};
  auto &table_heap = table_info_->table_;
  TupleMeta inserted_tuple_meta;
  while (child_executor_->Next(&child_tuple, rid)) {
    /** Insert tuple into the table. */
    inserted_tuple_meta.ts_ = 0;
    inserted_tuple_meta.is_deleted_ = false;

    auto new_rid = table_heap->InsertTuple(inserted_tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
                                           exec_ctx_->GetTransaction(), table_info_->oid_);
    if (new_rid == std::nullopt) {
      return false;
    }

    /** Update the affected indexes. */
    for (auto &affected_index : index_array_) {
      affected_index->index_->InsertEntry(child_tuple.KeyFromTuple(table_info_->schema_, affected_index->key_schema_,
                                                                   affected_index->index_->GetKeyAttrs()),
                                          new_rid.value(), exec_ctx_->GetTransaction());
    }

    row_amount_++;
  }
  row_value_ = Value(INTEGER, row_amount_);
  std::vector<Value> output{};
  output.reserve(GetOutputSchema().GetColumnCount());
  output.push_back(row_value_);

  *tuple = Tuple{output, &GetOutputSchema()};
  is_end_ = true;

  return true;
}

}  // namespace bustub
