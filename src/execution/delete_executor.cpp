//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_oid_t table_id = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_id);
  index_array_ = catalog->GetTableIndexes(table_info_->name_);
  is_end_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  printf("run delete\n");
  int32_t row_amount = 0;
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    /** Delete the tuple in the table */
    auto &table_heap = table_info_->table_;
    TupleMeta tuple_meta{};
    tuple_meta.is_deleted_ = true;
    tuple_meta.ts_ = 0;
    table_heap->UpdateTupleMeta(tuple_meta, *rid);

    /** Update the affected index */
    for (auto &index_info : index_array_) {
      auto &index = index_info->index_;
      printf("delete index %s\n", index_info->name_.c_str());
      index->DeleteEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs()), *rid,
                         exec_ctx_->GetTransaction());
      printf("delete index %s done\n", index_info->name_.c_str());
    }

    row_amount++;
  }
  auto row_value = Value(INTEGER, row_amount);
  std::vector<Value> output{};
  output.reserve(GetOutputSchema().GetColumnCount());
  output.push_back(row_value);

  *tuple = Tuple{output, &GetOutputSchema()};
  is_end_ = true;

  return true;
}

}  // namespace bustub
