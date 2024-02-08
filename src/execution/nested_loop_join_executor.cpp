//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <sys/types.h>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "execution/executors/abstract_executor.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      is_inner_loop_end_(true),
      is_match_(false),
      index_num_(0) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    inner_table_.emplace_back(tuple, rid);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto join_type = plan_->GetJoinType();
  if (join_type == JoinType::INNER) {
    while (!is_inner_loop_end_ || left_executor_->Next(&outer_tuple_, &outer_rid_)) {
      is_inner_loop_end_ = false;
      Tuple inner_tuple;
      RID inner_rid;
      while (index_num_ < inner_table_.size()) {
        inner_tuple = inner_table_[index_num_].first;
        inner_rid = inner_table_[index_num_].second;
        auto predicate_expr = plan_->Predicate();
        std::vector<Value> join_array;
        uint32_t left_col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
        uint32_t right_col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
        join_array.reserve(left_col_cnt + right_col_cnt);
        for (u_int32_t i = 0; i < left_col_cnt; ++i) {
          join_array.emplace_back(outer_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (u_int32_t i = 0; i < right_col_cnt; ++i) {
          join_array.emplace_back(inner_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        index_num_++;
        *tuple = Tuple(join_array, &GetOutputSchema());
        auto val = predicate_expr->EvaluateJoin(&outer_tuple_, left_executor_->GetOutputSchema(), &inner_tuple,
                                                right_executor_->GetOutputSchema());
        if (!val.IsNull() && val.GetAs<bool>()) {
          *rid = tuple->GetRid();
          return true;
        }
      }
      is_inner_loop_end_ = true;
      right_executor_->Init();
      index_num_ = 0;
    }
    return false;
  }

  if (join_type == JoinType::LEFT) {
    uint32_t left_col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
    uint32_t right_col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
    while (!is_inner_loop_end_ || left_executor_->Next(&outer_tuple_, &outer_rid_)) {
      if (is_inner_loop_end_) {
        is_match_ = false;
      }
      is_inner_loop_end_ = false;
      Tuple inner_tuple;
      RID inner_rid;

      while (index_num_ < inner_table_.size()) {
        inner_tuple = inner_table_[index_num_].first;
        inner_rid = inner_table_[index_num_].second;
        auto predicate_expr = plan_->Predicate();
        std::vector<Value> join_array;
        join_array.reserve(left_col_cnt + right_col_cnt);
        for (u_int32_t i = 0; i < left_col_cnt; ++i) {
          join_array.emplace_back(outer_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (u_int32_t i = 0; i < right_col_cnt; ++i) {
          join_array.emplace_back(inner_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        index_num_++;
        *tuple = Tuple(join_array, &GetOutputSchema());
        auto val = predicate_expr->EvaluateJoin(&outer_tuple_, left_executor_->GetOutputSchema(), &inner_tuple,
                                                right_executor_->GetOutputSchema());
        if (!val.IsNull() && val.GetAs<bool>()) {
          is_match_ = true;
          *rid = tuple->GetRid();
          return true;
        }
      }

      if (!is_match_) {
        std::vector<Value> left_join_array;
        left_join_array.reserve(left_col_cnt + right_col_cnt);
        for (u_int32_t i = 0; i < left_col_cnt; ++i) {
          left_join_array.emplace_back(outer_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (u_int32_t i = 0; i < right_col_cnt; ++i) {
          left_join_array.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
        }
        *tuple = Tuple(left_join_array, &GetOutputSchema());
        *rid = tuple->GetRid();
        is_match_ = true;
        return true;
      }

      is_inner_loop_end_ = true;
      right_executor_->Init();
      index_num_ = 0;
    }
    return false;
  }

  return false;
}

}  // namespace bustub
