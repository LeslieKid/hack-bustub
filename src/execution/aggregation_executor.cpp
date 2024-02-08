//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdio>
#include <memory>
#include <utility>
#include <vector>
#include "common/rid.h"
#include "execution/plans/aggregation_plan.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(SimpleAggregationHashTable(plan->aggregates_, plan->agg_types_)),
      aht_iterator_(aht_.Begin()),
      special_case_end_(false) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.GenerateInitialAggregateValue();
  Tuple tuple;
  RID rid;

  // Aggregation is pipeline breaker.
  while (child_executor_->Next(&tuple, &rid)) {
    auto key_set = MakeAggregateKey(&tuple);
    auto value_set = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key_set, value_set);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (aht_iterator_ != aht_.End()) {
    auto &agg_val = aht_iterator_.Val();
    auto &agg_key = aht_iterator_.Key();
    std::vector<Value> output_vals;
    output_vals.reserve(agg_val.aggregates_.size() + agg_key.group_bys_.size());
    for (auto &val : agg_key.group_bys_) {
      output_vals.emplace_back(val);
    }
    for (auto &val : agg_val.aggregates_) {
      output_vals.emplace_back(val);
    }
    ++aht_iterator_;
    *tuple = Tuple(output_vals, &GetOutputSchema());
    std::cout << output_vals[0].GetAs<int>() << "\n";
    *rid = tuple->GetRid();
    return true;
  }

  if (aht_.Begin() == aht_.End() && !special_case_end_) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }
    auto agg_val = aht_.GenerateInitialAggregateValue();
    std::vector<Value> output_vals = agg_val.aggregates_;
    *tuple = Tuple(output_vals, &GetOutputSchema());
    *rid = tuple->GetRid();
    special_case_end_ = true;
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
