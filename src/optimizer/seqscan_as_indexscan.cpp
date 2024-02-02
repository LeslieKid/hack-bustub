#include <memory>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "concurrency/transaction.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto &filter_expr = seq_scan_plan.filter_predicate_;
    if (filter_expr != nullptr) {
      auto comp_expr = dynamic_cast<ComparisonExpression *>(filter_expr.get());
      if (comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
        const table_oid_t table_id = seq_scan_plan.GetTableOid();
        auto column_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
        if (column_expr != nullptr) {
          auto match_index = MatchIndex(seq_scan_plan.table_name_, column_expr->GetColIdx());
          auto pred_key = dynamic_cast<ConstantValueExpression *>(comp_expr->GetChildAt(1).get());
          if (pred_key != nullptr && match_index.has_value()) {
            auto [index_id, index_name] = *match_index;
            return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_id, index_id, filter_expr,
                                                       pred_key);
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
