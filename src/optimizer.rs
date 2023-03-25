use crate::analyzer::logical_plan::{LogicalExpr, LogicalPlan, Query, TableReference};

fn find_all_referenced_columns(expr: &LogicalExpr) -> Vec<Vec<String>> {
    let mut columns = vec![];
    let mut to_visit = vec![expr];
    while let Some(expr) = to_visit.pop() {
        match expr {
            LogicalExpr::Column(path) => columns.push(path.clone()),
            LogicalExpr::Unary { op: _, expr } => to_visit.push(expr),
            LogicalExpr::Binary { left, op: _, right } => {
                to_visit.push(left);
                to_visit.push(right);
            }
            LogicalExpr::IsNull(child) => to_visit.push(child),
            LogicalExpr::IsNotNull(child) => to_visit.push(child),
            _ => (),
        }
    }

    columns
}

fn count_referenced_columns(table_reference: &TableReference, columns: &[Vec<String>]) -> usize {
    match table_reference {
        TableReference::EmptyTable => 0,
        TableReference::BaseTable {
            table_id: _,
            name,
            schema: _,
            filter: _,
        } => columns
            .iter()
            .filter(|col| col.get(0).unwrap() == name)
            .count(),
        TableReference::Join {
            left,
            right,
            join_type: _,
            on: _,
        } => count_referenced_columns(left, columns) + count_referenced_columns(right, columns),
    }
}

fn all_columns_match_table_reference(
    table_reference: &TableReference,
    columns: &[Vec<String>],
) -> bool {
    count_referenced_columns(table_reference, columns) == columns.len()
}

/// Tries to push down a filter expression to a table reference.
/// Returns None if it was successful, else returns the the very same expression back
fn push_down_filter(
    table_reference: &mut TableReference,
    expr: LogicalExpr,
) -> Option<LogicalExpr> {
    let columns = find_all_referenced_columns(&expr);
    if !all_columns_match_table_reference(table_reference, &columns) {
        return Some(expr);
    }

    match table_reference {
        TableReference::BaseTable {
            table_id: _,
            name: _,
            schema: _,
            filter,
        } => {
            filter.push(expr);
        }
        TableReference::Join {
            left,
            right,
            join_type: _,
            on,
        } => {
            let all_left = all_columns_match_table_reference(&*left, &columns);
            let all_right = all_columns_match_table_reference(&*right, &columns);
            if all_left {
                push_down_filter(&mut *left, expr);
            } else if all_right {
                push_down_filter(&mut *right, expr);
            } else {
                on.push(expr);
            }
        }
        TableReference::EmptyTable => unreachable!(),
    };

    None
}

fn push_down_query_filters(mut query: Query) -> Query {
    let mut filters = vec![];
    for filter in query.filter {
        if let Some(expr) = push_down_filter(&mut query.from, filter) {
            filters.push(expr);
        }
    }
    query.filter = filters;
    query
}

/// takes a LogicalPlan and optimizes it based on heuristic rules
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    if let LogicalPlan::Select(query) = plan {
        let query = push_down_query_filters(query);
        LogicalPlan::Select(query)
    } else {
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::optimize;
    use crate::analyzer::logical_plan::{LogicalExpr, LogicalPlan, Query, TableReference};
    use crate::catalog::schema::Schema;
    use crate::parser::ast::{BinaryOperator, JoinType};

    #[test]
    fn can_push_down_cross_joins() {
        let plan = LogicalPlan::Select(Query {
            values: vec![],
            from: TableReference::Join {
                left: Box::new(TableReference::BaseTable {
                    table_id: 1,
                    name: "table_a".to_owned(),
                    schema: Schema::new(vec![]),
                    filter: vec![],
                }),
                right: Box::new(TableReference::BaseTable {
                    table_id: 2,
                    name: "table_b".to_owned(),
                    schema: Schema::new(vec![]),
                    filter: vec![],
                }),
                join_type: JoinType::Inner,
                on: vec![],
            },
            projections: vec![],
            filter: vec![
                LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Column(vec![
                        "table_a".to_owned(),
                        "id".to_owned(),
                    ])),
                    op: BinaryOperator::Eq,
                    right: Box::new(LogicalExpr::Column(vec![
                        "table_b".to_owned(),
                        "table_a_id".to_owned(),
                    ])),
                },
                LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Column(vec![
                        "table_a".to_owned(),
                        "count".to_owned(),
                    ])),
                    op: BinaryOperator::Greater,
                    right: Box::new(LogicalExpr::Integer(3)),
                },
            ],
            output_schema: Schema::new(vec![]),
        });

        let optimized_plan = optimize(plan);
        let expected_plan = LogicalPlan::Select(Query {
            values: vec![],
            from: TableReference::Join {
                left: Box::new(TableReference::BaseTable {
                    table_id: 1,
                    name: "table_a".to_owned(),
                    schema: Schema::new(vec![]),
                    filter: vec![LogicalExpr::Binary {
                        left: Box::new(LogicalExpr::Column(vec![
                            "table_a".to_owned(),
                            "count".to_owned(),
                        ])),
                        op: BinaryOperator::Greater,
                        right: Box::new(LogicalExpr::Integer(3)),
                    }],
                }),
                right: Box::new(TableReference::BaseTable {
                    table_id: 2,
                    name: "table_b".to_owned(),
                    schema: Schema::new(vec![]),
                    filter: vec![],
                }),
                join_type: JoinType::Inner,
                on: vec![LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Column(vec![
                        "table_a".to_owned(),
                        "id".to_owned(),
                    ])),
                    op: BinaryOperator::Eq,
                    right: Box::new(LogicalExpr::Column(vec![
                        "table_b".to_owned(),
                        "table_a_id".to_owned(),
                    ])),
                }],
            },
            projections: vec![],
            filter: vec![],
            output_schema: Schema::new(vec![]),
        });

        assert_eq!(optimized_plan, expected_plan);
    }
}
