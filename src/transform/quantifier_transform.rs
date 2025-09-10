use crate::config::table_context::AliasMap::{ToBool, ToFloat, ToInt, ToString};
use crate::config::table_context::SeriesContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use polars::prelude::{AnyValue, NamedFrom, Series};
use std::num::{ParseFloatError, ParseIntError};
use std::str::ParseBoolError;

pub struct QuantifierTransform {}

impl Strategy for QuantifierTransform {
    fn is_valid(&self, _table: &ContextualizedDataFrame) -> bool {
        true
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let mut col_name_quantifier_pairs = vec![];
        for series_context in &table.context().context {
            if let Some(cc) = series_context.get_cell_context_option() {
                if let Some(partition) = cc.get_quantifier() {
                    //todo currently just implementing this for single_sc because I don't yet know how to work with regex
                    if let SeriesContext::Single(single_sc) = series_context {
                        col_name_quantifier_pairs.push((single_sc.identifier.clone(), partition.clone()));
                    }
                }
            }
        }

        for (col_name, quantifier) in col_name_quantifier_pairs {
            let table_name = &table.context().name;
            let col_search_result = table.data().column(&col_name);
            match col_search_result {
                Ok(col) => {
                    let vec_of_strings = col
                        .as_series()
                        .ok_or(StrategyError(
                            format!("Could not convert column {col_name} to a series.").to_string(),
                        ))?
                        .iter()
                        .map(|val| match val {
                            AnyValue::String(s) => s.to_string(),
                            _ => val.to_string(),
                        })
                        .collect::<Vec<String>>();

                    fn comparison(num: f64, upper_bound: f64, inclusive_exclusive: bool) -> bool {
                        if inclusive_exclusive {num < upper_bound} else {num <= upper_bound}
                    }

                    let mut sorted_interval_boundaries = quantifier.interval_boundaries.clone();
                    sorted_interval_boundaries.sort_by(|a, b| a.total_cmp(b));

                    let transformed_vec:Vec<String> = vec_of_strings.iter().map(|str| {
                        match str.parse::<f64>() {
                            Ok(num) => {
                                let mut less_than_a_boundary = false;
                                for interval_boundary in sorted_interval_boundaries {
                                    if comparison(num,interval_boundary,quantifier.inclusive_exclusive) {
                                        less_than_a_boundary = true;
                                        quantifier.interval_aliases[]
                                    }
                                }
                            },
                            Err(_) => str,
                        }
                    }).collect();

                    let lower_boundary = f64::NEG_INFINITY;
                    for interval_boundary in quantifier.interval_boundaries {

                    }

                }
                Err(_e) => {
                    return Err(StrategyError(
                        format!("Could not find column {col_name} in table {table_name}.")
                            .to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::AliasMap::{ToInt, ToString};
    use crate::config::table_context::Context::{AgeInYears, SubjectId};
    use crate::config::table_context::SeriesContext::Single;
    use crate::config::table_context::{CellContext, SingleSeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::alias_map_transform::AliasMapTransform;
    use crate::transform::traits::Strategy;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;

    #[fixture]
    fn hm1() -> HashMap<String, String> {
        let mut hm1 = HashMap::new();
        hm1.insert(String::from("P002"), String::from("patient 2"));
        hm1.insert(String::from("P004"), String::from("P4"));
        hm1
    }

    #[fixture]
    fn cc1(hm1: HashMap<String, String>) -> CellContext {
        CellContext::new(SubjectId, None, Some(ToString(hm1)))
    }

    #[fixture]
    fn ssc1(cc1: CellContext) -> SingleSeriesContext {
        SingleSeriesContext::new("patient_id".to_string(), SubjectId, Some(cc1), vec![])
    }

    #[fixture]
    fn hm2() -> HashMap<String, i64> {
        let mut hm2 = HashMap::new();
        hm2.insert(String::from("35"), 40);
        hm2
    }

    #[fixture]
    fn cc2(hm2: HashMap<String, i64>) -> CellContext {
        CellContext::new(AgeInYears, None, Some(ToInt(hm2)))
    }

    #[fixture]
    fn ssc2(cc2: CellContext) -> SingleSeriesContext {
        SingleSeriesContext::new("age".to_string(), AgeInYears, Some(cc2), vec![])
    }

    #[fixture]
    fn tc(ssc1: SingleSeriesContext, ssc2: SingleSeriesContext) -> TableContext {
        TableContext::new("patient_data".to_string(), vec![Single(ssc1), Single(ssc2)])
    }

    #[fixture]
    fn data() -> DataFrame {
        let col1 = Column::new("patient_id".into(), ["P001", "P002", "P003", "P004"]);
        let col2 = Column::new("age".into(), [35, 16, 35, 25]);
        DataFrame::new(vec![col1, col2]).unwrap()
    }

    #[fixture]
    fn cdf(tc: TableContext, data: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, data)
    }

    #[rstest]
    fn test_transformation(mut cdf: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapTransform {};
        println!("{:?}", cdf);
        alias_map_transform.transform(&mut cdf).unwrap();
        println!("{:?}", cdf);
    }
}