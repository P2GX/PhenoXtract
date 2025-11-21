use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::info;

use crate::extract::contextualized_dataframe_filters::Filter;

use crate::config::context::Context;
use crate::transform::utils::is_iso8601_duration;
use polars::prelude::{DataType, IntoSeries};
use std::any::type_name;
use std::collections::{HashMap, HashSet};

#[allow(dead_code)]
#[derive(Debug)]
/// Given a column whose cells contains ages (e.g. subject age, age of death, age of onset)
/// this strategy converts integer entries to ISO8601 durations: 47 -> P47Y
/// NOTE: the integers must be between 0 and 150.
///
/// If an entry is already in ISO8601 duration format, it will be left unchanged.
///
/// If there are cell values which are neither ISO8601 durations nor integers
/// an error will be returned.
pub struct AgeToIso8601Strategy {
    min_age: i32,
    max_age: i32,
}

impl Default for AgeToIso8601Strategy {
    fn default() -> Self {
        AgeToIso8601Strategy::new()
    }
}

impl AgeToIso8601Strategy {
    pub fn new() -> Self {
        AgeToIso8601Strategy {
            min_age: 0,
            max_age: 150,
        }
    }

    fn is_valid_age(&self, age: i32) -> bool {
        age >= self.min_age && age <= self.max_age
    }
}

impl Strategy for AgeToIso8601Strategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context_is_age()
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying AgeToISO8601 strategy to data.");

        let years_to_iso8601: HashMap<i32, String> = (self.min_age..=self.max_age)
            .map(|n| (n, format!("P{n}Y")))
            .collect();

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        for table in tables.iter_mut() {
            let column_names = table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context_is_age()
                .collect_owned_names();

            for col_name in column_names {
                let col = table.data().column(&col_name)?;

                let cast_col = if col.dtype() != &DataType::String {
                    &col.cast(&DataType::String)?
                } else {
                    col
                };

                let mapped_column = cast_col.str()?.apply_mut(|cell_value| {
                    if is_iso8601_duration(cell_value) {
                        cell_value
                    } else if let Ok(years) = cell_value.parse::<i32>()
                        && self.is_valid_age(years)
                    {
                        years_to_iso8601
                            .get(&years)
                            .expect("Age was too high or too low.")
                    } else if let Ok(years) = cell_value.parse::<f64>()
                        && years.fract() == 0.0
                        && self.is_valid_age(years as i32)
                    {
                        let years_i32 = years as i32;
                        years_to_iso8601
                            .get(&years_i32)
                            .expect("Age was too high or too low")
                    } else {
                        if !cell_value.is_empty() {
                            let mapping_error_info = MappingErrorInfo {
                                column: col.name().to_string(),
                                table: table.context().name().to_string(),
                                old_value: cell_value.to_string(),
                                possible_mappings: vec![],
                            };
                            if !error_info.contains(&mapping_error_info) {
                                error_info.insert(mapping_error_info);
                            }
                        }
                        cell_value
                    }
                });
                table
                    .builder()
                    .replace_column(&col_name, mapped_column.into_series())?
                    .build()?;
            }
        }

        // return an error if not every cell term could be parsed
        if !error_info.is_empty() {
            Err(MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::context::Context;
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::error::{MappingErrorInfo, StrategyError};
    use crate::transform::strategies::age_to_iso8601::AgeToIso8601Strategy;
    use crate::transform::traits::Strategy;
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};

    #[fixture]
    fn tc() -> TableContext {
        let sc_pid = SeriesContext::default()
            .with_identifier(Identifier::from("subject_ids"))
            .with_data_context(Context::SubjectId);
        let sc_age = SeriesContext::default()
            .with_identifier(Identifier::from("age"))
            .with_data_context(Context::SubjectAge);
        TableContext::new("patient_data".to_string(), vec![sc_pid, sc_age])
    }

    #[rstest]
    fn test_age_to_iso8601_ints(tc: TableContext) {
        let col_pid = Column::new("subject_ids".into(), ["1", "2", "3", "4"]);
        let age_col = Column::new(
            "age".into(),
            [
                AnyValue::Int64(32),
                AnyValue::Int64(47),
                AnyValue::Null,
                AnyValue::Int64(15),
            ],
        );
        let df = DataFrame::new(vec![col_pid.clone(), age_col]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let age_to_iso8601_strat = AgeToIso8601Strategy::default();
        age_to_iso8601_strat.transform(&mut [&mut cdf]).unwrap();

        let expected_transformed_age_col = Column::new(
            "age".into(),
            [
                AnyValue::String("P32Y"),
                AnyValue::String("P47Y"),
                AnyValue::Null,
                AnyValue::String("P15Y"),
            ],
        );
        let expected_df =
            DataFrame::new(vec![col_pid.clone(), expected_transformed_age_col]).unwrap();
        assert_eq!(cdf.into_data(), expected_df);
    }

    #[rstest]
    fn test_age_to_iso8601_floats(tc: TableContext) {
        let col_pid = Column::new("subject_ids".into(), ["1", "2", "3", "4"]);
        let age_col = Column::new(
            "age".into(),
            [
                AnyValue::Float64(32.0),
                AnyValue::Float64(47.0),
                AnyValue::Null,
                AnyValue::Float64(15.0),
            ],
        );
        let df = DataFrame::new(vec![col_pid.clone(), age_col]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let age_to_iso8601_strat = AgeToIso8601Strategy::default();
        age_to_iso8601_strat.transform(&mut [&mut cdf]).unwrap();

        let expected_transformed_age_col = Column::new(
            "age".into(),
            [
                AnyValue::String("P32Y"),
                AnyValue::String("P47Y"),
                AnyValue::Null,
                AnyValue::String("P15Y"),
            ],
        );
        let expected_df =
            DataFrame::new(vec![col_pid.clone(), expected_transformed_age_col]).unwrap();
        assert_eq!(cdf.into_data(), expected_df);
    }

    #[rstest]
    fn test_age_to_iso8601_mixed_bag(tc: TableContext) {
        let col_pid = Column::new("subject_ids".into(), ["1", "2", "3", "4"]);
        let age_col = Column::new(
            "age".into(),
            [
                AnyValue::String("32.0"),
                AnyValue::String("P47Y5M12D"),
                AnyValue::Null,
                AnyValue::String("15"),
            ],
        );
        let df = DataFrame::new(vec![col_pid.clone(), age_col]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let age_to_iso8601_strat = AgeToIso8601Strategy::default();
        age_to_iso8601_strat.transform(&mut [&mut cdf]).unwrap();

        let expected_transformed_age_col = Column::new(
            "age".into(),
            [
                AnyValue::String("P32Y"),
                AnyValue::String("P47Y5M12D"),
                AnyValue::Null,
                AnyValue::String("P15Y"),
            ],
        );
        let expected_df =
            DataFrame::new(vec![col_pid.clone(), expected_transformed_age_col]).unwrap();
        assert_eq!(cdf.into_data(), expected_df);
    }

    #[rstest]
    fn test_age_to_iso8601_err(tc: TableContext) {
        let col_pid = Column::new("subject_ids".into(), ["1", "2", "3", "4"]);
        let age_col = Column::new(
            "age".into(),
            [
                AnyValue::String("321"),
                AnyValue::String("47Y5M12D"),
                AnyValue::Null,
                AnyValue::String("15"),
            ],
        );
        let df = DataFrame::new(vec![col_pid.clone(), age_col]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let age_to_iso8601_strat = AgeToIso8601Strategy::default();
        let result = age_to_iso8601_strat.transform(&mut [&mut cdf]);

        assert!(result.is_err());

        if let Err(StrategyError::MappingError {
            strategy_name,
            info,
        }) = result
        {
            assert_eq!(strategy_name, "AgeToIso8601Strategy");
            let expected_error_info: Vec<MappingErrorInfo> = Vec::from([
                MappingErrorInfo {
                    column: "age".to_string(),
                    table: "patient_data".to_string(),
                    old_value: "321".to_string(),
                    possible_mappings: vec![],
                },
                MappingErrorInfo {
                    column: "age".to_string(),
                    table: "patient_data".to_string(),
                    old_value: "47Y5M12D".to_string(),
                    possible_mappings: vec![],
                },
            ]);

            for i in info {
                assert!(expected_error_info.contains(&i));
            }
        }
    }
}
