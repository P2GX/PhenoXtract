use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::ontology::hpo_bidict::HPOBiDict;
use crate::transform::error::TransformError::{MappingError, StrategyError};
use crate::transform::error::{MappingErrorInfo, TransformError};
use crate::transform::traits::Strategy;
use log::info;

use polars::prelude::{DataType, PlSmallStr};
use std::any::type_name;
use std::collections::HashSet;
use std::sync::Arc;

/// Given a collection of contextualised dataframes, this strategy will find all columns with HpoLabel as their data context
/// for each of these columns, it will check if the cells contain a HPO term synonym. If they do, it will change them to the Primary HPO term.
/// If any of the cells do not contain a HPO term synonym, then it will return an error.
#[allow(dead_code)]
#[derive(Debug)]
pub struct HPOSynonymsToPrimaryTermsStrategy {
    hpo_dict: Arc<HPOBiDict>,
}

impl HPOSynonymsToPrimaryTermsStrategy {
    pub fn new(hpo_dict: Arc<HPOBiDict>) -> Self {
        Self { hpo_dict }
    }
}

impl Strategy for HPOSynonymsToPrimaryTermsStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().all(|table| {
            table.check_correct_data_type(&Context::None, &Context::HpoLabel, &DataType::String)
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), TransformError> {
        info!("Applying HPOSynonymsToPrimaryTerms strategy to data.");

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        for table in tables.iter_mut() {
            let table_name = table.context().name.to_string();

            let names_of_hpo_label_cols: Vec<PlSmallStr> = table
                .get_cols_with_data_context(&Context::HpoLabel)
                .iter()
                .map(|col| col.name())
                .cloned()
                .collect();

            for col_name in names_of_hpo_label_cols {
                let col = table.data.column(&col_name).map_err(|_| {
                    StrategyError(format!(
                        "Unexpectedly could not find column {col_name} in DataFrame."
                    ))
                })?;
                let mapped_column = col.str().unwrap().apply_mut(|cell_value| {
                    let hpo_id = self.hpo_dict.get(cell_value);

                    if let Some(hpo_id) = hpo_id {
                        return self.hpo_dict.get(hpo_id).unwrap();
                    }
                    if !cell_value.is_empty() && cell_value != "null" {
                        error_info.insert(MappingErrorInfo {
                            column: col.name().to_string(),
                            table: table.context().clone().name,
                            old_value: cell_value.to_string(),
                            possible_mappings: vec![],
                        });
                    }
                    cell_value
                });
                table.data.replace(&col_name, mapped_column).map_err(|_| {
                    StrategyError(format!(
                        "Could not replace {col_name} column in {table_name}."
                    ))
                })?;
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
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::test_utils::HPO_DICT;
    use crate::transform::error::{MappingErrorInfo, TransformError};
    use crate::transform::strategies::hpo_synonyms_to_primary_terms::HPOSynonymsToPrimaryTermsStrategy;
    use crate::transform::traits::Strategy;
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};

    #[fixture]
    fn tc() -> TableContext {
        let sc = SeriesContext::new(
            Identifier::Regex("phenotypic_features".to_string()),
            Context::None,
            Context::HpoLabel,
            None,
            None,
            vec![],
        );
        TableContext::new("patient_data".to_string(), vec![sc])
    }

    #[rstest]
    fn test_hpo_syns_strategy(tc: TableContext) {
        let col1 = Column::new(
            "phenotypic_features".into(),
            [
                "pneumonia",
                "Big calvaria",
                "Joint inflammation",
                "Nail psoriasis",
            ],
        );
        let col2 = Column::new(
            "more_phenotypic_features".into(),
            [
                "bronchial asthma",
                "Reactive airway disease",
                "Joint inflammation",
                "Nail psoriasis",
            ],
        );
        let df = DataFrame::new(vec![col1, col2]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let get_hpo_labels_strat = HPOSynonymsToPrimaryTermsStrategy {
            hpo_dict: HPO_DICT.clone(),
        };
        assert!(get_hpo_labels_strat.transform(&mut [&mut cdf]).is_ok());

        let expected_col1 = Column::new(
            "phenotypic_features".into(),
            ["Pneumonia", "Macrocephaly", "Arthritis", "Nail psoriasis"],
        );
        let expected_col2 = Column::new(
            "more_phenotypic_features".into(),
            ["Asthma", "Asthma", "Arthritis", "Nail psoriasis"],
        );
        let expected_df = DataFrame::new(vec![expected_col1, expected_col2]).unwrap();
        assert_eq!(cdf.data, expected_df);
    }

    #[rstest]
    fn test_hpo_syns_strategy_fail(tc: TableContext) {
        let col1 = Column::new(
            "phenotypic_features".into(),
            ["abcdef", "Big calvaria", "Joint inflammation", "12355"],
        );
        let col2 = Column::new(
            "more_phenotypic_features".into(),
            [
                "bronchial asthma",
                "Reactive airway disease",
                "jimmy",
                "Nail psoriasis",
            ],
        );
        let df = DataFrame::new(vec![col1, col2]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let get_hpo_labels_strat = HPOSynonymsToPrimaryTermsStrategy {
            hpo_dict: HPO_DICT.clone(),
        };
        let strat_result = get_hpo_labels_strat.transform(&mut [&mut cdf]);

        if let Err(TransformError::MappingError {
            strategy_name,
            info,
        }) = strat_result
        {
            assert_eq!(strategy_name, "HPOSynonymsToPrimaryTermsStrategy");
            let expected_error_info: Vec<MappingErrorInfo> = Vec::from([
                MappingErrorInfo {
                    column: "phenotypic_features".to_string(),
                    table: "patient_data".to_string(),
                    old_value: "abcdef".to_string(),
                    possible_mappings: vec![],
                },
                MappingErrorInfo {
                    column: "more_phenotypic_features".to_string(),
                    table: "patient_data".to_string(),
                    old_value: "jimmy".to_string(),
                    possible_mappings: vec![],
                },
                MappingErrorInfo {
                    column: "phenotypic_features".to_string(),
                    table: "patient_data".to_string(),
                    old_value: "12355".to_string(),
                    possible_mappings: vec![],
                },
            ]);

            for i in info {
                assert!(expected_error_info.contains(&i));
            }
        }

        let col1_after_strat = Column::new(
            "phenotypic_features".into(),
            ["abcdef", "Macrocephaly", "Arthritis", "12355"],
        );
        let col2_after_strat = Column::new(
            "more_phenotypic_features".into(),
            ["Asthma", "Asthma", "jimmy", "Nail psoriasis"],
        );
        let df_after_strat = DataFrame::new(vec![col1_after_strat, col2_after_strat]).unwrap();
        assert_eq!(cdf.data, df_after_strat);
    }

    #[rstest]
    fn test_hpo_syns_strategy_with_nulls(tc: TableContext) {
        let col1 = Column::new(
            "phenotypic_features".into(),
            [
                AnyValue::String("pneumonia"),
                AnyValue::Null,
                AnyValue::String("bronchial asthma"),
                AnyValue::String("Nail psoriasis"),
                AnyValue::String("Big calvaria"),
                AnyValue::Null,
            ],
        );
        let col2 = Column::new(
            "more_phenotypic_features".into(),
            [
                AnyValue::String("Reactive airway disease"),
                AnyValue::Null,
                AnyValue::String("asthma"),
                AnyValue::String("nail psoriasis"),
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let df = DataFrame::new(vec![col1, col2]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let get_hpo_labels_strat = HPOSynonymsToPrimaryTermsStrategy {
            hpo_dict: HPO_DICT.clone(),
        };
        assert!(get_hpo_labels_strat.transform(&mut [&mut cdf]).is_ok());

        let expected_col1 = Column::new(
            "phenotypic_features".into(),
            [
                AnyValue::String("Pneumonia"),
                AnyValue::Null,
                AnyValue::String("Asthma"),
                AnyValue::String("Nail psoriasis"),
                AnyValue::String("Macrocephaly"),
                AnyValue::Null,
            ],
        );
        let expected_col2 = Column::new(
            "more_phenotypic_features".into(),
            [
                AnyValue::String("Asthma"),
                AnyValue::Null,
                AnyValue::String("Asthma"),
                AnyValue::String("Nail psoriasis"),
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let expected_df = DataFrame::new(vec![expected_col1, expected_col2]).unwrap();
        assert_eq!(cdf.data, expected_df);
    }
}
