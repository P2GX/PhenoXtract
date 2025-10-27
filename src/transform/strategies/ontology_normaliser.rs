use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::info;

use crate::extract::contextualized_dataframe_filters::Filter;

use polars::prelude::{DataType, IntoSeries, PlSmallStr};
use std::any::type_name;
use std::collections::HashSet;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug)]
/// A strategy that converts ontology labels in cells (or synonyms of them) to the corresponding IDs.
/// It is case-insensitive.
///
/// This strategy processes string columns in data tables by looking up values in an ontology
/// bidirectional dictionary and replacing labels with their corresponding IDs.
/// It only operates on columns that have no header context and match the specified data context.
///
/// # Fields
///
/// * `ontology_dict` - A thread-safe reference to a bidirectional ontology dictionary that
///   maps between HPO labels and their primary identifiers. E.g. the HPO bidirectional dictionary
/// * `data_context` - The specific data context that columns must match to be processed
///   by this strategy. E.g. HpoLabelOrId
///
/// # Behavior
///
/// When applied to tables, the strategy:
/// 1. Identifies string columns with no header context that match the data context
/// 2. For each cell value, attempts to maps it via the ontology dictionary to its ID.
/// 3. Replaces the original value with the ID
/// 4. Collects mapping errors for any values that couldn't be resolved
/// 5. Returns an error if any labels failed to map (except for null values)
pub struct OntologyNormaliserStrategy {
    ontology_dict: Arc<OntologyBiDict>,
    data_context: Context,
}

impl OntologyNormaliserStrategy {
    pub fn new(ontology_dict: Arc<OntologyBiDict>, data_context: Context) -> Self {
        Self {
            ontology_dict,
            data_context,
        }
    }
}

impl Strategy for OntologyNormaliserStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&self.data_context))
                .where_dtype(Filter::Is(&DataType::String))
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying HPOSynonymsToPrimaryTerms strategy to data.");

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        for table in tables.iter_mut() {
            let column_names: Vec<PlSmallStr> = table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&self.data_context))
                .collect()
                .iter()
                .map(|col| col.name())
                .cloned()
                .collect();

            for col_name in column_names {
                let col = table.data().column(&col_name)?;
                let mapped_column = col.str()?.apply_mut(|cell_value| {
                    if self.ontology_dict.is_id(cell_value) {
                        cell_value
                    } else if let Some(curie_id) = self.ontology_dict.get(cell_value) {
                        curie_id
                    } else {
                        if !cell_value.is_empty() {
                            error_info.insert(MappingErrorInfo {
                                column: col.name().to_string(),
                                table: table.context().name().to_string(),
                                old_value: cell_value.to_string(),
                                possible_mappings: vec![],
                            });
                        }
                        cell_value
                    }
                });
                table
                    .builder()
                    .replace_column(&col_name, mapped_column.into_series())?;
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
    use crate::transform::error::{MappingErrorInfo, StrategyError};
    use crate::transform::strategies::ontology_normaliser::OntologyNormaliserStrategy;
    use crate::transform::traits::Strategy;
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};

    #[fixture]
    fn tc() -> TableContext {
        let sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("phenotypic_features".to_string()))
            .with_data_context(Context::HpoLabelOrId);
        TableContext::new("patient_data".to_string(), vec![sc])
    }

    #[rstest]
    fn test_hpo_syns_strategy(tc: TableContext) {
        let col1 = Column::new(
            "phenotypic_features".into(),
            ["pneumonia", "HP:0000256", "HP:0001369", "Nail psoriasis"],
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

        let get_hpo_labels_strat = OntologyNormaliserStrategy {
            ontology_dict: HPO_DICT.clone(),
            data_context: Context::HpoLabelOrId,
        };
        assert!(get_hpo_labels_strat.transform(&mut [&mut cdf]).is_ok());

        let expected_col1 = Column::new(
            "phenotypic_features".into(),
            ["HP:0002090", "HP:0000256", "HP:0001369", "HP:0033327"],
        );
        let expected_col2 = Column::new(
            "more_phenotypic_features".into(),
            ["HP:0002099", "HP:0002099", "HP:0001369", "HP:0033327"],
        );
        let expected_df = DataFrame::new(vec![expected_col1, expected_col2]).unwrap();
        assert_eq!(cdf.into_data(), expected_df);
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

        let get_hpo_labels_strat = OntologyNormaliserStrategy {
            ontology_dict: HPO_DICT.clone(),
            data_context: Context::HpoLabelOrId,
        };
        let strat_result = get_hpo_labels_strat.transform(&mut [&mut cdf]);

        if let Err(StrategyError::MappingError {
            strategy_name,
            info,
        }) = strat_result
        {
            assert_eq!(strategy_name, "OntologyNormaliserStrategy");
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
            ["abcdef", "HP:0000256", "HP:0001369", "12355"],
        );
        let col2_after_strat = Column::new(
            "more_phenotypic_features".into(),
            ["HP:0002099", "HP:0002099", "jimmy", "HP:0033327"],
        );
        let df_after_strat = DataFrame::new(vec![col1_after_strat, col2_after_strat]).unwrap();
        assert_eq!(cdf.into_data(), df_after_strat);
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
                AnyValue::String("HP:0002099"),
                AnyValue::Null,
                AnyValue::String("asthma"),
                AnyValue::String("nail psoriasis"),
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let df = DataFrame::new(vec![col1, col2]).unwrap();
        let mut cdf = ContextualizedDataFrame::new(tc, df);

        let get_hpo_labels_strat = OntologyNormaliserStrategy {
            ontology_dict: HPO_DICT.clone(),
            data_context: Context::HpoLabelOrId,
        };
        assert!(get_hpo_labels_strat.transform(&mut [&mut cdf]).is_ok());

        let expected_col1 = Column::new(
            "phenotypic_features".into(),
            [
                AnyValue::String("HP:0002090"),
                AnyValue::Null,
                AnyValue::String("HP:0002099"),
                AnyValue::String("HP:0033327"),
                AnyValue::String("HP:0000256"),
                AnyValue::Null,
            ],
        );
        let expected_col2 = Column::new(
            "more_phenotypic_features".into(),
            [
                AnyValue::String("HP:0002099"),
                AnyValue::Null,
                AnyValue::String("HP:0002099"),
                AnyValue::String("HP:0033327"),
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let expected_df = DataFrame::new(vec![expected_col1, expected_col2]).unwrap();
        assert_eq!(cdf.into_data(), expected_df);
    }
}
