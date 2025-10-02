use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError::{MappingError, StrategyError};
use crate::transform::error::{MappingErrorInfo, TransformError};
use crate::transform::traits::Strategy;
use log::{debug, info};
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::{MinimalTerm, Synonymous};
use polars::prelude::{DataType, IntoSeries, PlSmallStr};
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Given a collection of contextualised dataframes, this strategy will find all columns with HpoLabel as their data context
/// for each of these columns, it will check if the cells contain a HPO term synonym. If they do, it will change them to the Primary HPO term.
/// If any of the cells do not contain a HPO term synonym, then it will return an error.
#[allow(dead_code)]
#[derive(Debug)]
pub struct HPOSynonymsToPrimaryTermsStrategy {
    hpo_ontology: Arc<FullCsrOntology>,
}

impl HPOSynonymsToPrimaryTermsStrategy {
    pub fn new(hpo_ontology: Arc<FullCsrOntology>) -> Self {
        Self { hpo_ontology }
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

        //first we create our hash map
        let mut synonym_to_primary_term_map: HashMap<String, String> = HashMap::new();
        for table in tables.iter_mut() {
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

                for cell_term in col
                    .str()
                    .map_err(|_| {
                        StrategyError(format!(
                            "Unexpectedly could not convert column {col_name} to string column."
                        ))
                    })?
                    .into_iter()
                    .flatten()
                {
                    // first we check if the cell term is already in the hash map
                    if synonym_to_primary_term_map.contains_key(cell_term) {
                        continue;
                    } else {
                        // if the term isn't already a key in the hash map
                        // then we search the HPO for primary terms
                        // that either match the cell term, or whose synonyms contain the cell term
                        // and we insert the pair into the hash map
                        let primary_term_search_opt =
                            self.hpo_ontology.iter_terms().find(|primary_term| {
                                let synonyms = primary_term
                                    .synonyms()
                                    .iter()
                                    .map(|syn| syn.name.trim().to_lowercase())
                                    .collect::<Vec<String>>();
                                (cell_term.to_lowercase().trim()
                                    == primary_term.name().trim().to_lowercase()
                                    || synonyms.contains(&cell_term.trim().to_lowercase()))
                                    && (primary_term.is_current())
                            });
                        //we insert the pair (cell_term,primary_term) if the Option is Some, and (cell_term,"") if the Option is None
                        match primary_term_search_opt {
                            Some(primary_term) => {
                                synonym_to_primary_term_map
                                    .insert(cell_term.to_string(), primary_term.name().to_string());
                            }
                            None => {
                                // we do not consider cells with the string "null" as being errors
                                if cell_term != "null" {
                                    error_info.insert(MappingErrorInfo {
                                        column: col.name().to_string(),
                                        table: table.context().clone().name,
                                        old_value: cell_term.to_string(),
                                        possible_mappings: vec![],
                                    });
                                }
                                synonym_to_primary_term_map
                                    .insert(cell_term.to_string(), "".to_string());
                            }
                        }
                    }
                }
            }
        }
        for table in tables.iter_mut() {
            let table_name = &table.context().name.clone();
            info!("Applying HPOSynonymsToPrimaryTerms strategy to table: {table_name}");

            let names_of_hpo_label_cols: Vec<PlSmallStr> = table
                .get_cols_with_data_context(&Context::HpoLabel)
                .iter()
                .map(|col| col.name())
                .cloned()
                .collect();

            // we apply the primary term aliases when we can
            // and we do not change the cell term in the cases where we could not find a HPO primary term
            for col_name in names_of_hpo_label_cols {
                let col = table.data.column(&col_name).unwrap();
                let mapped_col = col
                    .str()
                    .map_err(|_| {
                        StrategyError(format!(
                            "Unexpectedly could not convert column {col_name} to string column."
                        ))
                    })?
                    .apply_mut(|cell_term| {
                        let primary_term = synonym_to_primary_term_map.get(cell_term);
                        if cell_term.is_empty() {
                            cell_term
                        } else {
                            match primary_term {
                                Some(primary_term) => {
                                    if primary_term.is_empty() {
                                        cell_term
                                    } else {
                                        debug!("Converted {cell_term} to {primary_term}");
                                        primary_term
                                    }
                                }
                                None => cell_term,
                            }
                        }
                    });
                table
                    .data
                    .replace(&col_name, mapped_col.into_series())
                    .map_err(|_| {
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
    use crate::test_utils::HPO;
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
            hpo_ontology: HPO.clone(),
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
            hpo_ontology: HPO.clone(),
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
            hpo_ontology: HPO.clone(),
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
