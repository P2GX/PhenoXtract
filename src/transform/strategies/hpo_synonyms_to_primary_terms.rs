use crate::config::table_context::Context::HpoLabel;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::{MappingErrorInfo, TransformError};
use crate::transform::strategies::utils::convert_col_to_string_vec;
use crate::transform::traits::Strategy;
use log::{info, warn};
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::{MinimalTerm, Synonymous};
use polars::prelude::{Column, DataType};
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Given a contextualised dataframe, this strategy will find all columns with HpoLabel as their data context
/// for each of these columns, it will check if the cells contain a HPO term synonym. If they do, it will change them to the Primary HPO term.
/// If any of the cells do not contain a HPO term synonym, then it will return an error.
#[allow(dead_code)]
pub struct HPOSynonymsToPrimaryTermsStrategy {
    hpo_ontology: Rc<FullCsrOntology>,
}
impl Strategy for HPOSynonymsToPrimaryTermsStrategy {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        let data_context = HpoLabel;
        let dtype = DataType::String;

        let columns = table.get_cols_with_data_context(data_context.clone());
        let is_valid = columns.iter().all(|col| col.dtype() == &dtype);

        if !is_valid {
            warn!(
                "Not all columns with {} data context have {} type in table {}.",
                data_context,
                dtype,
                table.context().name
            );
        }
        is_valid
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let table_name = &table.context().name.clone();
        info!("Applying HPOSynonymsToPrimaryTerms strategy to table: {table_name}");

        let hpo_label_cols: Vec<Column> = table
            .get_cols_with_data_context(HpoLabel)
            .into_iter()
            .cloned()
            .collect();

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        //first we create our hash map
        let mut synonym_to_primary_term_map: HashMap<String, String> = HashMap::new();
        for col in &hpo_label_cols {
            let stringified_col = convert_col_to_string_vec(col)?;
            for cell_term in stringified_col.iter() {
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
                                .map(|syn| syn.name.to_lowercase())
                                .collect::<Vec<String>>();
                            (cell_term.to_lowercase().trim() == primary_term.name().to_lowercase()
                                || synonyms.contains(&cell_term.to_lowercase()))
                                && (primary_term.is_current())
                        });
                    //we insert the pair (cell_term,primary_term) if the Option is Some, and (cell_term,"") if the Option is None
                    match primary_term_search_opt {
                        Some(primary_term) => {
                            synonym_to_primary_term_map
                                .insert(cell_term.clone(), primary_term.name().to_string());
                        }
                        None => {
                            error_info.insert(MappingErrorInfo {
                                column: col.name().to_string(),
                                table: table.context().clone().name,
                                old_value: cell_term.clone(),
                                possible_mappings: vec![],
                            });
                            synonym_to_primary_term_map.insert(cell_term.clone(), "".to_string());
                        }
                    }
                }
            }
        }

        // we apply the primary term aliases when we can
        // and we do not change the cell term in the cases where we could not find a HPO rimary term
        for col in hpo_label_cols {
            let string_vec_to_transform = convert_col_to_string_vec(&col)?;
            let parsed_col = string_vec_to_transform
                .iter()
                .filter_map(|cell_term| {
                    let primary_term = synonym_to_primary_term_map.get(cell_term);
                    match primary_term {
                        Some(primary_term) => {
                            if primary_term.is_empty() {
                                Some(cell_term.clone())
                            } else {
                                Some(primary_term.clone())
                            }
                        }
                        None => None,
                    }
                })
                .collect::<Vec<String>>();
            table.replace_column(parsed_col, col.name())?;
        }

        // return an error if not every cell term could be parsed
        if !error_info.is_empty() {
            Err(TransformError::MappingError {
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
    use crate::config::table_context::Context::HpoLabel;
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use crate::ontology::utils::init_ontolius;
    use crate::skip_in_ci;
    use crate::transform::error::{MappingErrorInfo, TransformError};
    use crate::transform::strategies::hpo_synonyms_to_primary_terms::HPOSynonymsToPrimaryTermsStrategy;
    use crate::transform::traits::Strategy;
    use ontolius::ontology::csr::FullCsrOntology;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use std::rc::Rc;
    use tempfile::TempDir;

    #[fixture]
    fn tmp_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn hpo_init_ontology(tmp_dir: TempDir) -> Rc<FullCsrOntology> {
        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp_dir.path().into());
        let hpo_path = hpo_registry.register("latest").unwrap();
        init_ontolius(hpo_path).unwrap()
    }

    #[fixture]
    fn tc() -> TableContext {
        let sc = SeriesContext::new(
            Identifier::Regex("phenotypic_features".to_string()),
            Context::None,
            HpoLabel,
            None,
            None,
            vec![],
        );
        TableContext::new("patient_data".to_string(), vec![sc])
    }

    #[rstest]
    fn test_get_hpo_labels_strategy(tmp_dir: TempDir, tc: TableContext) {
        skip_in_ci!();
        let hpo_ontology = hpo_init_ontology(tmp_dir);
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

        let get_hpo_labels_strat = HPOSynonymsToPrimaryTermsStrategy { hpo_ontology };
        assert!(get_hpo_labels_strat.transform(&mut cdf).is_ok());

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
    fn test_get_hpo_labels_strategy_fail(tmp_dir: TempDir, tc: TableContext) {
        skip_in_ci!();

        let hpo_ontology = hpo_init_ontology(tmp_dir);
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

        let get_hpo_labels_strat = HPOSynonymsToPrimaryTermsStrategy { hpo_ontology };
        let strat_result = get_hpo_labels_strat.transform(&mut cdf);

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
}
