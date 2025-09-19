use crate::config::table_context::Context::HpoLabel;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::strategies::utils::convert_col_to_string_vec;
use crate::transform::traits::Strategy;
use log::{info, warn};
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::{MinimalTerm, Synonymous};
use polars::prelude::{Column, DataType};
use std::collections::HashMap;
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
        let hpo_cols = table.get_cols_with_data_context(HpoLabel);
        let hpo_cols_are_str = hpo_cols.iter().all(|col| col.dtype() == &DataType::String);
        if hpo_cols_are_str {
            true
        } else {
            warn!("Not all columns with HPOLabel data context have string type.");
            false
        }
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

        let mut unparseable_terms: Vec<String> = vec![];

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
                            synonym_to_primary_term_map.insert(cell_term.clone(), "".to_string());
                            //this means that we won't get an error if a cell is empty
                            //and that the AnyValue::Null cell will now truly be the string "null"
                            //this is admittedly slightly strange behaviour. And we should perhaps come up with a better plan for how we deal with nulls.
                            if cell_term != "null" {
                                unparseable_terms.push(cell_term.clone());
                            }
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
        if !unparseable_terms.is_empty() {
            Err(StrategyError(format!(
                "Could not parse {unparseable_terms:?} as HPO terms."
            )))
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
    use crate::transform::error::TransformError;
    use crate::transform::strategies::hpo_synonyms_to_primary_terms::HPOSynonymsToPrimaryTermsStrategy;
    use crate::transform::traits::Strategy;
    use ontolius::ontology::csr::FullCsrOntology;
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use std::rc::Rc;
    use tempfile::TempDir;

    #[fixture]
    fn hpo_ontology() -> Rc<FullCsrOntology> {
        let tmp = TempDir::new().unwrap();
        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp.path().into());
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
    fn test_hpo_syns_strategy(hpo_ontology: Rc<FullCsrOntology>, tc: TableContext) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_get_hpo_labels_strategy");
            return;
        }

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
    fn test_hpo_syns_strategy_fail(hpo_ontology: Rc<FullCsrOntology>, tc: TableContext) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_get_hpo_labels_strategy_fail");
            return;
        }

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
        let expected_unparseables = vec!["abcdef", "12355", "jimmy"];
        assert_eq!(
            strat_result.unwrap_err(),
            TransformError::StrategyError(format!(
                "Could not parse {expected_unparseables:?} as HPO terms."
            ))
        );

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
    fn test_hpo_syns_strategy_with_nulls(hpo_ontology: Rc<FullCsrOntology>, tc: TableContext) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_get_hpo_labels_strategy");
            return;
        }

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

        let get_hpo_labels_strat = HPOSynonymsToPrimaryTermsStrategy { hpo_ontology };
        assert!(get_hpo_labels_strat.transform(&mut cdf).is_ok());

        let expected_col1 = Column::new(
            "phenotypic_features".into(),
            [
                "Pneumonia",
                "null",
                "Asthma",
                "Nail psoriasis",
                "Macrocephaly",
                "null",
            ],
        );
        let expected_col2 = Column::new(
            "more_phenotypic_features".into(),
            ["Asthma", "null", "Asthma", "Nail psoriasis", "null", "null"],
        );
        let expected_df = DataFrame::new(vec![expected_col1, expected_col2]).unwrap();
        assert_eq!(cdf.data, expected_df);
    }
}
