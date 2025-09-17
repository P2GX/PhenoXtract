use crate::config::table_context::Context::HpoLabel;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
use crate::ontology::traits::OntologyRegistry;
use crate::ontology::utils::init_ontolius;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::strategies::utils::convert_col_to_string_vec;
use crate::transform::traits::Strategy;
use log::info;
use ontolius::ontology::OntologyTerms;
use ontolius::term::{MinimalTerm, Synonymous};
use polars::prelude::{Column, DataType};

/// Given a contextualised dataframe, this strategy will find all columns with HpoLabel as their data context
/// for each of these columns, it will check if the cells contain a HPO term synonym. If they do, it will change them to the Primary HPO term.
/// If any of the cells do not contain a HPO term synonym, then it will return an error.
#[allow(dead_code)]
pub struct GetHPOLabelsStrategy {
    hpo_registry: GithubOntologyRegistry,
}
impl Strategy for GetHPOLabelsStrategy {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        let hpo_cols = table.get_cols_with_data_context(HpoLabel);
        hpo_cols.iter().all(|col| col.dtype() == &DataType::String)
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let table_name = &table.context().name.clone();
        info!("Applying GetHPOLabels strategy to table: {table_name}");

        let hpo_label_cols: Vec<Column> = table
            .get_cols_with_data_context(HpoLabel)
            .into_iter()
            .cloned()
            .collect();
        let hpo_path = self.hpo_registry.register("latest").unwrap();
        let hpo_ontology = init_ontolius(hpo_path).unwrap();

        let mut unparseable_cells: Vec<String> = vec![];
        let mut col_name_partially_parsed_col_pairs = vec![];

        for col in hpo_label_cols {
            //todo This search is really not optimised. We could for example create a HashMap which stores HPO synonym searches which we have already performed
            // so that we don't have to do them twice.
            let string_vec_to_transform = convert_col_to_string_vec(&col)?;
            let partially_parsed_col = string_vec_to_transform
                .iter()
                .map(|cell_data| {
                    //first we search the HPO for primary terms that either match the cell data, or whose synonyms contain the cell data
                    let primary_term_search_opt = hpo_ontology.iter_terms().find(|primary_term| {
                        let synonyms = primary_term
                            .synonyms()
                            .iter()
                            .map(|syn| syn.name.to_lowercase())
                            .collect::<Vec<String>>();
                        cell_data.to_lowercase() == primary_term.name().to_lowercase()
                            || synonyms.contains(&cell_data.to_lowercase())
                    });
                    //if there was a matching primary term, we return it, otherwise we return an empty error
                    match primary_term_search_opt {
                        Some(primary_term) => Ok(primary_term.name().to_string()),
                        None => {
                            unparseable_cells.push(cell_data.clone());
                            Err("".to_string())
                        }
                    }
                })
                .collect::<Vec<Result<String, String>>>();
            col_name_partially_parsed_col_pairs
                .push((col.name().to_string(), partially_parsed_col));
        }
        let combined_result: Result<Vec<(String, Vec<String>)>, String> =
            col_name_partially_parsed_col_pairs
                .into_iter()
                .map(|(col_name, vec_of_results)| {
                    // Convert Vec<Result<String, String>> -> Result<Vec<String>, String>
                    let result = vec_of_results
                        .into_iter()
                        .collect::<Result<Vec<String>, String>>();
                    // Wrap the successfully parsed vectors with the column name
                    result.map(|successfully_parsed_col| (col_name, successfully_parsed_col))
                })
                .collect();

        // We only actually apply the change if every column was successfully parsed
        match combined_result {
            Ok(col_name_successfully_parsed_col_pairs) => {
                for (col_name, successfully_parsed_col) in col_name_successfully_parsed_col_pairs {
                    table.replace_column(successfully_parsed_col, &col_name)?;
                }
                Ok(())
            }
            Err(_) => Err(StrategyError(format!(
                "Could not parse {unparseable_cells:?} as HPO terms."
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::Context::HpoLabel;
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::transform::error::TransformError;
    use crate::transform::strategies::get_hpo_labels::GetHPOLabelsStrategy;
    use crate::transform::traits::Strategy;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    //todo why am I getting the warning: "Unknown synonym category "http://purl.obolibrary.org/obo/hp#allelic_requirement"" when I run these tests?

    #[fixture]
    fn hpo_registry() -> GithubOntologyRegistry {
        let tmp = TempDir::new().unwrap();
        GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp.path().into())
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
    fn test_get_hpo_labels_strategy(hpo_registry: GithubOntologyRegistry, tc: TableContext) {
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

        let get_hpo_labels_strat = GetHPOLabelsStrategy { hpo_registry };
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
    fn test_get_hpo_labels_strategy_fail(hpo_registry: GithubOntologyRegistry, tc: TableContext) {
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

        let get_hpo_labels_strat = GetHPOLabelsStrategy { hpo_registry };
        let strat_result = get_hpo_labels_strat.transform(&mut cdf);
        let expected_unparseables = vec!["abcdef", "12355", "jimmy"];
        assert_eq!(
            strat_result.unwrap_err(),
            TransformError::StrategyError(format!(
                "Could not parse {expected_unparseables:?} as HPO terms."
            ))
        );
    }
}
