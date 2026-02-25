use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::BiDict;
use crate::transform::error::StrategyError;
use crate::transform::strategies::traits::Strategy;
use log::info;
use polars::prelude::{AnyValue, Column};
use std::sync::Arc;

/// This strategy will find every column whose context is HpoOrDisease
/// And split it into two separate columns: a Hpo column and a disease column.
///
/// Hpo is prioritised: the strategy will find all Hpo labels and IDs, and then put them into the
/// Hpo column. All other cells will be assumed to refer to disease.
///
/// No validation is done as part of this strategy to see if the disease labels or IDs are valid.
#[derive(Debug)]
pub struct HpoDiseaseSplitterStrategy {
    hpo_dict: Arc<OntologyBiDict>,
}

impl HpoDiseaseSplitterStrategy {
    pub fn new(hpo_dict: Arc<OntologyBiDict>) -> Self {
        Self { hpo_dict }
    }
}

impl Strategy for HpoDiseaseSplitterStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_data_context_kind(Filter::Is(&ContextKind::HpoOrDisease))
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying HpoDiseaseSplitter strategy to data.");

        for table in tables.iter_mut() {
            let hpo_or_disease_col_names = table
                .filter_columns()
                .where_data_context_kind(Filter::Is(&ContextKind::HpoOrDisease))
                .collect_owned_names();

            for hpo_or_disease_col_name in hpo_or_disease_col_names {
                let mut new_hpo_col_data = vec![];
                let mut new_disease_col_data = vec![];

                let hpo_or_disease_col = table.data().column(&hpo_or_disease_col_name)?;

                for hpo_or_disease_opt in hpo_or_disease_col.str()?.iter() {
                    match hpo_or_disease_opt {
                        Some(hpo_or_disease) => {
                            if self.hpo_dict.get(hpo_or_disease).is_ok() {
                                new_hpo_col_data.push(AnyValue::String(hpo_or_disease));
                                new_disease_col_data.push(AnyValue::Null);
                            } else {
                                new_hpo_col_data.push(AnyValue::Null);
                                new_disease_col_data.push(AnyValue::String(hpo_or_disease))
                            }
                        }
                        None => {
                            new_hpo_col_data.push(AnyValue::Null);
                            new_disease_col_data.push(AnyValue::Null);
                        }
                    }
                }

                let new_hpo_col_name = format!("{hpo_or_disease_col_name}_hpo");
                let new_disease_col_name = format!("{hpo_or_disease_col_name}_disease");

                let new_hpo_col = Column::new(new_hpo_col_name.into(), new_hpo_col_data);
                let new_disease_col =
                    Column::new(new_disease_col_name.into(), new_disease_col_data);

                table
                    .builder()
                    .insert_col_with_context(new_hpo_col, Context::HpoLabelOrId, Context::None)?
                    .insert_col_with_context(
                        new_disease_col,
                        Context::DiseaseLabelOrId,
                        Context::None,
                    )?
                    .build()?;
            }

            table
                .builder()
                .drop_scs_alongside_cols_with_context(&Context::None, &Context::HpoOrDisease)?
                .build()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::context::Context;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::ontology_mocking::HPO_DICT;
    use crate::transform::strategies::hpo_disease_splitter::HpoDiseaseSplitterStrategy;
    use crate::transform::strategies::traits::Strategy;
    use polars::prelude::{AnyValue, Column};
    use rstest::rstest;

    #[rstest]
    fn test_hpo_disease_splitter() {
        let mut cdf = generate_minimal_cdf(5, 1);
        let disease_hpo_col = Column::new(
            "HpoAndDisease".into(),
            vec![
                AnyValue::String("Asthma"),
                AnyValue::String("Marfan Syndrome"),
                AnyValue::Null,
                AnyValue::String("HP:0001166"),
                AnyValue::String("Random disease"),
            ],
        );

        cdf.builder()
            .insert_col_with_context(disease_hpo_col, Context::HpoOrDisease, Context::None)
            .unwrap()
            .build()
            .unwrap();

        let strategy = HpoDiseaseSplitterStrategy {
            hpo_dict: HPO_DICT.clone(),
        };

        strategy.transform(&mut [&mut cdf]).unwrap();

        dbg!(&cdf);
    }
}
