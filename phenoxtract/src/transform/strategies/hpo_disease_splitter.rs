use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;

use crate::transform::bidict_library::BiDictLibrary;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, PushMappingError, StrategyError};
use crate::transform::strategies::traits::Strategy;
use log::info;
use polars::prelude::{AnyValue, Column};
use std::any::type_name;
use std::collections::HashSet;
use std::sync::Arc;

/// This strategy will find every column whose context is HpoOrDisease
/// And split it into two separate columns: a Hpo column and a disease column.
///
/// Hpo is prioritised: the strategy will find all Hpo labels and IDs, and then put them into the
/// Hpo column. All other cells will be assumed to refer to disease.
///
#[derive(Debug)]
pub struct HpoDiseaseSplitterStrategy {
    hpo_dict_lib: Arc<BiDictLibrary>,
    disease_dict_lib: Arc<BiDictLibrary>,
}

impl HpoDiseaseSplitterStrategy {
    #[allow(unused)]
    pub fn new(hpo_dict_lib: Arc<BiDictLibrary>, disease_dict_lib: Arc<BiDictLibrary>) -> Self {
        Self {
            hpo_dict_lib,
            disease_dict_lib,
        }
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
        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

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
                            if self.hpo_dict_lib.lookup(hpo_or_disease).is_some() {
                                new_hpo_col_data.push(AnyValue::String(hpo_or_disease));
                                new_disease_col_data.push(AnyValue::Null);
                            } else if self.disease_dict_lib.lookup(hpo_or_disease).is_some() {
                                new_hpo_col_data.push(AnyValue::Null);
                                new_disease_col_data.push(AnyValue::String(hpo_or_disease))
                            } else {
                                error_info.insert_error(
                                    hpo_or_disease_col.name().to_string(),
                                    table.context().name().to_string(),
                                    hpo_or_disease.to_string(),
                                    vec![],
                                );
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
                print!("Inserting hpo column in hpo data.");

                if !error_info.is_empty() {
                    return Err(MappingError {
                        strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                        message: "Could not find ontology terms for these strings.".to_string(),
                        info: error_info.into_iter().collect(),
                    });
                }

                table
                    .builder()
                    .insert_col_with_context(new_hpo_col, Context::None, Context::Hpo)?
                    .insert_col_with_context(new_disease_col, Context::None, Context::Disease)?
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
    use crate::extract::contextualized_dataframe_filters::Filter;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::ontology_mocking::{HPO_DICT, MONDO_BIDICT};
    use crate::transform::bidict_library::BiDictLibrary;
    use crate::transform::strategies::hpo_disease_splitter::HpoDiseaseSplitterStrategy;
    use crate::transform::strategies::traits::Strategy;
    use polars::prelude::{AnyValue, Column};
    use rstest::rstest;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[rstest]
    fn test_hpo_disease_splitter() {
        let mut cdf = generate_minimal_cdf(2, 3);

        let phenotypes = ["Abnormality of head or neck", "HP:0000496", ""];
        let diseases = [
            "heart defects-limb shortening syndrome",
            "MONDO:0000252",
            "",
        ];

        fn to_anyvalues<'a>(items: &[&'a str]) -> Vec<AnyValue<'a>> {
            items
                .iter()
                .map(|&s| {
                    if s.is_empty() {
                        AnyValue::Null
                    } else {
                        AnyValue::String(s)
                    }
                })
                .collect()
        }

        let mut values = to_anyvalues(&phenotypes);
        values.extend(to_anyvalues(&diseases));

        let disease_hpo_col = Column::new("HpoAndDisease".into(), values);

        cdf.builder()
            .insert_col_with_context(disease_hpo_col, Context::None, Context::HpoOrDisease)
            .unwrap()
            .build()
            .unwrap();

        let strategy = HpoDiseaseSplitterStrategy {
            hpo_dict_lib: Arc::new(BiDictLibrary::new("hpo", vec![Box::new(HPO_DICT.clone())])),
            disease_dict_lib: Arc::new(BiDictLibrary::new(
                "disease",
                vec![Box::new(MONDO_BIDICT.clone())],
            )),
        };

        strategy.transform(&mut [&mut cdf]).unwrap();

        assert_eq!(cdf.data().iter().len(), 3);
        let scs: HashSet<Context> = cdf
            .context()
            .context()
            .iter()
            .map(|sc| sc.get_data_context().clone())
            .collect();

        assert_eq!(
            scs,
            HashSet::from_iter([Context::Hpo, Context::Disease, Context::SubjectId])
        );

        let assert_column_contains = |context: Context, expected_items: &[&str]| {
            let col = cdf
                .filter_columns()
                .where_data_context(Filter::Is(&context))
                .collect()
                .first()
                .cloned()
                .unwrap()
                .clone();

            let col_values = col
                .str()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<&str>>();

            for v in col_values {
                assert!(expected_items.contains(&v));
            }
        };

        assert_column_contains(Context::Hpo, &phenotypes);
        assert_column_contains(Context::Disease, &diseases);
    }
}
