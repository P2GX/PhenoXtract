use crate::config::table_context::{Context, Identifier, SeriesContext};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use log::{info, warn};
use polars::prelude::{AnyValue, Column, DataType};
use regex::Regex;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct MultiHPOColExpansionStrategy;
impl Strategy for MultiHPOColExpansionStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().all(|table| {
            table.contexts_have_dtype(&Context::None, &Context::MultiHpoId, &DataType::String)
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), TransformError> {
        for table in tables.iter_mut() {
            let table_name = table.context().name.clone();
            info!("Applying MultiHPOColExpansion strategy to table: {table_name}");

            let subject_id_cols = table.get_cols_with_data_context(&Context::SubjectId);
            if subject_id_cols.len() > 1 {
                return Err(StrategyError(format!(
                    "Multiple SubjectID columns were found in table {table_name}."
                )));
            }

            let subject_id_col = subject_id_cols.last().ok_or(StrategyError(format!(
                "Could not find SubjectID column in table {table_name}"
            )))?;

            let stringified_subject_id_col = subject_id_col
                .str()
                .map_err(|_| {
                    StrategyError("Unexpectedly could not convert SubjectID column to string column when applying MultiHPOColExpansion strategy.".to_string())})?;

            let multi_hpo_col_names = table
                .get_cols_with_contexts(&Context::None, &Context::MultiHpoId)
                .iter()
                .map(|col| col.name().to_string())
                .collect::<Vec<String>>();

            let mut patient_to_hpo: HashMap<String, HashSet<String>> = HashMap::new();
            let mut hpos = HashSet::new();

            //first the relevant data is collected
            for multi_hpo_col_name in multi_hpo_col_names.iter() {
                let multi_hpo_col = table.data.column(&multi_hpo_col_name).unwrap();

                let stringified_multi_hpo_col = multi_hpo_col.str().map_err(|_| {
                    StrategyError(format!("Unexpectedly could not convert HPO column {multi_hpo_col_name} to string column when applying MultiHPOColExpansion strategy."))})?;

                let patient_id_multi_hpo_pairs = stringified_subject_id_col
                    .into_iter()
                    .zip(stringified_multi_hpo_col.into_iter());

                for (patient_id, multi_hpo) in patient_id_multi_hpo_pairs {
                    match multi_hpo {
                        None => continue,
                        Some(multi_hpo) => match patient_id {
                            None => {
                                warn!(
                                    "The entry {multi_hpo} in the column {multi_hpo_col_name} was found with no corresponding SubjectID."
                                );
                                continue;
                            }
                            Some(patient_id) => {
                                let hpo_ids = hpo_id_search(multi_hpo);
                                let patient_to_hpo_entry =
                                    patient_to_hpo.entry(patient_id.to_string()).or_default();

                                hpo_ids.into_iter().for_each(|hpo_id| {
                                    hpos.insert(hpo_id.clone());
                                    patient_to_hpo_entry.insert(hpo_id);
                                })
                            }
                        },
                    }
                }
            }

            //then the columns are created
            let new_hpo_col_names = hpos.into_iter().collect::<Vec<String>>();
            let mut new_hpo_cols = vec![];

            for hpo_col_name in new_hpo_col_names.iter() {
                let mut observation_statuses = vec![];
                stringified_subject_id_col.iter().for_each(|patient_id| {
                    let observation_status = match patient_id {
                        None => AnyValue::Null,
                        Some(patient_id) => match patient_to_hpo.get(patient_id) {
                            None => AnyValue::String("UNKNOWN"),
                            Some(patient_hpos) => {
                                if patient_hpos.contains(hpo_col_name) {
                                    AnyValue::String("OBSERVED")
                                } else {
                                    AnyValue::String("UNKNOWN")
                                }
                            }
                        },
                    };
                    observation_statuses.push(observation_status);
                });

                let new_hpo_column = Column::new(hpo_col_name.into(), observation_statuses);
                new_hpo_cols.push(new_hpo_column);
            }

            //then they are added to the table
            for new_hpo_col in new_hpo_cols {
                let new_hpo_col_name = new_hpo_col.name().clone();
                table
                    .data_mut()
                    .with_column(new_hpo_col)
                    .map_err(|_| StrategyError(format!("Unexpectedly could not add HPO column {new_hpo_col_name} to table {table_name}.")))?;
            }

            //and the old multi_hpo_columns are removed
            for multi_hpo_col_name in multi_hpo_col_names.iter() {
                table.data_mut().drop_in_place(multi_hpo_col_name).unwrap();
            }

            //and the ContextualisedDataFrame is updated
            let multi_hpo_scs = table.get_series_contexts_with_contexts(&Context::None, &Context::MultiHpoId);
            for multi_hpo_sc in multi_hpo_scs {
                
            }
            table
                .context_mut()
                .remove_context_pair((&Context::None, &Context::MultiHpoId));
            let new_sc = SeriesContext::new(Identifier::Multi(new_hpo_col_names), Context::HpoId, Context::ObservationStatus, None, None, None);
        }

        Ok(())
    }
}

fn hpo_id_search(string_to_search: &str) -> Vec<String> {
    let hpo_regex = Regex::new(r"HP:\d{7}").unwrap();
    hpo_regex
        .find_iter(string_to_search)
        .map(|mat| mat.as_str().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::Context::SubjectAge;
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use polars::prelude::*;
    use rstest::rstest;

    fn make_test_dataframe() -> ContextualizedDataFrame {
        let df = df![
            "subject_id" => &[AnyValue::String("P001"), AnyValue::String("P002"), AnyValue::String("P003"),AnyValue::String("P003"),AnyValue::String("P003"), AnyValue::String("P004"), AnyValue::String("P005")],
            "age" => &[AnyValue::Int32(51), AnyValue::Int32(4), AnyValue::Int32(22), AnyValue::Int32(15), AnyValue::Int32(11), AnyValue::Int32(11),AnyValue::Int32(555)],
            "HPO" => &[
                AnyValue::String("HP:0001410"),
                AnyValue::String("HP:0012622 Chronic kidney disease
                                HP:0001410	Leukoencephalopathy"),
                AnyValue::String("HP:0000212	Gingival overgrowth
                                HP:0011800 Hypoplasia of midface"),
                AnyValue::Null,
                AnyValue::String("HP:0001410,HP:0012622"),AnyValue::String("HP:0001410,HP:0012622"),AnyValue::String("HP:0001410,HP:0012622")],
        ].unwrap();

        let tc = TableContext::new(
            "TestTable".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("subject_id".to_string()),
                    Default::default(),
                    Context::SubjectId,
                    None,
                    None,
                    None,
                ),
                SeriesContext::new(
                    Identifier::Regex("age".to_string()),
                    Default::default(),
                    SubjectAge,
                    None,
                    None,
                    None,
                ),
                SeriesContext::new(
                    Identifier::Regex("HPO".to_string()),
                    Default::default(),
                    Context::MultiHpoId,
                    None,
                    None,
                    None,
                ),
            ],
        );

        ContextualizedDataFrame::new(tc, df)
    }

    #[rstest]
    fn test_multi_hpo_col_to_observed_hpo_cols_success() {
        let mut table = make_test_dataframe();

        dbg!(&table.data);

        let strategy = MultiHPOColExpansionStrategy;

        strategy.transform(&mut [&mut table]).unwrap();

        dbg!(&table.data);
    }
}
