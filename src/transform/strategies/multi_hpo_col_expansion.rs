use crate::config::table_context::Identifier::Multi;
use crate::config::table_context::{Context, SeriesContext};
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

            let mut new_hpo_cols = vec![];
            let mut new_series_contexts = vec![];

            let multi_hpo_scs = table
                .get_series_contexts_with_contexts(&Context::None, &Context::MultiHpoId)
                .into_iter()
                .cloned()
                .collect::<Vec<SeriesContext>>();

            //the columns are created SC by SC
            for multi_hpo_sc in multi_hpo_scs.iter() {

                //NB. These are just the multi_HPO columns associated to multi_hpo_sc
                let multi_hpo_col_names = table
                    .get_columns(multi_hpo_sc.get_identifier())
                    .iter()
                    .map(|col| col.name().to_string())
                    .collect::<Vec<String>>();

                let mut patient_to_hpo: HashMap<String, HashSet<String>> = HashMap::new();
                let mut hpos = HashSet::new();

                //a patient_to_hpo hash map is created (needed in order to create the new columns)
                //the set of all HPO IDs encountered is also collected
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
                for hpo in hpos.iter() {
                    let mut observation_statuses = vec![];
                    stringified_subject_id_col.iter().for_each(|patient_id| {
                        let observation_status = patient_id
                            .and_then(|id| patient_to_hpo.get(id))
                            .filter(|hpos| hpos.contains(hpo))
                            .map(|_| AnyValue::String("OBSERVED"))
                            .unwrap_or(AnyValue::Null);
                        observation_statuses.push(observation_status);
                    });

                    let new_hpo_col = Column::new(hpo.into(), observation_statuses);
                    new_hpo_cols.push(new_hpo_col);
                }

                //then the new SC is created
                let block_id = multi_hpo_sc.get_building_block_id();
                let new_hpo_col_names = hpos.iter().cloned().collect::<Vec<String>>();
                let new_sc = SeriesContext::default()
                    .with_identifier(Multi(new_hpo_col_names))
                    .with_header_context(Context::HpoId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id(block_id.clone());
                new_series_contexts.push(new_sc);
            }

            let old_multi_hpo_col_names = table
                .get_cols_with_contexts(&Context::None, &Context::MultiHpoId)
                .iter()
                .map(|col| col.name().to_string())
                .collect::<Vec<String>>();

            //old multi_hpo_columns are removed
            for multi_hpo_col_name in old_multi_hpo_col_names.iter() {
                table.data_mut().drop_in_place(multi_hpo_col_name).map_err(|_| StrategyError(format!("Unexpectedly could not remove MultiHPO column {multi_hpo_col_name} from table {table_name}.")))?;
            }

            //old series contexts are removed
            for multi_hpo_sc in multi_hpo_scs.iter() {
                table.context_mut().remove_series_context(multi_hpo_sc);
            }

            //new series contexts are added
            for new_sc in new_series_contexts {
                table.context_mut().add_series_context(new_sc);
            }

            //new columns are added
            for new_hpo_col in new_hpo_cols {
                let new_hpo_col_name = new_hpo_col.name().clone();
                table
                    .data_mut()
                    .with_column(new_hpo_col)
                    .map_err(|_| StrategyError(format!("Unexpectedly could not add HPO column {new_hpo_col_name} to table {table_name}.")))?;
            }
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
                AnyValue::String("HP:0001410,HP:0012622"),
                AnyValue::String("HP:0001410,HP:0012622"),
                AnyValue::String("HP:0001410,HP:0012622")],
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
