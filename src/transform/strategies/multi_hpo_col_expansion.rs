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
#[allow(dead_code)]
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

            let building_block_ids = table.get_building_block_ids();

            //we create the columns building block by building block
            for bb_id in building_block_ids.iter() {
                let multi_hpo_col_names = table
                    .get_building_block_with_contexts(bb_id, &Context::None, &Context::MultiHpoId)
                    .iter()
                    .map(|col| col.name().to_string())
                    .collect::<Vec<String>>();

                let mut patient_to_hpo: HashMap<String, HashSet<String>> = HashMap::new();
                let mut hpos = HashSet::new();

                //a patient_to_hpo hash map is created (needed in order to create the new columns)
                //the set of all HPO IDs encountered is also collected
                for multi_hpo_col_name in multi_hpo_col_names.iter() {
                    let multi_hpo_col = table.data.column(multi_hpo_col_name).unwrap();

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

                //it's convenient to sort the HPOs so that we get a consistent output
                let mut sorted_hpos = hpos.iter().collect::<Vec<&String>>();
                sorted_hpos.sort();

                //then the columns are created
                let mut new_hpo_col_names = vec![];

                for hpo in sorted_hpos {
                    let mut observation_statuses = vec![];
                    stringified_subject_id_col.iter().for_each(|patient_id| {
                        let observation_status = patient_id
                            .and_then(|id| patient_to_hpo.get(id))
                            .filter(|hpos| hpos.contains(hpo))
                            .map(|_| AnyValue::String("OBSERVED"))
                            .unwrap_or(AnyValue::Null);
                        observation_statuses.push(observation_status);
                    });

                    let mut new_hpo_col_name = hpo.clone();
                    if let Some(block_id) = bb_id {
                        new_hpo_col_name.push_str(" (block ");
                        new_hpo_col_name.push_str(block_id);
                        new_hpo_col_name.push(')')
                    }

                    let new_hpo_col =
                        Column::new(new_hpo_col_name.clone().into(), observation_statuses);
                    new_hpo_cols.push(new_hpo_col);
                    new_hpo_col_names.push(new_hpo_col_name);
                }

                //then the new SC is created
                let new_sc = SeriesContext::default()
                    .with_identifier(Identifier::Multi(new_hpo_col_names))
                    .with_header_context(Context::HpoId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id(bb_id.clone());
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
                    .map_err(|_| StrategyError(format!("Unexpectedly could not add HPO column {new_hpo_col_name} to table {table_name}. Possible duplicates?")))?;
            }
        }

        Ok(())
    }
}

#[allow(unused)]
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
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use polars::prelude::*;
    use rstest::{fixture, rstest};

    #[fixture]
    fn cdf() -> ContextualizedDataFrame {
        let df = df![
            "subject_id" => &[AnyValue::String("P001"), AnyValue::String("P002"), AnyValue::String("P002"),AnyValue::String("P003")],
            "age" => &[AnyValue::Int32(51), AnyValue::Int32(4), AnyValue::Int32(4), AnyValue::Int32(15)],
            "Multi_HPOs_Block_A_1" => &[
                AnyValue::String("patient 1 - HP:1111111 asd"),
                AnyValue::String("patient 2 - asd HP:2222222 HP:3333333asd"),
                AnyValue::String("patient 2 - asdHP:2222222 asfn "),
                AnyValue::Null,],
            "Multi_HPOs_Block_A_2" => &[
                AnyValue::Null,
                AnyValue::String("patient 2 - asd HP:1111111 HP:3333333asd"),
                AnyValue::Null,
                AnyValue::Null,],
            "Multi_HPOs_No_Block_1" => &[
                AnyValue::String("patient 1 - HP:1111111"),
                AnyValue::String("patient 2 - HP:4444444 - HP:5555555"),
                AnyValue::String("patient 2 - wljkehg"),
                AnyValue::String("patient 3 - asd")],
            "Multi_HPOs_No_Block_2" => &[
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::String("patient 3 - HP:4444444123123")],
        ].unwrap();

        let tc = TableContext::new(
            "TestTable".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("subject_id".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("age".to_string()))
                    .with_data_context(Context::SubjectAge),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Multi_HPOs_Block_A".to_string()))
                    .with_data_context(Context::MultiHpoId)
                    .with_building_block_id(Some("A".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Multi_HPOs_No_Block_1".to_string()))
                    .with_data_context(Context::MultiHpoId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Multi_HPOs_No_Block_2".to_string()))
                    .with_data_context(Context::MultiHpoId),
            ],
        );

        ContextualizedDataFrame::new(tc, df)
    }

    #[fixture]
    fn expected_transformed_cdf() -> ContextualizedDataFrame {
        let expected_df = df![
            "subject_id" => &[AnyValue::String("P001"), AnyValue::String("P002"), AnyValue::String("P002"),AnyValue::String("P003")],
            "age" => &[AnyValue::Int32(51), AnyValue::Int32(4), AnyValue::Int32(4), AnyValue::Int32(15)],
            "HP:1111111 (block A)" => &[
                AnyValue::String("OBSERVED"),
                AnyValue::String("OBSERVED"),
                AnyValue::String("OBSERVED"),
                AnyValue::Null,],
            "HP:2222222 (block A)" => &[
                AnyValue::Null,
                AnyValue::String("OBSERVED"),
                AnyValue::String("OBSERVED"),
                AnyValue::Null,],
            "HP:3333333 (block A)" => &[
                AnyValue::Null,
                AnyValue::String("OBSERVED"),
                AnyValue::String("OBSERVED"),
                AnyValue::Null,],
            "HP:1111111" => &[
                AnyValue::String("OBSERVED"),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,],
            "HP:4444444" => &[
                AnyValue::Null,
                AnyValue::String("OBSERVED"),
                AnyValue::String("OBSERVED"),
                AnyValue::String("OBSERVED")],
            "HP:5555555" => &[
                AnyValue::Null,
                AnyValue::String("OBSERVED"),
                AnyValue::String("OBSERVED"),
                AnyValue::Null,],
        ].unwrap();

        let expected_tc = TableContext::new(
            "TestTable".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("subject_id".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("age".to_string()))
                    .with_data_context(Context::SubjectAge),
                SeriesContext::default()
                    .with_identifier(Identifier::Multi(vec![
                        "HP:1111111 (block A)".to_string(),
                        "HP:2222222 (block A)".to_string(),
                        "HP:3333333 (block A)".to_string(),
                    ]))
                    .with_header_context(Context::HpoId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id(Some("A".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Multi(vec![
                        "HP:1111111".to_string(),
                        "HP:4444444".to_string(),
                        "HP:5555555".to_string(),
                    ]))
                    .with_header_context(Context::HpoId)
                    .with_data_context(Context::ObservationStatus),
            ],
        );

        ContextualizedDataFrame::new(expected_tc, expected_df)
    }

    #[rstest]
    fn test_multi_hpo_col_expansion(
        mut cdf: ContextualizedDataFrame,
        expected_transformed_cdf: ContextualizedDataFrame,
    ) {
        let strategy = MultiHPOColExpansionStrategy;
        strategy.transform(&mut [&mut cdf]).unwrap();
        assert_eq!(cdf, expected_transformed_cdf);
    }

    #[rstest]
    fn test_hpo_id_search() {
        let string_to_search =
            "asdasdHP:0012622 Chronic kidney disease aerh21HP:0001410	Leukoencephalopathy";
        assert_eq!(
            hpo_id_search(string_to_search),
            vec!["HP:0012622".to_string(), "HP:0001410".to_string()]
        );
    }

    #[rstest]
    fn test_hpo_id_search_no_hits() {
        let string_to_search = "asdasdH:0012622 aerh21 0001410	Leukoencephalopathy";
        let empty_vec = Vec::<String>::new();
        assert_eq!(hpo_id_search(string_to_search), empty_vec);
    }
}
