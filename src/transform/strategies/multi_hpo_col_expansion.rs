use crate::config::table_context::{Context, Identifier, SeriesContext};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use log::{info, warn};
use ordermap::{OrderMap, OrderSet};
use polars::prelude::{AnyValue, Column, DataType, StringChunked};
use regex::Regex;

/// A strategy for converting columns whose cells contain HPO IDs
/// into several columns whose headers are exactly those HPO IDs
/// and whose cells contain the ObservationStatus for each patient.
///
/// The columns are created on a "block by block" basis
/// so that building blocks are preserved after the transformation.
///
/// A new SeriesContext will be added for each block of new columns.
///
/// The old columns and contexts will be removed.
#[derive(Debug)]
#[allow(dead_code)]
pub struct MultiHPOColExpansionStrategy;
impl Strategy for MultiHPOColExpansionStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::MultiHpoId))
                .where_dtype(Filter::Is(&DataType::String))
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), TransformError> {
        for table in tables.iter_mut() {
            if table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::MultiHpoId))
                .collect()
                .is_empty()
            {
                continue;
            }

            let table_name = table.context().name().to_string();
            info!("Applying MultiHPOColExpansion strategy to table: {table_name}");

            let stringified_subject_id_col = table.filter_columns().where_data_context(Filter::Is(&Context::SubjectId)).collect()
                .last()
                .ok_or(StrategyError(format!(
                    "Could not find SubjectID column in table {table_name}"
                )))?.str()
                .map_err(|_| {
                    StrategyError("Unexpectedly could not convert SubjectID column to string column when applying MultiHPOColExpansion strategy.".to_string())})?;

            let mut new_hpo_cols = vec![];
            let mut new_series_contexts = vec![];

            let mut bb_ids = table
                .get_building_block_ids()
                .iter()
                .map(|&bb_id| Some(bb_id))
                .collect::<Vec<Option<&str>>>();
            bb_ids.push(None);
            bb_ids.sort();

            for bb_id in bb_ids {
                let multi_hpo_filter = table
                    .filter_columns()
                    .where_header_context(Filter::Is(&Context::None))
                    .where_data_context(Filter::Is(&Context::MultiHpoId));

                let multi_hpo_block = match bb_id {
                    None => multi_hpo_filter
                        .where_building_block(Filter::IsNone)
                        .collect(),
                    Some(bb_id) => multi_hpo_filter
                        .where_building_block(Filter::Is(bb_id))
                        .collect(),
                };

                let stringified_multi_hpo_block = multi_hpo_block.iter()
                    .map(|col| {
                        col.str().map_err(|_| StrategyError(
                            "Unexpectedly could not convert SubjectID column to string column when applying MultiHPOColExpansion strategy.".to_string()
                        ))
                    })
                    .collect::<Result<Vec<&StringChunked>, TransformError>>()?;

                let patient_to_hpo = Self::get_patient_to_hpo_data(
                    stringified_subject_id_col,
                    stringified_multi_hpo_block,
                );

                let (new_hpo_cols_from_this_block, new_sc) = Self::create_new_cols_with_sc(
                    stringified_subject_id_col,
                    bb_id,
                    patient_to_hpo,
                );

                new_hpo_cols.extend(new_hpo_cols_from_this_block);
                new_series_contexts.push(new_sc);
            }

            for new_hpo_col in new_hpo_cols {
                let new_hpo_col_name = new_hpo_col.name().clone();
                table
                    .data_mut()
                    .with_column(new_hpo_col)
                    .map_err(|_| StrategyError(format!("Unexpectedly could not add HPO column {new_hpo_col_name} to table {table_name}. Possible duplicates?")))?;
            }

            for new_sc in new_series_contexts {
                table.add_series_context(new_sc);
            }

            table.remove_scs_and_cols_with_context(&Context::None, &Context::MultiHpoId);
        }

        Ok(())
    }
}

impl MultiHPOColExpansionStrategy {
    fn hpo_id_search(string_to_search: &str) -> Vec<&str> {
        let hpo_regex = Regex::new(r"HP:\d{7}").unwrap();
        hpo_regex
            .find_iter(string_to_search)
            .map(|mat| mat.as_str())
            .collect()
    }

    /// This function takes a SubjectID column and several MultiHPO columns
    /// and creates a patient-to-HPO HashMap (=patient_to_hpo)
    /// where the keys are the SubjectIDs and the values are the set of HPOs observed for that patient.
    fn get_patient_to_hpo_data<'a, 'b>(
        stringified_subject_id_col: &'a StringChunked,
        stringified_multi_hpo_cols: Vec<&'b StringChunked>,
    ) -> OrderMap<&'a str, OrderSet<&'b str>> {
        let mut patient_to_hpo: OrderMap<&'a str, OrderSet<&'b str>> = OrderMap::new();

        for stringified_multi_hpo_col in stringified_multi_hpo_cols {
            let patient_id_multi_hpo_pairs = stringified_subject_id_col
                .iter()
                .zip(stringified_multi_hpo_col.iter());

            for (patient_id, multi_hpo) in patient_id_multi_hpo_pairs {
                if let Some(multi_hpo) = multi_hpo {
                    match patient_id {
                        None => {
                            warn!(
                                "The entry {multi_hpo} in the column {} was found with no corresponding SubjectID.",
                                stringified_multi_hpo_col.name()
                            );
                            continue;
                        }
                        Some(patient_id) => {
                            let hpo_ids = Self::hpo_id_search(multi_hpo);
                            let patient_to_hpo_entry =
                                patient_to_hpo.entry(patient_id).or_default();

                            hpo_ids.iter().for_each(|hpo_id| {
                                patient_to_hpo_entry.insert(hpo_id);
                            })
                        }
                    }
                }
            }
        }

        patient_to_hpo
    }

    /// Given some patient_to_hpo data (=patient_to_hpo)
    /// this function will appropriately construct new HPO columns and a new series context.
    fn create_new_cols_with_sc(
        stringified_subject_id_col: &StringChunked,
        building_block_id: Option<&str>,
        patient_to_hpo: OrderMap<&str, OrderSet<&str>>,
    ) -> (Vec<Column>, SeriesContext) {
        let hpos = patient_to_hpo
            .clone()
            .into_values()
            .flatten()
            .collect::<OrderSet<&str>>();

        let mut new_hpo_cols = vec![];
        let mut new_hpo_col_names = vec![];

        for hpo in hpos {
            let observation_statuses: Vec<AnyValue> = stringified_subject_id_col
                .iter()
                .map(|patient_id| {
                    patient_id
                        .and_then(|id| patient_to_hpo.get(id))
                        .filter(|hpos| hpos.contains(hpo))
                        .map(|_| AnyValue::Boolean(true))
                        .unwrap_or(AnyValue::Null)
                })
                .collect();

            let new_hpo_col_name = match building_block_id {
                None => hpo.to_string(),
                Some(block_id) => format!("{hpo}#(block {block_id})"),
            };

            let new_hpo_col = Column::new(new_hpo_col_name.clone().into(), observation_statuses);
            new_hpo_cols.push(new_hpo_col);
            new_hpo_col_names.push(new_hpo_col_name);
        }
        let new_sc = SeriesContext::default()
            .with_identifier(Identifier::Multi(new_hpo_col_names.clone()))
            .with_header_context(Context::HpoId)
            .with_data_context(Context::ObservationStatus)
            .with_building_block_id(building_block_id.map(|bb_id| bb_id.to_string()));

        (new_hpo_cols, new_sc)
    }
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
                AnyValue::String("patient 2 - asd HP:2222222HP:3333333asd"),
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
            "HP:1111111" => &[
                AnyValue::Boolean(true),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,],
            "HP:4444444" => &[
                AnyValue::Null,
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Boolean(true)],
            "HP:5555555" => &[
                AnyValue::Null,
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Null,],
            "HP:1111111 (block A)" => &[
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Null,],
            "HP:2222222 (block A)" => &[
                AnyValue::Null,
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Null,],
            "HP:3333333 (block A)" => &[
                AnyValue::Null,
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
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
                        "HP:1111111".to_string(),
                        "HP:4444444".to_string(),
                        "HP:5555555".to_string(),
                    ]))
                    .with_header_context(Context::HpoId)
                    .with_data_context(Context::ObservationStatus),
                SeriesContext::default()
                    .with_identifier(Identifier::Multi(vec![
                        "HP:1111111#(block A)".to_string(),
                        "HP:2222222#(block A)".to_string(),
                        "HP:3333333#(block A)".to_string(),
                    ]))
                    .with_header_context(Context::HpoId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id(Some("A".to_string())),
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
            MultiHPOColExpansionStrategy::hpo_id_search(string_to_search),
            vec!["HP:0012622".to_string(), "HP:0001410".to_string()]
        );
    }

    #[rstest]
    fn test_hpo_id_search_no_hits() {
        let string_to_search = "asdasdH:0012622 aerh21 0001410	Leukoencephalopathy";
        let empty_vec = Vec::<String>::new();
        assert_eq!(
            MultiHPOColExpansionStrategy::hpo_id_search(string_to_search),
            empty_vec
        );
    }

    #[rstest]
    fn test_get_patient_to_hpo_data(cdf: ContextualizedDataFrame) {
        let stringified_subject_id_col = cdf.data().column("subject_id").unwrap().str().unwrap();
        let hpo_col_indexes = vec![2, 3, 4, 5];
        let stringified_multi_hpo_cols = hpo_col_indexes
            .into_iter()
            .map(|idx| cdf.data().get_columns()[idx].str().unwrap())
            .collect::<Vec<&StringChunked>>();
        let patient_to_hpo = MultiHPOColExpansionStrategy::get_patient_to_hpo_data(
            stringified_subject_id_col,
            stringified_multi_hpo_cols,
        );

        let mut expected_patient_1_hpos = OrderSet::new();
        expected_patient_1_hpos.insert("HP:1111111");

        let mut expected_patient_2_hpos = OrderSet::new();
        expected_patient_2_hpos.extend(vec![
            "HP:2222222",
            "HP:3333333",
            "HP:1111111",
            "HP:4444444",
            "HP:5555555",
        ]);

        let mut expected_patient_3_hpos = OrderSet::new();
        expected_patient_3_hpos.insert("HP:4444444");

        let mut expected_patient_to_hpo = OrderMap::new();
        expected_patient_to_hpo.insert("P001", expected_patient_1_hpos);
        expected_patient_to_hpo.insert("P002", expected_patient_2_hpos);
        expected_patient_to_hpo.insert("P003", expected_patient_3_hpos);
        assert_eq!(patient_to_hpo, expected_patient_to_hpo)
    }

    #[rstest]
    fn test_create_new_cols_with_sc(cdf: ContextualizedDataFrame) {
        let stringified_subject_id_col = cdf.data().column("subject_id").unwrap().str().unwrap();

        let mut patient_1_hpos = OrderSet::new();
        patient_1_hpos.extend(vec!["HP:1111111", "HP:2222222"]);
        let mut patient_2_hpos = OrderSet::new();
        patient_2_hpos.insert("HP:2222222");
        let patient_3_hpos = OrderSet::new();

        let mut patient_to_hpo = OrderMap::new();
        patient_to_hpo.insert("P001", patient_1_hpos);
        patient_to_hpo.insert("P002", patient_2_hpos);
        patient_to_hpo.insert("P003", patient_3_hpos);

        let (new_cols, new_sc) = MultiHPOColExpansionStrategy::create_new_cols_with_sc(
            stringified_subject_id_col,
            Some("A"),
            patient_to_hpo,
        );

        let expected_col1 = Column::new(
            "HP:1111111#(block A)".into(),
            vec![
                AnyValue::Boolean(true),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let expected_col2 = Column::new(
            "HP:2222222#(block A)".into(),
            vec![
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Null,
            ],
        );

        assert_eq!(new_cols, vec![expected_col1, expected_col2]);

        let expected_sc = SeriesContext::default()
            .with_header_context(Context::HpoId)
            .with_data_context(Context::ObservationStatus)
            .with_building_block_id(Some("A".to_string()))
            .with_identifier(Identifier::Multi(vec![
                "HP:1111111#(block A)".to_string(),
                "HP:2222222#(block A)".to_string(),
            ]));

        assert_eq!(new_sc, expected_sc);
    }
}
