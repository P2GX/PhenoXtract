use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use log::info;
use polars::prelude::{Column, DataType};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct MultiHPOColExpansionStrategy;
impl Strategy for MultiHPOColExpansionStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().all(|table| {
            table.contexts_have_dtype(&Context::None, &Context::MultiHPOId, &DataType::String)
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), TransformError> {
        for table in tables.iter_mut() {
            let table_name = &table.context().name;
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

            let multi_hpo_cols = table.get_cols_with_data_context(&Context::MultiHpoId);

            let mut patient_to_hpo: HashMap<String, HashSet<String>> = HashMap::new();
            let mut hpos = HashSet::new();

            //first the relevant data is collected
            for multi_hpo_col in multi_hpo_cols {
                let patient_id_multi_hpo_pairs = multi_hpo_col
                    .str()
                    .unwrap()
                    .into_iter()
                    .zip(subject_id_col.str().unwrap().into_iter());

                for (patient_id, multi_hpo) in patient_id_multi_hpo_pairs {
                    let hpo_ids = get_hpo_ids_from_string(multi_hpo);

                    let patient_hm_entry = patient_to_hpo.entry(patient_id.clone()).or_default();

                    hpo_ids.iter().for_each(|hpo_id| {
                        hpos.insert(hpo_id.clone());
                        patient_hm_entry.insert(hpo_id.clone());
                    })
                }
            }

            let new_hpo_col_names = hpos.iter().collect::<Vec<&String>>();

            for hpo_col_name in new_hpo_col_names {
                //first we create the column
                let mut observation_statuses = vec![];
                stringified_subject_id_col.iter().for_each(|patient_id| {
                    if patient_to_hpo
                        .get(patient_id)
                        .unwrap()
                        .contains(hpo_col_name)
                    {
                        observation_statuses.push("OBSERVED");
                    } else {
                        observation_statuses.push("UNKNOWN");
                    }
                });
                let new_hpo_column = Column::new(hpo_col_name.into(), observation_statuses);

                //then we add it to the table
                table
                    .data_mut()
                    .with_column(new_hpo_column)
                    .map_err(|_err| StrategyError("blah".to_string()))?;
            }

            //and we drop the old multi_hpo_columns
            let multi_hpo_col_names = multi_hpo_cols
                .iter()
                .map(|col| col.name().to_string())
                .collect::<Vec<String>>();

            for multi_hpo_col_name in multi_hpo_col_names.iter() {
                table.data_mut().drop_in_place(multi_hpo_col_name).unwrap();
            }
        }

        Ok(())
    }
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
                    SubjectId,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("age".to_string()),
                    Default::default(),
                    SubjectAge,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("HPO".to_string()),
                    Default::default(),
                    MultiHpoId,
                    None,
                    None,
                    vec![],
                ),
            ],
        );

        ContextualizedDataFrame::new(tc, df)
    }

    #[rstest]
    fn test_multi_hpo_col_to_observed_hpo_cols_success() {
        let mut table = make_test_dataframe();

        dbg!(&table.data);

        let strategy = MultiHPOColToObservedHpoColsStrategy;

        strategy.transform(&mut table).unwrap();

        dbg!(&table.data);
    }
}
