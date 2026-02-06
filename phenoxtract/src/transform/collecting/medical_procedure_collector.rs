use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;

use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use polars::prelude::StringChunked;
use std::any::Any;

struct ProcedureIterator<'a> {
    procedure_col: &'a StringChunked,
    body_part_col: Option<&'a StringChunked>,
    time_element_col: Option<&'a StringChunked>,
    current_index: usize,
}

struct ProcedureIterElement<'a> {
    pub procedure: &'a str,
    pub body_part: Option<&'a str>,
    pub time_element: Option<&'a str>,
}
impl<'a> ProcedureIterator<'a> {
    pub fn new(
        procedure_col: &'a StringChunked,
        body_part_col: Option<&'a StringChunked>,
        time_element_col: Option<&'a StringChunked>,
    ) -> Self {
        Self {
            procedure_col,
            body_part_col,
            time_element_col,
            current_index: 0,
        }
    }
}

impl<'a> Iterator for ProcedureIterator<'a> {
    type Item = ProcedureIterElement<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.procedure_col.len();

        if self.current_index >= len {
            return None;
        }

        let procedure = self.procedure_col.get(self.current_index)?;

        let body_part = self
            .body_part_col
            .as_ref()
            .and_then(|col| col.get(self.current_index));

        let time_element = self
            .time_element_col
            .as_ref()
            .and_then(|col| col.get(self.current_index));

        self.current_index += 1;

        Some(ProcedureIterElement {
            procedure,
            body_part,
            time_element,
        })
    }
}

#[derive(Debug)]
pub struct MedicalProcedureCollector;

impl Collect for MedicalProcedureCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let procedures = patient_cdf
                .filter_series_context()
                .where_data_context(Filter::Is(&Context::ProcedureLabelOrId))
                .collect();

            for procedure_sc in procedures {
                let body_part_col = patient_cdf.get_single_linked_column_as_str(
                    procedure_sc.get_building_block_id(),
                    &[Context::ProcedureBodySide],
                )?;

                let procedure_time_element_col = patient_cdf.get_single_linked_column_as_str(
                    procedure_sc.get_building_block_id(),
                    &[Context::ProcedureTimeElement],
                )?;

                let procedure_col = patient_cdf
                    .get_columns(procedure_sc.get_identifier())
                    .first()
                    .unwrap_or_else(|| panic!("Found dangling SeriesContext with for identifier {}. Validation should make this impossible.",
                        procedure_sc.get_identifier())).str()?;

                let procedure_iterator = ProcedureIterator::new(
                    procedure_col,
                    body_part_col.as_ref(),
                    procedure_time_element_col.as_ref(),
                );

                for procedure_values in procedure_iterator {
                    builder.insert_medical_procedure(
                        patient_id,
                        procedure_values.procedure,
                        procedure_values.body_part,
                        procedure_values.time_element,
                    )?
                }
            }
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::extract::ContextualizedDataFrame;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf};
    use crate::test_suite::phenopacket_component_generation::default_procedure;
    use crate::test_suite::phenopacket_component_generation::{
        default_phenotype, default_procedure_body_side_oc, default_timestamp,
    };

    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series, TimeUnit};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn cdf() -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);
        let procedure = Series::new(
            "procedure".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_procedure().clone().code.unwrap().label),
            ],
        );

        let body_site = Series::new(
            "body_site".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_procedure_body_side_oc().label),
            ],
        );

        let time_element = Series::new(
            "at".into(),
            &[
                AnyValue::Null,
                AnyValue::Datetime(
                    default_timestamp().nanos as i64,
                    TimeUnit::Nanoseconds,
                    None,
                ),
            ],
        );

        patient_cdf
            .builder()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("procedure".into())
                    .with_data_context(Context::ProcedureLabelOrId)
                    .with_building_block_id(Some("procedure_1".to_string())),
                vec![procedure.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("body_site".into())
                    .with_data_context(Context::ProcedureBodySide)
                    .with_building_block_id(Some("procedure_1".to_string())),
                vec![body_site.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("at".into())
                    .with_data_context(Context::ProcedureTimeElement)
                    .with_building_block_id(Some("procedure_1".to_string())),
                vec![time_element.into_column()].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap()
            .clone()
    }

    #[rstest]
    fn test_collect_procedure(temp_dir: TempDir, cdf: ContextualizedDataFrame) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let collector = MedicalProcedureCollector;

        let patient_id = default_patient_id();

        let mut fractured_nose_excluded = default_phenotype().clone();
        fractured_nose_excluded.excluded = true;

        collector
            .collect(&mut builder, &[cdf], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();
    }
}
