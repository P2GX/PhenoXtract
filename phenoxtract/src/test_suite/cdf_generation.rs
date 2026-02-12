use crate::config::TableContext;
use crate::config::context::Context;
use crate::config::table_context::{Identifier, SeriesContext};
use crate::config::traits::SeriesContextBuilding;
use crate::extract::ContextualizedDataFrame;
use polars::df;
use polars::prelude::Column;

pub(crate) fn generate_minimal_cdf(
    n_patients: i64,
    n_entries_per_patient: i64,
) -> ContextualizedDataFrame {
    let mut patient_ids = Vec::new();
    for n_pat in generate_patient_ids(n_patients) {
        for _ in 0..n_entries_per_patient {
            patient_ids.push(n_pat.clone());
        }
    }

    let df = df!["subject_id" => patient_ids].unwrap();

    let table_context = TableContext::new(
        "Test",
        vec![
            SeriesContext::default()
                .with_identifier("subject_id")
                .with_data_context(Context::SubjectId),
        ],
    );

    ContextualizedDataFrame::new(table_context, df).unwrap()
}

pub(crate) fn generate_minimal_cdf_components(
    n_patients: i64,
    n_entries_per_patient: i64,
) -> (Column, SeriesContext) {
    let mut patient_ids = Vec::new();
    for n_pat in generate_patient_ids(n_patients) {
        for _ in 0..n_entries_per_patient {
            patient_ids.push(n_pat.clone());
        }
    }

    let column = Column::new("subject_id".into(), patient_ids);

    let series_context = SeriesContext::default()
        .with_identifier(Identifier::from("subject_id"))
        .with_data_context(Context::SubjectId);

    (column, series_context)
}

pub(crate) fn generate_patient_id(n: i64) -> String {
    format!("P{}", n)
}

pub(crate) fn generate_patient_ids(n: i64) -> Vec<String> {
    let mut patient_ids = Vec::new();
    for n_pat in 0..n {
        patient_ids.push(generate_patient_id(n_pat));
    }
    patient_ids
}

pub(crate) fn default_patient_id() -> String {
    generate_patient_id(0)
}
