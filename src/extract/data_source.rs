use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;
use polars::io::SerReader;
use polars::prelude::CsvReadOptions;

use crate::extract::excel_data_source::ExcelDatasource;
use crate::extract::traits::Extractable;
use serde::{Deserialize, Serialize};

/// An enumeration of all supported data source types.
///
/// This enum uses a `tag` to differentiate between source types (e.g., "csv", "excel")
/// when deserializing from a configuration file.
#[derive(Debug, Deserialize, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum DataSource {
    Csv(CSVDataSource),
    Excel(ExcelDatasource),
}

impl Extractable for DataSource {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error> {
        match self {
            DataSource::Csv(csv_source) => {
                let csv_dataframe = CsvReadOptions::default()
                    .with_has_header(true)
                    .try_into_reader_with_file_path(Some(csv_source.source.clone()))?
                    .finish()?;

                Ok(vec![ContextualizedDataFrame::new(
                    csv_source.table.clone(),
                    csv_dataframe,
                )])
            }
            DataSource::Excel(_excel_source) => {
                todo!("Excel extraction is not yet implemented.")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::{
        CellContext, Context, Identifier, SeriesContext, SingleSeriesContext, TableContext,
    };
    use rstest::{fixture, rstest};
    use std::fmt::Write;
    use std::fs::File;
    use std::io::Write as StdWrite;
    use tempfile::TempDir;

    #[fixture]
    fn column_names() -> [&'static str; 4] {
        ["patient_id", "hpo_id", "disease_id", "sex"]
    }
    #[fixture]
    fn patient_ids() -> [&'static str; 4] {
        ["P001", "P002", "P003", "P004"]
    }

    #[fixture]
    fn hpo_ids() -> [&'static str; 4] {
        ["HP:0000054", "HP:0000046", "HP:0000040", "HP:0030265"]
    }

    #[fixture]
    fn disease_ids() -> [&'static str; 4] {
        [
            "MONDO:100100",
            "MONDO:100200",
            "MONDO:100300",
            "MONDO:100400",
        ]
    }

    #[fixture]
    fn subject_sexes() -> [&'static str; 4] {
        ["Male", "Female", "Male", "Female"]
    }

    #[fixture]
    fn csv_data(
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
    ) -> Vec<u8> {
        let mut csv_content = column_names.join(",") + "\n";

        for i in 0..patient_ids.len() {
            writeln!(
                &mut csv_content,
                "{},{},{},{}",
                patient_ids[i], hpo_ids[i], disease_ids[i], subject_sexes[i]
            )
            .unwrap();
        }

        csv_content.into_bytes()
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_load_csv(
        temp_dir: TempDir,
        csv_data: Vec<u8>,
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
    ) {
        let file_path = temp_dir.path().join("csv_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(csv_data.as_slice()).unwrap();

        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("patient_id".to_string()),
                Context::None,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                Some("Link_A".to_string()),
                vec!["HP:0000054".to_string()],
            ))],
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("test_row".to_string()),
                Context::None,
                Some(CellContext::new(Context::None, None, Default::default())),
                Some("Link_A".to_string()),
                vec!["HP:0000054".to_string()],
            ))],
        );

        let data_source =
            DataSource::Csv(CSVDataSource::new(file_path, None, table_context.clone()));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().expect("No data");

        assert_eq!(context_df.context(), &table_context);

        let column_data_pairs = [
            ("patient_id", patient_ids),
            ("hpo_id", hpo_ids),
            ("disease_id", disease_ids),
            ("sex", subject_sexes),
        ];

        for (col_name, expected_values) in column_data_pairs.iter() {
            let loaded_data = context_df.data();
            let col_content = loaded_data
                .column(col_name)
                .expect("Failed to load column")
                .str()
                .unwrap();

            for (i, value) in col_content.iter().enumerate() {
                let unwrapped_value = value.unwrap();
                assert_eq!(expected_values[i], unwrapped_value);
            }
        }
    }
}
