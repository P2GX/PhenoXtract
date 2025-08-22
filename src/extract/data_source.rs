use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;
use polars::io::SerReader;
use polars::prelude::CsvReadOptions;

use std::sync::Arc;

use crate::extract::error::ExtractionError;
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
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, ExtractionError> {
        match self {
            DataSource::Csv(csv_source) => {
                let mut csv_read_options =
                    CsvReadOptions::default().with_has_header(csv_source.has_header);

                if let Some(sep) = csv_source.separator {
                    let new_parse_options = (*csv_read_options.parse_options)
                        .clone()
                        .with_separator(sep as u8);
                    csv_read_options.parse_options = Arc::from(new_parse_options);
                }
                let csv_data = csv_read_options
                    .try_into_reader_with_file_path(Some(csv_source.source.clone()))?
                    .finish()?;

                Ok(vec![ContextualizedDataFrame::new(
                    csv_source.context.clone(),
                    csv_data,
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
        CellContext, Context, SeriesContext, SingleSeriesContext, TableContext,
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
        column_names: [&'static str; 4],
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
                "patient_id".to_string(),
                Context::None,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                vec!["HP:0000054".to_string()],
            ))],
            true,
        );

        let data_source = DataSource::Csv(CSVDataSource::new(
            file_path,
            Some(','),
            table_context.clone(),
            true,
        ));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().expect("No data");

        assert_eq!(context_df.context(), &table_context);

        let column_data: [&[&str]; 4] = [&patient_ids, &hpo_ids, &disease_ids, &subject_sexes];

        let column_data_pairs: Vec<(&str, &[&str])> = column_names
            .iter()
            .zip(column_data.iter())
            .map(|(&col_name, &col_data)| (col_name, col_data))
            .collect();

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
