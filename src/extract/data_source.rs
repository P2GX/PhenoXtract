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
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    const CSV_DATA: &[u8] = br#"patient_id,hpo_id,disease_id,sex
                                P001,HP:0000505,MONDO:100100,M
                                P002,HP:0000252,MONDO:100200,F
                                P003,HP:0001250,MONDO:100300,M
                                P004,HP:0000768,MONDO:100400,F"#;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_load_csv(temp_dir: TempDir) {
        let file_path = temp_dir.path().join("csv_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(CSV_DATA).unwrap();

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
            vec![],
        );

        let data_source =
            DataSource::Csv(CSVDataSource::new(file_path, None, table_context.clone()));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().unwrap();

        assert_eq!(context_df.context(), &table_context);
        dbg!(&data_frames);
    }
}
