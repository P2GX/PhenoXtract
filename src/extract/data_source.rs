use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;

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
