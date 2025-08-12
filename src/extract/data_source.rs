use crate::config::table_context::TableContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::extractable::Extractable;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct CSVDataSource {
    #[allow(unused)]
    source: PathBuf,
    #[allow(unused)]
    separator: Option<String>,
    #[allow(unused)]
    table: Option<TableContext>,
}
#[derive(Debug, Deserialize)]
pub struct ExcelDatasource {
    #[allow(unused)]
    source: PathBuf,
    #[allow(unused)]
    sheets: Option<Vec<TableContext>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum DataSource {
    Csv(CSVDataSource),
    Excel(ExcelDatasource),
}

impl Extractable for DataSource {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error> {
        match self {
            // Rename input withoug _, when implementing
            DataSource::Csv(_csv_source) => {
                todo!()
            }
            DataSource::Excel(_excel_source) => {
                todo!()
            }
        }
    }
}
