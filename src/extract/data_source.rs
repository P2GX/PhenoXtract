use crate::config::table_context::TableContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::extractable::Extractable;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct CSVDataSource {
    source: PathBuf,
    separator: Option<String>,
    table: Option<TableContext>,
}
#[derive(Debug, Deserialize)]
pub struct ExcelDatasource {
    source: PathBuf,
    sheets: Option<Vec<TableContext>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum DataSource {
    CSV(CSVDataSource),
    Excel(ExcelDatasource),
}

impl Extractable for DataSource {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error> {
        match self {
            DataSource::CSV(csv_source) => {
                todo!()
            }
            DataSource::Excel(excel_source) => {
                todo!()
            }
        }
    }
}
