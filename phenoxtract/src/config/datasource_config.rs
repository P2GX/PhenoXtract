use crate::config::context::Context;
use crate::config::table_context::{CellValue, Identifier, OutputDataType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataSourceConfig {
    Csv(CsvConfig),
    Excel(ExcelWorkbookConfig),
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct CsvConfig {
    pub source: PathBuf,
    #[serde(default)]
    pub separator: Option<char>,
    #[serde(default)]
    pub contexts: Vec<SeriesContextConfig>,
    pub has_headers: bool,
    pub patients_are_rows: bool,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct ExcelWorkbookConfig {
    pub source: PathBuf,
    #[serde(default)]
    pub sheets: Vec<ExcelSheetConfig>,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct ExcelSheetConfig {
    pub sheet_name: String,
    #[serde(default)]
    pub contexts: Vec<SeriesContextConfig>,
    pub has_headers: bool,
    pub patients_are_rows: bool,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct SeriesContextConfig {
    pub identifier: Identifier,
    #[serde(default)]
    pub header_context: Context,
    #[serde(default)]
    pub data_context: Context,
    #[serde(default)]
    pub fill_missing: Option<CellValue>,
    #[serde(default)]
    pub alias_map_config: Option<AliasMapConfig>,
    #[serde(default)]
    pub building_block_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct AliasMapConfig {
    pub mappings: MappingsConfig,
    pub output_data_type: OutputDataType,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum MappingsConfig {
    Path(PathBuf),
    HashMap(HashMap<String, Option<String>>),
}

impl CsvConfig {
    pub fn new(
        source: PathBuf,
        separator: Option<char>,
        contexts: Vec<SeriesContextConfig>,
        has_headers: bool,
        patients_are_rows: bool,
    ) -> Self {
        Self {
            source,
            separator,
            contexts,
            has_headers,
            patients_are_rows,
        }
    }
}

impl ExcelWorkbookConfig {
    pub fn new(source: PathBuf, sheets: Vec<ExcelSheetConfig>) -> Self {
        Self { source, sheets }
    }
}

impl ExcelSheetConfig {
    pub fn new(
        sheet_name: String,
        contexts: Vec<SeriesContextConfig>,
        has_headers: bool,
        patients_are_rows: bool,
    ) -> Self {
        Self {
            sheet_name,
            contexts,
            has_headers,
            patients_are_rows,
        }
    }
}

impl SeriesContextConfig {
    pub fn new(
        identifier: Identifier,
        header_context: Context,
        data_context: Context,
        fill_missing: Option<CellValue>,
        alias_map_config: Option<AliasMapConfig>,
        building_block_id: Option<String>,
    ) -> Self {
        Self {
            identifier,
            header_context,
            data_context,
            fill_missing,
            alias_map_config,
            building_block_id,
        }
    }
}

impl AliasMapConfig {
    pub fn new(mappings: MappingsConfig, output_data_type: OutputDataType) -> Self {
        Self {
            mappings,
            output_data_type,
        }
    }
}
