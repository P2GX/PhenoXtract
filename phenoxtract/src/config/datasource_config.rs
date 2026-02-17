use crate::config::context::Context;
use crate::config::table_context::{CellValue, Identifier, OutputDataType};
use crate::config::traits::{IntoOptionalString, SeriesContextBuilding};
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
    #[serde(default)]
    pub sub_blocks: Vec<String>,
}

impl SeriesContextBuilding<AliasMapConfig> for SeriesContextConfig {
    fn from_identifier(identifier: impl Into<Identifier>) -> Self {
        Self {
            identifier: identifier.into(),
            header_context: Context::default(),
            data_context: Context::default(),
            fill_missing: None,
            alias_map_config: None,
            building_block_id: None,
            sub_blocks: vec![],
        }
    }

    fn with_identifier(mut self, identifier: impl Into<Identifier>) -> Self {
        self.identifier = identifier.into();
        self
    }

    fn with_header_context(mut self, header_context: Context) -> Self {
        self.header_context = header_context;
        self
    }

    fn with_data_context(mut self, data_context: Context) -> Self {
        self.data_context = data_context;
        self
    }

    fn with_fill_missing(mut self, fill_missing: CellValue) -> Self {
        self.fill_missing = Some(fill_missing);
        self
    }

    fn with_alias_map(mut self, alias_map_config: AliasMapConfig) -> Self {
        self.alias_map_config = Some(alias_map_config);
        self
    }

    fn with_building_block_id(mut self, building_block_id: impl IntoOptionalString) -> Self {
        if let Some(id) = building_block_id.into_opt_string() {
            self.building_block_id = Some(id);
            self
        } else {
            self.building_block_id = None;
            self
        }
    }

    fn push_sub_building_block(mut self, building_block_id: impl Into<String>) -> Self {
        self.sub_blocks.push(building_block_id.into());
        self
    }

    fn with_sub_building_blocks(mut self, sub_building_blocks: Vec<impl Into<String>>) -> Self {
        self.sub_blocks = sub_building_blocks.into_iter().map(Into::into).collect();
        self
    }
}

impl SeriesContextConfig {
    pub fn new(identifier: impl Into<Identifier>) -> Self {
        Self {
            identifier: identifier.into(),
            header_context: Context::default(),
            data_context: Context::default(),
            fill_missing: None,
            alias_map_config: None,
            building_block_id: None,
            sub_blocks: vec![],
        }
    }

    pub fn header_context(mut self, header_context: Context) -> Self {
        self.header_context = header_context;
        self
    }

    pub fn data_context(mut self, data_context: Context) -> Self {
        self.data_context = data_context;
        self
    }

    pub fn fill_missing(mut self, fill_missing: CellValue) -> Self {
        self.fill_missing = Some(fill_missing);
        self
    }

    pub fn alias_map_config(mut self, alias_map_config: AliasMapConfig) -> Self {
        self.alias_map_config = Some(alias_map_config);
        self
    }

    pub fn building_block_id(mut self, building_block_id: String) -> Self {
        self.building_block_id = Some(building_block_id);
        self
    }
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct AliasMapConfig {
    pub mappings: MappingsConfig,
    pub output_data_type: OutputDataType,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum MappingsConfig {
    Csv(MappingsCsvConfig),
    HashMap(HashMap<String, Option<String>>),
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct MappingsCsvConfig {
    pub path: PathBuf,
    pub key_column_name: String,
    pub alias_column_name: String,
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

impl AliasMapConfig {
    pub fn new(mappings: MappingsConfig, output_data_type: OutputDataType) -> Self {
        Self {
            mappings,
            output_data_type,
        }
    }
}

impl MappingsCsvConfig {
    pub fn new(path: PathBuf, key_column_name: String, alias_column_name: String) -> Self {
        MappingsCsvConfig {
            path,
            key_column_name,
            alias_column_name,
        }
    }
}
