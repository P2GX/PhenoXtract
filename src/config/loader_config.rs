use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LoaderConfig {
    FileSystem {
        output_dir: PathBuf,
        create_dir: bool,
    },
}
