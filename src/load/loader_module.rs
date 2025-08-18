use crate::load::file_system_loader::FileSystemLoader;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
#[allow(dead_code)]
enum Loader {
    #[allow(unused)]
    FileSystem(FileSystemLoader),
}
