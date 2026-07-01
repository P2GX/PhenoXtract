use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectPanel {
    pub name: String,
    pub directory: String,
}

impl ProjectPanel {
    pub fn new(name: impl Into<String>, directory: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            directory: directory.into(),
        }
    }
}
