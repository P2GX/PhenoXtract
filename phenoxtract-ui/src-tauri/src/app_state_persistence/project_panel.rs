use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectPanel {
    pub name: String,
    pub dir: String,
    #[serde(rename = "squareColor")]
    pub square_color: String,
}

impl ProjectPanel {
    pub fn new(
        name: impl Into<String>,
        dir: impl Into<String>,
        square_color: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            dir: dir.into(),
            square_color: square_color.into(),
        }
    }
}
