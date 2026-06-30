use crate::app_state_persistence::project_panel::ProjectPanel;
use crate::error::PhenoxtractBackendError;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::Path;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct AppState {
    pub project_panels: VecDeque<ProjectPanel>,
}

impl AppState {
    pub fn save(&self, path: &Path) -> Result<(), PhenoxtractBackendError> {
        let path = path.join("app_state.json");

        let file = std::fs::File::create(path)
            .map_err(|err| PhenoxtractBackendError::CantWriteAppState(err.to_string()))?;
        serde_json::to_writer_pretty(&file, &self)
            .map_err(|err| PhenoxtractBackendError::CantWriteAppState(err.to_string()))
    }

    pub fn load(path: &Path) -> Result<Self, PhenoxtractBackendError> {
        let path = path.join("app_state.json");

        if path.exists() {
            let state_str = std::fs::read_to_string(path)
                .map_err(|err| PhenoxtractBackendError::CantReadAppState(err.to_string()))?;
            serde_json::from_str(&state_str)
                .map_err(|err| PhenoxtractBackendError::CantReadAppState(err.to_string()))
        } else {
            Ok(Self::default())
        }
    }
}
