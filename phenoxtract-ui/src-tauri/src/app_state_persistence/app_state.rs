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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state_persistence::app_state::AppState;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_load_non_existent_file_returns_default() {
        let dir = tempdir().expect("Failed to create temp dir");

        let result = AppState::load(dir.path());

        assert!(result.is_ok());
        let state = result.unwrap();
        assert!(state.project_panels.is_empty());
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = tempdir().expect("Failed to create temp dir");
        let state = AppState::default();

        state.save(dir.path()).expect("Failed to save state");
        let loaded_state = AppState::load(dir.path()).expect("Failed to load state");

        assert_eq!(
            state.project_panels.len(),
            loaded_state.project_panels.len()
        );
    }

    #[test]
    fn test_save_to_invalid_path_returns_error() {
        let state = AppState::default();
        let bad_path = Path::new("/some/non/existent/path/that/should/fail");

        let result = state.save(bad_path);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PhenoxtractBackendError::CantWriteAppState(_)
        ));
    }

    #[test]
    fn test_load_malformed_json_returns_error() {
        let dir = tempdir().expect("Failed to create temp dir");
        let file_path = dir.path().join("app_state.json");
        fs::write(&file_path, "{ this is not valid json ]").expect("Failed to write garbage file");

        let result = AppState::load(dir.path());

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PhenoxtractBackendError::CantReadAppState(_)
        ));
    }
}
