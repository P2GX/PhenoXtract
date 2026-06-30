use crate::app_state_persistence::project_panel::ProjectPanel;
use crate::types::SharedAppState;
use std::collections::VecDeque;

#[tauri::command]
pub fn get_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[tauri::command]
pub fn get_project_panels(
    state: tauri::State<SharedAppState>,
) -> Result<VecDeque<ProjectPanel>, String> {
    let app_state = state.read().map_err(|e| e.to_string())?;
    Ok(app_state.project_panels.clone())
}

#[tauri::command]
pub fn post_project_panel(
    project_panels: ProjectPanel,
    state: tauri::State<SharedAppState>,
) -> Result<(), String> {
    let mut app_state = state.write().map_err(|e| e.to_string())?;

    app_state.project_panels.push_front(project_panels);
    Ok(())
}
