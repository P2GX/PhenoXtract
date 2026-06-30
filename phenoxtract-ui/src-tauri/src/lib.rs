use crate::app_dirs::{init_app_dirs, AppDirs};
use crate::app_state_persistence::app_state::AppState;
use crate::app_state_persistence::project_panel::ProjectPanel;
use crate::commands::{get_project_panels, get_version, post_project_panel};
use std::sync::{Arc, RwLock};
use tauri::Manager;

mod app_dirs;
mod app_state_persistence;
mod commands;
mod error;
mod types;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let dummy_panels = vec![
        ProjectPanel::new("Immunology Data", "~/projects/my-project", "#ff3e00"),
        ProjectPanel::new("prechter_data_analysis", "~/projects/my-project", "#ff00ff"),
        ProjectPanel::new("acuteKidneyInjury", "~/projects/my-project", "orange"),
    ];

    let app_dirs = init_app_dirs().expect("Could not determine application directories");

    let mut state = AppState::load(app_dirs.config_dir()).expect("Could not load app state");

    state.project_panels = dummy_panels.into_iter().collect();

    tauri::Builder::default()
        .manage(Arc::new(RwLock::new(state)))
        .manage(app_dirs)
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            get_version,
            get_project_panels,
            post_project_panel
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| match event {
            tauri::RunEvent::ExitRequested { .. } => {
                let app_dirs = app_handle.state::<AppDirs>();
                let state_manager = app_handle.state::<Arc<RwLock<AppState>>>();
                let state = state_manager.read().expect("Could not read state");
                state
                    .save(app_dirs.config_dir())
                    .expect("Could not save state");
            }
            tauri::RunEvent::Exit => {
                // cleanup, app is exiting, can't be stopped here
            }
            _ => {}
        });
}
