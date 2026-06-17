use phenoxtract::config::context::{Context, ContextKind};
use strum::IntoEnumIterator;

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}
#[tauri::command]
fn contexts() -> Vec<String> {
    ContextKind::iter().map(|c| c.to_string()).collect()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .invoke_handler(tauri::generate_handler![contexts])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
