use crate::app_state_persistence::app_state::AppState;
use std::sync::{Arc, RwLock};

pub type SharedAppState = Arc<RwLock<AppState>>;
