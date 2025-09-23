pub mod config;
pub mod extract;
pub mod load;

pub use pipeline::Pipeline;

pub mod pipeline;
pub mod transform;

mod validation;

mod error;

pub mod ontology;

#[macro_export]
macro_rules! skip_in_ci {
    ($test_name:expr) => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", $test_name);
            return;
        }
    };
    () => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", module_path!());
            return;
        }
    };
}
