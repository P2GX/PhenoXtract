use crate::config::loader_config::LoaderConfig;
use crate::load::FileSystemLoader;
use crate::load::traits::Loadable;
use config::ConfigError;

pub struct LoaderFactory;

impl LoaderFactory {
    pub fn try_from_config(config: LoaderConfig) -> Result<Box<dyn Loadable>, ConfigError> {
        match config {
            LoaderConfig::FileSystem {
                output_dir,
                create_dir,
            } => Ok(Box::new(FileSystemLoader::new(output_dir, create_dir))),
        }
    }
}
