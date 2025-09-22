use crate::load::error::LoadError;
use crate::load::traits::Loadable;
use log::warn;
use phenopackets::schema::v2::Phenopacket;
use serde::Deserialize;
use std::fs::File;
use std::path::PathBuf;

/// A loader that saves phenopackets as individual JSON files to the local file system.
///
/// This struct specifies an output directory where each `Phenopacket` will be
/// serialized and saved.
#[derive(Debug, Deserialize)]
pub struct FileSystemLoader {
    /// The path to the output directory where phenopacket files will be saved.
    #[allow(unused)]
    out_path: PathBuf,
}

impl FileSystemLoader {
    pub fn new(out_path: PathBuf) -> Self {
        Self { out_path }
    }
}

impl Loadable for FileSystemLoader {
    /// Saves a slice of `Phenopacket`s to the directory specified in `out_path`.
    ///
    /// Each `Phenopacket` is serialized into a pretty-printed JSON file. The filename
    /// is derived from the phenopacket's ID, followed by the `.json` extension
    /// (e.g., `PMIT-00001.json`).
    ///
    /// If serialization for a specific phenopacket fails, a warning is logged,
    /// and the process continues with the next phenopacket.
    ///
    /// # Parameters
    ///
    /// * `phenopackets`: A slice of `Phenopacket`s to be saved.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the operation completes (even if individual file writes fail).
    /// * `Err(LoadError)` if a file cannot be created due to an I/O error (e.g., permissions).
    fn load(&self, phenopackets: &[Phenopacket]) -> Result<(), LoadError> {
        for pp in phenopackets.iter() {
            let file = File::create(self.out_path.join(format!("{}.json", pp.id)))?;
            warn!("Storing file to: {:?}", file);
            let res = serde_json::to_writer_pretty(file, pp);
            if res.is_err() {
                warn!(
                    "Could not save Phenopacket for subject: {}. Error: {:?}",
                    pp.clone().subject.unwrap().id.as_str(),
                    res
                )
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::fs;
    use tempfile::tempdir;

    #[rstest]
    fn test_filesystem_loader_writes_json_files() {
        let tmp_dir = tempdir().unwrap();
        let loader = FileSystemLoader {
            out_path: tmp_dir.path().to_path_buf(),
        };

        let phenopacket = Phenopacket {
            id: "test123".to_string(),
            ..Default::default()
        };

        loader
            .load(std::slice::from_ref(&phenopacket))
            .expect("load should succeed");

        let output_path = tmp_dir.path().join("test123.json");
        assert!(output_path.exists(), "Expected file to be created");

        let contents = fs::read_to_string(&output_path).unwrap();
        let json: Phenopacket = serde_json::from_str(&contents).unwrap();

        assert_eq!(json.id, phenopacket.id);
    }
}
