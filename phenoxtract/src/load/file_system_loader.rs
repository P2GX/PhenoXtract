use crate::load::error::LoadError;
use crate::load::traits::Loadable;
use log::debug;
use phenopackets::schema::v2::Phenopacket;
use serde::Deserialize;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::path::PathBuf;

/// A loader that saves phenopackets as individual JSON files to the local file system.
///
/// This struct specifies an output directory where each `Phenopacket` will be
/// serialized and saved.
#[derive(Debug, Deserialize)]
pub struct FileSystemLoader {
    /// The path to the output directory where phenopacket files will be saved.
    out_path: PathBuf,
    /// If true will create the full out path
    create_dir: bool,
}

impl FileSystemLoader {
    pub fn new(out_path: PathBuf, create_dir: bool) -> Self {
        Self {
            out_path,
            create_dir,
        }
    }

    pub fn remove_default_survival_time(phenopacket: &mut Value) -> Result<(), LoadError> {
        if let Some(vital_status) = phenopacket.pointer_mut("/subject/vitalStatus")
            && let Some(vital_status_obj) = vital_status.as_object_mut()
        {
            let should_remove = vital_status_obj
                .get("survivalTimeInDays")
                .and_then(|v| v.as_i64())
                .map(|days| days == 0)
                .unwrap_or(false);

            if should_remove {
                vital_status_obj.remove("survivalTimeInDays");
            }
        }

        Ok(())
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
        if !phenopackets.is_empty() && self.create_dir {
            fs::create_dir_all(self.out_path.as_path()).map_err(|err| LoadError::NoStorage {
                reason: err.to_string(),
            })?;
        }
        for pp in phenopackets.iter() {
            let file =
                File::create(self.out_path.join(format!("{}.json", pp.id))).map_err(|err| {
                    LoadError::CantStore {
                        pp_id: pp.id.clone(),
                        reason: err.to_string(),
                    }
                })?;

            debug!("Storing file to: {:?}", file);
            let mut pp_value =
                serde_json::to_value(pp).map_err(|_| LoadError::ConversionError {
                    pp_id: pp.id.clone(),
                    format: "json".to_string(),
                })?;

            Self::remove_default_survival_time(&mut pp_value)?;
            serde_json::to_writer_pretty(file, &pp_value).map_err(|err| LoadError::CantStore {
                pp_id: pp.id.clone(),
                reason: err.to_string(),
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_suite::cdf_generation::default_patient_id;
    use crate::test_suite::phenopacket_component_generation::default_phenopacket_id;
    use phenopackets::schema::v2::core::{Individual, VitalStatus};
    use rstest::*;
    use std::fs;
    use tempfile::tempdir;

    #[rstest]
    fn test_filesystem_loader_writes_json_files() {
        let tmp_dir = tempdir().unwrap();
        let loader = FileSystemLoader {
            out_path: tmp_dir.path().to_path_buf(),
            create_dir: true,
        };

        let phenopacket = Phenopacket {
            id: default_phenopacket_id(),
            subject: Some(Individual {
                id: default_patient_id(),
                vital_status: Some(VitalStatus {
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        loader
            .load(std::slice::from_ref(&phenopacket))
            .expect("load should succeed");

        let output_path = tmp_dir
            .path()
            .join(format!("{}.json", default_phenopacket_id()));
        assert!(output_path.exists(), "Expected file to be created");

        let contents = fs::read_to_string(&output_path).unwrap();
        let json: Value = serde_json::from_str(&contents).unwrap();

        assert_eq!(json.get("id").unwrap().as_str().unwrap(), phenopacket.id);
        assert!(!contents.contains("survivalTimeInDays"));
    }
}
