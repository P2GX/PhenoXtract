use crate::load::error::LoadError;
use crate::load::traits::Loadable;
use log::warn;
use phenopackets::schema::v2::Phenopacket;
use serde::Deserialize;
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct FileSystemLoader {
    #[allow(unused)]
    pub out_path: PathBuf,
}

impl Loadable for FileSystemLoader {
    fn load(&self, phenopacket: &[Phenopacket]) -> Result<(), LoadError> {
        for pp in phenopacket.iter() {
            let file = File::create(self.out_path.join(format!("{}.json", pp.id)))?;
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
