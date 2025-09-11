use crate::ontology::error::RegistryError;
use flate2::Compression;
use flate2::bufread::GzDecoder;
use flate2::write::GzEncoder;
use ontolius::io::OntologyLoaderBuilder;
use ontolius::ontology::csr::FullCsrOntology;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Write};
use std::path::PathBuf;
use std::rc::Rc;

pub fn init_ontolius(hpo_path: PathBuf) -> Result<Rc<FullCsrOntology>, RegistryError> {
    let loader = OntologyLoaderBuilder::new().obographs_parser().build();

    let mut json_data = Vec::new();
    BufReader::new(File::open(hpo_path)?).read_to_end(&mut json_data)?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

    encoder.write_all(&json_data).unwrap();
    let compressed = encoder.finish().unwrap();
    let reader = GzDecoder::new(Cursor::new(compressed));

    Ok(Rc::new(
        loader.load_from_read(reader).expect("HPO should be loaded"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use rstest::rstest;
    use tempfile::TempDir;

    #[rstest]
    fn test_init_ontolius() {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_init_ontolius");
            return;
        }

        let tmp = TempDir::new().unwrap();
        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp.path().into());
        let path = hpo_registry.register("latest").unwrap();
        let ontolius = init_ontolius(path).unwrap();
    }
}
