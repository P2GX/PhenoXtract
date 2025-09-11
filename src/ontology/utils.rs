use crate::ontology::error::RegistryError;
use ontolius::io::OntologyLoaderBuilder;
use ontolius::ontology::csr::FullCsrOntology;
use std::path::PathBuf;
use std::rc::Rc;

pub(crate) fn init_ontolius(hpo_path: PathBuf) -> Result<Rc<FullCsrOntology>, RegistryError> {
    let loader = OntologyLoaderBuilder::new().obographs_parser().build();

    Ok(Rc::new(loader.load_from_path(hpo_path.clone()).expect(
        &format!("Failed to load from {}", hpo_path.display()),
    )))
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
