use ontolius::io::OntologyLoaderBuilder;
use ontolius::ontology::csr::FullCsrOntology;
use std::path::PathBuf;
use std::rc::Rc;

#[allow(dead_code)]
pub(crate) fn init_ontolius(hpo_path: PathBuf) -> Result<Rc<FullCsrOntology>, anyhow::Error> {
    let loader = OntologyLoaderBuilder::new().obographs_parser().build();

    let ontolius = loader.load_from_path(hpo_path.clone())?;
    Ok(Rc::new(ontolius))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use ontolius::ontology::OntologyTerms;
    use ontolius::term::MinimalTerm;
    use ontolius::{Identified, TermId};
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

        let term_id: TermId = "HP:0100729".parse().unwrap();

        let term = ontolius.as_ref().term_by_id(&term_id).unwrap();
        assert_eq!(term.name(), "Large face");

        let term = ontolius
            .as_ref()
            .iter_terms()
            .find(|term| term.name() == "Large face")
            .unwrap();
        assert_eq!(term.identifier(), &term_id);
    }
}
