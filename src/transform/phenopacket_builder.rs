use crate::transform::error::TransformError;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use std::collections::HashMap;
use std::rc::Rc;

#[allow(dead_code)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
    hpo: Rc<FullCsrOntology>,
}

impl PhenopacketBuilder {
    pub fn new(hpo: Rc<FullCsrOntology>) -> PhenopacketBuilder {
        PhenopacketBuilder {
            subject_to_phenopacket: HashMap::default(),
            hpo,
        }
    }
    #[allow(dead_code)]
    pub fn build(&self) -> Vec<Phenopacket> {
        self.subject_to_phenopacket.values().cloned().collect()
    }
    #[allow(dead_code)]
    pub fn build_for_id(&self, #[allow(unused)] id: String) -> Result<Phenopacket, TransformError> {
        Ok(Phenopacket::default())
    }

    #[allow(dead_code)]
    pub fn upset_individual(&mut self) -> Result<(), anyhow::Error> {
        todo!()
    }

    /// Inserts or updates the `id` field of a `Phenopacket` for the given individual.
    ///
    /// If a `Phenopacket` for `individual_id` does not already exist in the builder,
    /// a new default one will be created. The `id` field of the `Phenopacket`
    /// will then be set to the provided `pp_id`.
    ///
    /// # Arguments
    ///
    /// * `individual_id` - A unique identifier for the individual associated with the `Phenopacket`.
    /// * `pp_id` - The identifier to assign to the `Phenopacket`'s `id` field.
    ///
    /// # Behavior
    ///
    /// Calling this method multiple times with the same `individual_id` will
    /// overwrite the existing `id` of that individual's `Phenopacket`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut builder = PhenopacketBuilder::new(hpo);
    /// builder.upsert_phenopacket_id("patient1".to_string(), "pp001".to_string());
    /// ```
    #[allow(dead_code)]
    pub fn upsert_phenopacket_id(&mut self, individual_id: String, pp_id: String) {
        let phenopacket = self
            .subject_to_phenopacket
            .entry(individual_id.clone())
            .or_default();

        phenopacket.id = pp_id.to_string();
    }

    #[allow(dead_code)]
    pub fn upset_phenotypic_feature(
        &mut self,
        #[allow(unused)] subject_id: String,
        #[allow(unused)] phenotype: String,
        #[allow(unused)] on_set: Option<String>,
        #[allow(unused)] is_observed: Option<bool>,
    ) -> Result<(), anyhow::Error> {
        todo!()
    }

    //Add further add and update functions here.....
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use crate::ontology::utils::init_ontolius;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create shared temporary directory")
    }

    #[rstest]
    fn test_upsert_new_individual(temp_dir: TempDir) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_upsert_new_individual");
            return;
        }

        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(temp_dir.path().to_path_buf());
        let path = hpo_registry.register("latest").unwrap();
        let hpo = init_ontolius(path).unwrap();

        let mut s = PhenopacketBuilder {
            subject_to_phenopacket: HashMap::new(),
            hpo,
        };

        s.upsert_phenopacket_id("ind1".to_string(), "pp1".to_string());

        assert_eq!(
            s.subject_to_phenopacket.get("ind1").unwrap().id,
            "ind1_pp1".to_string()
        );
    }

    #[rstest]
    fn test_upsert_existing_individual_overwrites(temp_dir: TempDir) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_upsert_existing_individual_overwrites");
            return;
        }

        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(temp_dir.path().to_path_buf());
        let path = hpo_registry.register("latest").unwrap();
        let hpo = init_ontolius(path).unwrap();

        let mut s = PhenopacketBuilder {
            subject_to_phenopacket: HashMap::new(),
            hpo,
        };

        let pp = Phenopacket {
            id: "old_id".to_string(),
            ..Default::default()
        };

        s.subject_to_phenopacket.insert("ind1".to_string(), pp);

        s.upsert_phenopacket_id("ind1".to_string(), "pp2".to_string());

        assert_eq!(
            s.subject_to_phenopacket.get("ind1").unwrap().id,
            "ind1_pp2".to_string()
        );
    }
}
