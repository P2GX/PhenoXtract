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
