use crate::transform::phenopacket::Phenopacket;
use std::collections::HashMap;

#[allow(dead_code)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<usize, Phenopacket>,
}

impl PhenopacketBuilder {
    #[allow(dead_code)]
    pub fn build(&self) -> Result<Vec<Phenopacket>, anyhow::Error> {
        Ok(Vec::new())
    }
    #[allow(dead_code)]
    pub fn build_for_id(
        &self,
        // Rename input withoug _, when implementing
        _id: usize,
    ) -> Result<Phenopacket, anyhow::Error> {
        Ok(Phenopacket::new("Magnus Knut Hansen".to_string()))
    }

    #[allow(dead_code)]
    pub fn add_phenotypic_feature(
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
