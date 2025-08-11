use std::collections::HashMap;
use crate::transform::phenopacket::Phenopacket;

pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<usize, Phenopacket>,
}

impl PhenopacketBuilder {
    pub fn build(&self) -> Result<Vec<Phenopacket>, anyhow::Error> {
        Ok(Vec::new())
    }

    pub fn build_for_id(&self, id: usize) -> Result<Phenopacket, anyhow::Error> {
        Ok(Phenopacket::new("Magnus Knut Hansen".to_string()))
    }

    pub fn add_phenotypic_feature(
        &mut self,
        subject_id: String,
        phenotype: String,
        on_set: Option<String>,
        is_observed: Option<bool>,
    ) -> Result<(), anyhow::Error> {
        todo!()
    }

    //Add further add and update functions here.....
}