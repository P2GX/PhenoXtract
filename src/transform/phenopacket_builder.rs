use crate::transform::error::TransformError;
use phenopackets::schema::v2::Phenopacket;
use std::collections::HashMap;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
}

impl PhenopacketBuilder {
    #[allow(dead_code)]
    pub fn build(&self) -> Result<Vec<Phenopacket>, TransformError> {
        Ok(Vec::new())
    }
    #[allow(dead_code)]
    pub fn build_for_id(&self, #[allow(unused)] id: String) -> Result<Phenopacket, TransformError> {
        Ok(Phenopacket::default())
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
