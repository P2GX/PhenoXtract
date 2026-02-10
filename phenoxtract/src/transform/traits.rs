use phenopackets::schema::v2::core::{
    Disease, Individual, Interpretation, Measurement, PhenotypicFeature, Resource,
};

pub(crate) trait PhenopacketAccessors {
    fn get_or_create_individual_mut(&mut self) -> &mut Individual;
    fn resources(&self) -> &[Resource];
    fn push_resource(&mut self, resource: Resource);
    fn find_interpretation_mut(&mut self, id: &str) -> Option<&mut Interpretation>;
    fn push_interpretation(&mut self, interpretation: Interpretation);

    fn phenotypes_with_id(&self, id: &str) -> Vec<&PhenotypicFeature>;
    fn first_phenotype_with_id_mut(&mut self, id: &str) -> Option<&mut PhenotypicFeature>;
    fn push_phenotype(&mut self, phenotypes: PhenotypicFeature);
    fn push_measurement(&mut self, measurement: Measurement);

    fn push_disease(&mut self, disease: Disease);
}
