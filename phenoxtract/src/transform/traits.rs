use phenopackets::schema::v2::core::{
    Disease, Individual, Interpretation, Measurement, PhenotypicFeature, Resource, VitalStatus,
};

pub(crate) trait PhenopacketAccessors {
    fn get_mut_individual(&mut self) -> &mut Individual;
    fn set_vital_status(&mut self, status: VitalStatus) -> bool;
    fn get_resources(&self) -> &[Resource];
    fn push_resource(&mut self, resource: Resource);
    fn get_interpretation(&self, id: &str) -> Option<&Interpretation>;
    fn get_mut_interpretation(&mut self, id: &str) -> Option<&mut Interpretation>;
    fn push_interpretation(&mut self, interpretation: Interpretation);

    fn get_phenotypes_by_id(&self, id: &str) -> Vec<&PhenotypicFeature>;
    fn get_mut_phenotypes_by_id(&mut self, id: &str) -> Vec<&mut PhenotypicFeature>;
    fn push_phenotype(&mut self, phenotypes: PhenotypicFeature);
    fn push_measurement(&mut self, measurement: Measurement);

    fn push_disease(&mut self, disease: Disease);
}
