use crate::transform::traits::PhenopacketAccessors;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::{
    Disease, Individual, Interpretation, Measurement, PhenotypicFeature, Resource, VitalStatus,
};

impl PhenopacketAccessors for Phenopacket {
    fn get_mut_individual(&mut self) -> &mut Individual {
        self.subject.get_or_insert(Individual::default())
    }
    fn set_vital_status(&mut self, status: VitalStatus) -> bool {
        if let Some(subject) = &mut self.subject {
            subject.vital_status = Some(status);
            true
        } else {
            false
        }
    }
    fn get_resources(&self) -> &[Resource] {
        if let Some(meta_data) = &self.meta_data {
            meta_data.resources.as_slice()
        } else {
            &[]
        }
    }

    fn push_resource(&mut self, resource: Resource) {
        self.meta_data
            .get_or_insert_with(Default::default)
            .resources
            .push(resource)
    }

    fn get_interpretation(&self, id: &str) -> Option<&Interpretation> {
        let index = self.interpretations.iter().position(|inter| inter.id == id);

        self.interpretations.get(index?)
    }

    fn get_mut_interpretation(&mut self, id: &str) -> Option<&mut Interpretation> {
        let index = self.interpretations.iter().position(|inter| inter.id == id);

        self.interpretations.get_mut(index?)
    }

    fn push_interpretation(&mut self, interpretation: Interpretation) {
        self.interpretations.push(interpretation)
    }

    fn get_phenotypes_by_id(&self, id: &str) -> Vec<&PhenotypicFeature> {
        self.phenotypic_features
            .iter()
            .filter(|feature| {
                if let Some(t) = &feature.r#type {
                    t.id == id
                } else {
                    false
                }
            })
            .collect::<Vec<&PhenotypicFeature>>()
    }

    fn get_mut_phenotypes_by_id(&mut self, id: &str) -> Vec<&mut PhenotypicFeature> {
        self.phenotypic_features
            .iter_mut()
            .filter(|feature| {
                if let Some(t) = &feature.r#type {
                    t.id == id
                } else {
                    false
                }
            })
            .collect::<Vec<&mut PhenotypicFeature>>()
    }

    fn push_phenotype(&mut self, phenotypes: PhenotypicFeature) {
        self.phenotypic_features.push(phenotypes)
    }

    fn push_measurement(&mut self, measurement: Measurement) {
        self.measurements.push(measurement)
    }

    fn push_disease(&mut self, disease: Disease) {
        self.diseases.push(disease)
    }
}
