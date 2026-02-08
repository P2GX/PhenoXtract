use crate::transform::traits::PhenopacketAccessors;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::{
    Disease, Individual, Interpretation, Measurement, PhenotypicFeature, Resource,
};

impl PhenopacketAccessors for Phenopacket {
    fn get_or_create_individual_mut(&mut self) -> &mut Individual {
        self.subject.get_or_insert(Individual::default())
    }
    fn resources(&self) -> &[Resource] {
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

    fn find_interpretation_mut(&mut self, id: &str) -> Option<&mut Interpretation> {
        let index = self.interpretations.iter().position(|inter| inter.id == id);

        self.interpretations.get_mut(index?)
    }

    fn push_interpretation(&mut self, interpretation: Interpretation) {
        self.interpretations.push(interpretation)
    }

    fn phenotypes_with_type_id(&self, id: &str) -> Vec<&PhenotypicFeature> {
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

    fn first_phenotype_with_type_id_mut(&mut self, id: &str) -> Option<&mut PhenotypicFeature> {
        self.phenotypic_features.iter_mut().find(|feature| {
            if let Some(t) = &feature.r#type {
                t.id == id
            } else {
                false
            }
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_suite::phenopacket_component_generation::{
        default_disease_oc, default_phenotype,
    };
    use crate::test_suite::resource_references::hp_meta_data_resource;

    #[test]
    fn test_get_or_create_individual_mut() {
        let mut pp = Phenopacket::default();

        {
            let subject = pp.get_or_create_individual_mut();
            subject.id = "patient-1".to_string();
        }

        assert_eq!(pp.subject.as_ref().unwrap().id, "patient-1");

        {
            let subject = pp.get_or_create_individual_mut();
            assert_eq!(subject.id, "patient-1");
            subject.id = "patient-updated".to_string();
        }

        assert_eq!(pp.subject.unwrap().id, "patient-updated");
    }

    #[test]
    fn test_resource_management() {
        let mut pp = Phenopacket::default();
        let resource = hp_meta_data_resource();

        pp.push_resource(resource);

        assert_eq!(pp.resources().len(), 1);
        assert_eq!(pp.resources()[0].id, "hp");
    }

    #[test]
    fn test_find_interpretation_mut() {
        let mut pp = Phenopacket::default();
        let inter = Interpretation {
            id: "inter-123".to_string(),
            ..Default::default()
        };
        pp.push_interpretation(inter);

        let found = pp.find_interpretation_mut("inter-123");
        assert!(found.is_some());
        found.unwrap().id = "inter-modified".to_string();

        assert!(pp.find_interpretation_mut("missing").is_none());
        assert_eq!(pp.interpretations[0].id, "inter-modified");
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_phenotypes_with_type_id() {
        let mut pp = Phenopacket::default();
        let feature = default_phenotype();

        pp.push_phenotype(feature.clone());
        pp.push_phenotype(PhenotypicFeature::default());

        let results = pp.phenotypes_with_type_id(&feature.r#type.clone().unwrap().id);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].r#type.as_ref().unwrap().id,
            feature.r#type.unwrap().id
        );
    }

    #[test]
    fn test_push_methods() {
        let mut pp = Phenopacket::default();

        pp.push_disease(Disease {
            term: Some(default_disease_oc()),
            ..Default::default()
        });
        pp.push_measurement(Measurement::default());

        assert_eq!(pp.diseases.len(), 1);
        assert_eq!(pp.measurements.len(), 1);
    }
}
