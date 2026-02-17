use crate::extract::ContextualizedDataFrame;
use crate::ontology::CachedOntologyFactory;
use crate::test_suite::utils::test_ontology_path;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
use crate::transform::traits::PhenopacketBuilding;
use mockall::mock;
use mockall::predicate::*;
use once_cell::sync::Lazy;
use ontology_registry::enums::{FileType, Version};
use ontology_registry::error::OntologyRegistryError;
use ontology_registry::traits::OntologyRegistration;
use phenopackets::schema::v2::Phenopacket;
use std::any::Any;
use std::fmt::Debug;
use std::fs;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

mock! {
    pub(crate) Collector {}

    impl Collect for Collector {
        fn collect(
            &self,
            builder: &mut dyn PhenopacketBuilding,
            patient_cdfs: &[ContextualizedDataFrame],
            phenopacket_id: &str,
        ) -> Result<(), CollectorError>;

        fn as_any(&self) -> &dyn Any;
    }

    impl Debug for Collector {
        fn fmt<'a>(&self, f: &mut std::fmt::Formatter<'a>) -> std::fmt::Result;
    }
}

mock! {
pub(crate) PhenopacketBuilding {}
impl PhenopacketBuilding for PhenopacketBuilding {
    fn build(&self) -> Vec<Phenopacket>;

    fn upsert_individual<'a>(
        &mut self,
        patient_id: &'a str,
        alternate_ids: Option<&'a [&'a str]>,
        date_of_birth: Option<&'a str>,
        time_at_last_encounter: Option<&'a str>,
        sex: Option<&'a str>,
        karyotypic_sex: Option<&'a str>,
        gender: Option<&'a str>,
        taxonomy: Option<&'a str>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn upsert_vital_status<'a>(
        &mut self,
        patient_id: &'a str,
        status: &'a str,
        time_of_death: Option<&'a str>,
        cause_of_death: Option<&'a str>,
        survival_time_in_days: Option<u32>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn upsert_phenotypic_feature<'a>(
        &mut self,
        patient_id: &'a str,
        phenotype: &'a str,
        description: Option<&'a str>,
        excluded: Option<bool>,
        severity: Option<&'a str>,
        modifiers: Option<Vec<&'a str>>,
        onset: Option<&'a str>,
        resolution: Option<&'a str>,
        evidence: Option<&'a str>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn upsert_interpretation<'a>(
        &mut self,
        patient_id: &'a str,
        disease: &'a str,
        gene_variant_data: &'a PathogenicGeneVariantData,
        subject_sex: Option<String>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_disease<'a>(
        &mut self,
        patient_id: &'a str,
        disease: &'a str,
        excluded: Option<bool>,
        onset: Option<&'a str>,
        resolution: Option<&'a str>,
        disease_stage: Option<&'a [&'a str]>,
        clinical_tnm_finding: Option<&'a [&'a str]>,
        primary_site: Option<&'a str>,
        laterality: Option<&'a str>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_quantitative_measurement<'a>(
        &mut self,
        patient_id: &'a str,
        quant_measurement: f64,
        time_observed: Option<&'a str>,
        assay_id: &'a str,
        unit_id: &'a str,
        reference_range: Option<(f64, f64)>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_qualitative_measurement<'a>(
        &mut self,
        patient_id: &'a str,
        qual_measurement: &'a str,
        time_observed: Option<&'a str>,
        assay_id: &'a str,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_medical_procedure<'a>(
        &mut self,
        patient_id: &'a str,
        procedure_code: &'a str,
        body_part: Option<&'a str>,
        procedure_time_element: Option<&'a str>,
        treatment_target: Option<&'a str>,
        treatment_intent: Option<&'a str>,
        response_to_treatment: Option<&'a str>,
        treatment_termination_reason: Option<&'a str>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_medical_treatment<'a>(
    &mut self,
    patient_id: &str,
    agent: &str,
    route_of_administration: Option<&'a str>,
    dose_intervals: Vec<usize>, // TODO
    drug_type: Option<&'a str>,
    unit: Option<&'a str>,
    value: Option<f64>,
    reference_range: Option<(f64, f64)>,
    treatment_target: Option<&'a str>,
    treatment_intent: Option<&'a str>,
    response_to_treatment: Option<&'a str>,
    treatment_termination_reason: Option<&'a str>,
) -> Result<(), PhenopacketBuilderError>;

}
}

pub(crate) static ONTOLOGY_FACTORY: Lazy<Arc<Mutex<CachedOntologyFactory<MockOntologyRegistry>>>> =
    Lazy::new(|| {
        Arc::new(Mutex::new(CachedOntologyFactory::new(
            MockOntologyRegistry::default(),
        )))
    });

#[derive(Debug)]
pub(crate) struct MockOntologyRegistry {
    registry_path: PathBuf,
}

impl Default for MockOntologyRegistry {
    fn default() -> Self {
        Self {
            registry_path: test_ontology_path(),
        }
    }
}

impl OntologyRegistration for MockOntologyRegistry {
    fn register(
        &self,
        ontology_id: &str,
        version: Version,
        file_type: FileType,
    ) -> Result<impl Read, OntologyRegistryError> {
        if version.to_string() == Version::Latest.to_string() {
            let entries =
                fs::read_dir(self.registry_path.clone()).expect("Failed to read registry path");

            for entry in entries {
                let entry = entry.expect("Failed to read entry");
                let path = entry.path();
                let file_name = path
                    .file_name()
                    .expect("No, filename")
                    .to_str()
                    .expect("Conversion error");

                let found_ontology_id = file_name.split("_").next().unwrap().to_string();
                if found_ontology_id == ontology_id {
                    return Ok(fs::File::open(&path).unwrap_or_else(|_| {
                        panic!("Failed to open file {}", path.to_str().unwrap())
                    }));
                }
            }
        }

        let file_name = format!("{ontology_id}_{version}{}", file_type.as_file_ending());
        let file_path = self.registry_path.join(file_name);

        if !file_path.exists() {
            return Err(OntologyRegistryError::UnableToRegister {
                reason: format!(
                    "Ontology not found at {}, when mocking OntologyRegistry",
                    file_path.to_str().unwrap()
                ),
            });
        }

        Ok(fs::File::open(&file_path)
            .unwrap_or_else(|_| panic!("Failed to open file {}", file_path.to_str().unwrap())))
    }

    #[allow(unused)]
    fn unregister(
        &self,
        ontology_id: &str,
        version: Version,
        file_type: FileType,
    ) -> Result<(), OntologyRegistryError> {
        todo!()
    }

    #[allow(unused)]
    fn get(&self, ontology_id: &str, version: Version, file_type: FileType) -> Option<impl Read> {
        panic!("Mock ontology factory get is not implemented yet");
        None::<Cursor<Vec<u8>>>
    }
    #[allow(unused)]
    fn list(&self) -> Vec<String> {
        todo!()
    }
}
