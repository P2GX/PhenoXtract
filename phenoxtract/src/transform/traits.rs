#![allow(clippy::too_many_arguments)]
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
use phenopackets::schema::v2::Phenopacket;

pub trait PhenopacketBuilding {
    fn build(&self) -> Vec<Phenopacket>;

    fn upsert_individual(
        &mut self,
        patient_id: &str,
        alternate_ids: Option<&[&str]>,
        date_of_birth: Option<&str>,
        time_at_last_encounter: Option<&str>,
        sex: Option<&str>,
        karyotypic_sex: Option<&str>,
        gender: Option<&str>,
        taxonomy: Option<&str>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn upsert_vital_status(
        &mut self,
        patient_id: &str,
        status: &str,
        time_of_death: Option<&str>,
        cause_of_death: Option<&str>,
        survival_time_in_days: Option<u32>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn upsert_phenotypic_feature(
        &mut self,
        patient_id: &str,
        phenotype: &str,
        description: Option<&str>,
        excluded: Option<bool>,
        severity: Option<&str>,
        modifiers: Option<Vec<&str>>,
        onset: Option<&str>,
        resolution: Option<&str>,
        evidence: Option<&str>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn upsert_interpretation(
        &mut self,
        patient_id: &str,
        disease: &str,
        gene_variant_data: &PathogenicGeneVariantData,
        subject_sex: Option<String>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_disease(
        &mut self,
        patient_id: &str,
        disease: &str,
        excluded: Option<bool>,
        onset: Option<&str>,
        resolution: Option<&str>,
        disease_stage: Option<&[&str]>,
        clinical_tnm_finding: Option<&[&str]>,
        primary_site: Option<&str>,
        laterality: Option<&str>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_quantitative_measurement(
        &mut self,
        patient_id: &str,
        quant_measurement: f64,
        time_observed: Option<&str>,
        assay_id: &str,
        unit_id: &str,
        reference_range: Option<(f64, f64)>,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_qualitative_measurement(
        &mut self,
        patient_id: &str,
        qual_measurement: &str,
        time_observed: Option<&str>,
        assay_id: &str,
    ) -> Result<(), PhenopacketBuilderError>;

    fn insert_medical_procedure(
        &mut self,
        patient_id: &str,
        procedure_code: &str,
        body_part: Option<&str>,
        procedure_time_element: Option<&str>,
        treatment_target: Option<&str>,
        treatment_intent: Option<&str>,
        response_to_treatment: Option<&str>,
        treatment_termination_reason: Option<&str>,
    ) -> Result<(), PhenopacketBuilderError>;
}
