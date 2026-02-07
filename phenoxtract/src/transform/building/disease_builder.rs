use crate::transform::building::ppb::PhenopacketBuilder;

pub struct DiseaseBuilder<'a> {
    pp_builder: &'a mut PhenopacketBuilder,
    patient_id: &'a str,
    disease: &'a str,
    excluded: bool,
    onset: Option<&'a str>,
    resolution: Option<&'a str>,
    disease_stage: Vec<&'a str>,
    clinical_tnm_finding: Vec<&'a str>,
    primary_site: Option<&'a str>,
    laterality: Option<&'a str>,
}

impl<'a> DiseaseBuilder<'a> {
    pub fn new(
        pp_builder: &'a mut PhenopacketBuilder,
        patient_id: &'a str,
        disease: &'a str,
    ) -> Self {
        Self {
            pp_builder,
            patient_id,
            disease,
            excluded: false,
            onset: None,
            resolution: None,
            disease_stage: vec![],
            clinical_tnm_finding: vec![],
            primary_site: None,
            laterality: None,
        }
    }

    pub fn excluded(mut self) -> Self {
        self.excluded = true;
        self
    }
    pub fn onset(mut self, onset: &'a str) -> Self {
        self.onset = Some(onset);
        self
    }
    pub fn resolution(mut self, resolution: &'a str) -> Self {
        self.resolution = Some(resolution);
        self
    }

    pub fn disease_stage(mut self, disease_stage: Vec<&'a str>) -> Self {
        self.disease_stage = disease_stage;
        self
    }

    pub fn clinical_tnm_finding(mut self, clinical_tnm_finding: Vec<&'a str>) -> Self {
        self.clinical_tnm_finding = clinical_tnm_finding;
        self
    }

    pub fn primary_site(mut self, primary_site: &'a str) -> Self {
        self.primary_site = Some(primary_site);
        self
    }

    pub fn laterality(mut self, laterality: &'a str) -> Self {
        self.laterality = Some(laterality);
        self
    }
}
