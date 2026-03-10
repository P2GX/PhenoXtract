use crate::config::MetaData;
use crate::ontology::traits::BiDict;
use crate::transform::bidict_library::BiDictLibrary;
use pivotal::hgnc::HGNCData;
use pivotal::hgvs::HGVSData;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct BuilderMetaData {
    cohort_name: String,
    created_by: String,
    submitted_by: String,
}

impl BuilderMetaData {
    pub fn new(
        cohort_name: impl Into<String>,
        created_by: impl Into<String>,
        submitted_by: impl Into<String>,
    ) -> BuilderMetaData {
        Self {
            cohort_name: cohort_name.into(),
            created_by: created_by.into(),
            submitted_by: submitted_by.into(),
        }
    }

    pub fn cohort_name(&self) -> &str {
        &self.cohort_name
    }
    pub fn created_by(&self) -> &str {
        &self.created_by
    }
    pub fn submitted_by(&self) -> &str {
        &self.submitted_by
    }
}

impl From<MetaData> for BuilderMetaData {
    fn from(config_meta_data: MetaData) -> Self {
        Self {
            cohort_name: config_meta_data.cohort_name,
            created_by: config_meta_data.created_by,
            submitted_by: config_meta_data.submitted_by,
        }
    }
}

#[derive(Debug)]
pub struct TransformContext {
    meta_data: BuilderMetaData,
    hgnc_client: Arc<dyn HGNCData>,
    hgvs_client: Arc<dyn HGVSData>,
    hpo_bidict_lib: Arc<BiDictLibrary>,
    disease_bidict_lib: Arc<BiDictLibrary>,
    unit_bidict_lib: Arc<BiDictLibrary>,
    assay_bidict_lib: Arc<BiDictLibrary>,
    qualitative_measurement_bidict_lib: Arc<BiDictLibrary>,
    procedure_bi_dict_lib: Arc<BiDictLibrary>,
    anatomy_bi_dict_lib: Arc<BiDictLibrary>,
    treatment_attributes_bi_dict: Arc<BiDictLibrary>,
}

impl PartialEq for TransformContext {
    fn eq(&self, other: &Self) -> bool {
        self.meta_data == other.meta_data
            && self.hpo_bidict_lib == other.hpo_bidict_lib
            && self.disease_bidict_lib == other.disease_bidict_lib
            && self.unit_bidict_lib == other.unit_bidict_lib
            && self.assay_bidict_lib == other.assay_bidict_lib
            && self.qualitative_measurement_bidict_lib == other.qualitative_measurement_bidict_lib
            && self.procedure_bi_dict_lib == other.procedure_bi_dict_lib
            && self.anatomy_bi_dict_lib == other.anatomy_bi_dict_lib
            && self.treatment_attributes_bi_dict == other.treatment_attributes_bi_dict
    }
}

impl TransformContext {
    pub fn builder(
        meta_data: BuilderMetaData,
        hgnc_client: Arc<dyn HGNCData>,
        hgvs_client: Arc<dyn HGVSData>,
    ) -> TransformContextBuilder {
        TransformContextBuilder::new(meta_data, hgnc_client, hgvs_client)
    }

    pub fn meta_data(&self) -> &BuilderMetaData {
        &self.meta_data
    }

    pub fn hpo_bidict_lib(&self) -> &Arc<BiDictLibrary> {
        &self.hpo_bidict_lib
    }

    pub fn hgnc_client(&self) -> &Arc<dyn HGNCData> {
        &self.hgnc_client
    }

    pub fn hgvs_client(&self) -> &Arc<dyn HGVSData> {
        &self.hgvs_client
    }

    pub fn disease_bidict_lib(&self) -> &Arc<BiDictLibrary> {
        &self.disease_bidict_lib
    }

    pub fn unit_bidict_lib(&self) -> &Arc<BiDictLibrary> {
        &self.unit_bidict_lib
    }

    pub fn assay_bidict_lib(&self) -> &Arc<BiDictLibrary> {
        &self.assay_bidict_lib
    }

    pub fn qualitative_measurement_bidict_lib(&self) -> &Arc<BiDictLibrary> {
        &self.qualitative_measurement_bidict_lib
    }

    pub fn procedure_bi_dict_lib(&self) -> &Arc<BiDictLibrary> {
        &self.procedure_bi_dict_lib
    }

    pub fn anatomy_bi_dict_lib(&self) -> &Arc<BiDictLibrary> {
        &self.anatomy_bi_dict_lib
    }

    pub fn treatment_attributes_bi_dict(&self) -> &Arc<BiDictLibrary> {
        &self.treatment_attributes_bi_dict
    }
}

pub struct TransformContextBuilder {
    meta_data: BuilderMetaData,
    hpo_bidict_lib: BiDictLibrary,
    hgnc_client: Arc<dyn HGNCData>,
    hgvs_client: Arc<dyn HGVSData>,
    disease_bidict_lib: BiDictLibrary,
    unit_bidict_lib: BiDictLibrary,
    assay_bidict_lib: BiDictLibrary,
    qualitative_measurement_bidict_lib: BiDictLibrary,
    procedure_bi_dict_lib: BiDictLibrary,
    anatomy_bi_dict_lib: BiDictLibrary,
    treatment_attributes_bi_dict: BiDictLibrary,
}

impl TransformContextBuilder {
    pub fn new(
        meta_data: BuilderMetaData,
        hgnc_client: Arc<dyn HGNCData>,
        hgvs_client: Arc<dyn HGVSData>,
    ) -> Self {
        Self {
            meta_data,
            hpo_bidict_lib: BiDictLibrary::empty_with_name("HPO"),
            hgnc_client,
            hgvs_client,
            disease_bidict_lib: BiDictLibrary::empty_with_name("DISEASE"),
            unit_bidict_lib: BiDictLibrary::empty_with_name("UNIT"),
            assay_bidict_lib: BiDictLibrary::empty_with_name("ASSY"),
            qualitative_measurement_bidict_lib: BiDictLibrary::empty_with_name("QUANTITY"),
            procedure_bi_dict_lib: BiDictLibrary::empty_with_name("PROCEDURE"),
            anatomy_bi_dict_lib: BiDictLibrary::empty_with_name("ANATOMY"),
            treatment_attributes_bi_dict: BiDictLibrary::empty_with_name("TREATMENT"),
        }
    }

    pub fn hgnc_client(mut self, client: Arc<dyn HGNCData>) -> Self {
        self.hgnc_client = client;
        self
    }

    pub fn hgvs_client(mut self, client: Arc<dyn HGVSData>) -> Self {
        self.hgvs_client = client;
        self
    }

    pub fn add_hpo_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.hpo_bidict_lib.add_bidict(bidict);
    }

    pub fn add_disease_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.disease_bidict_lib.add_bidict(bidict);
    }

    pub fn add_unit_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.unit_bidict_lib.add_bidict(bidict);
    }

    pub fn add_assay_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.assay_bidict_lib.add_bidict(bidict);
    }

    pub fn add_qualitative_measurement_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.qualitative_measurement_bidict_lib.add_bidict(bidict);
    }

    pub fn add_procedure_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.procedure_bi_dict_lib.add_bidict(bidict);
    }

    pub fn add_anatomy_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.anatomy_bi_dict_lib.add_bidict(bidict);
    }

    pub fn add_treatment_attributes_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.treatment_attributes_bi_dict.add_bidict(bidict);
    }

    pub fn build(self) -> TransformContext {
        TransformContext {
            meta_data: self.meta_data,
            hpo_bidict_lib: Arc::new(self.hpo_bidict_lib),
            hgnc_client: self.hgnc_client,
            hgvs_client: self.hgvs_client,
            disease_bidict_lib: Arc::new(self.disease_bidict_lib),
            unit_bidict_lib: Arc::new(self.unit_bidict_lib),
            assay_bidict_lib: Arc::new(self.assay_bidict_lib),
            qualitative_measurement_bidict_lib: Arc::new(self.qualitative_measurement_bidict_lib),
            procedure_bi_dict_lib: Arc::new(self.procedure_bi_dict_lib),
            anatomy_bi_dict_lib: Arc::new(self.anatomy_bi_dict_lib),
            treatment_attributes_bi_dict: Arc::new(self.treatment_attributes_bi_dict),
        }
    }
}
