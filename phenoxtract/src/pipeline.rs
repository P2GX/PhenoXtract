use crate::error::PipelineError;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;
use crate::load::traits::Loadable;

use crate::transform::strategies::traits::Strategy;
use crate::transform::transform_module::TransformerModule;
use log::info;
use phenopackets::schema::v2::Phenopacket;
use validator::Validate;

#[derive(Debug)]
pub struct Pipeline {
    pub(crate) transformer_module: TransformerModule,
    pub(crate) loader_module: Box<dyn Loadable>,
}

impl Pipeline {
    pub fn new(
        transformer_module: TransformerModule,
        loader_module: Box<dyn Loadable>,
    ) -> Pipeline {
        Pipeline {
            transformer_module,
            loader_module,
        }
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy>) {
        self.transformer_module.add_strategy(strategy);
    }
    pub fn insert_strategy(&mut self, idx: usize, strategy: Box<dyn Strategy>) {
        self.transformer_module.insert_strategy(idx, strategy);
    }

    pub fn run(
        &mut self,
        extractables: &mut [impl Extractable + Validate],
    ) -> Result<(), PipelineError> {
        let data = self.extract(extractables)?;
        let phenopackets = self.transform(data)?;
        self.load(phenopackets.as_slice())?;
        Ok(())
    }

    pub fn extract(
        &self,
        extractables: &mut [impl Extractable + Validate],
    ) -> Result<Vec<ContextualizedDataFrame>, PipelineError> {
        info!("Starting extract");
        extractables.validate()?;

        let tables: Vec<ContextualizedDataFrame> = extractables
            .iter()
            .map(|ex| ex.extract())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        info!("Concluded extraction extracted {:?} tables", tables.len());
        Ok(tables)
    }

    pub fn transform(
        &mut self,
        data: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, PipelineError> {
        info!("Starting Transformation");
        data.iter().try_for_each(|t| t.validate())?;

        let phenopackets = self.transformer_module.run(data)?;
        info!(
            "Concluded Transformation. Found {:?} Phenopackets",
            phenopackets.len()
        );
        Ok(phenopackets)
    }

    pub fn load(&self, phenopackets: &[Phenopacket]) -> Result<(), PipelineError> {
        self.loader_module.load(phenopackets)?;

        info!("Concluded Loading");
        Ok(())
    }
}

impl TryFrom<PipelineConfig> for Pipeline {
    type Error = ConstructionError;

    fn try_from(config: PipelineConfig) -> Result<Self, Self::Error> {
        let ontology_registry_dir = get_cache_dir()?.join("ontology_registry");

        if !ontology_registry_dir.exists() {
            fs::create_dir_all(&ontology_registry_dir)?;
        }

        let mut resource_factory = ResourceConfigFactory::default();

        let mut hpo_bidict_library = BiDictLibrary::empty_with_name("HPO");
        let mut disease_bidict_library = BiDictLibrary::empty_with_name("DISEASE");
        let mut assay_bidict_library = BiDictLibrary::empty_with_name("ASSAY");
        let mut unit_bidict_library = BiDictLibrary::empty_with_name("UNIT");
        let mut qualitative_measurement_bidict_library = BiDictLibrary::empty_with_name("QUAL");

        if let Some(hp_resource) = &config.meta_data.hp_resource {
            let hpo_bidict = resource_factory.build(hp_resource)?;
            hpo_bidict_library.add_bidict(hpo_bidict);
        };

        for disease_resource in &config.meta_data.disease_resources {
            let disease_bidict = resource_factory.build(disease_resource)?;
            disease_bidict_library.add_bidict(disease_bidict);
        }

        for assay_resource in &config.meta_data.assay_resources {
            let assay_bidict = resource_factory.build(assay_resource)?;
            assay_bidict_library.add_bidict(assay_bidict);
        }

        for unit_ontology_ref in &config.meta_data.unit_resources {
            let unit_bidict = resource_factory.build(unit_ontology_ref)?;
            unit_bidict_library.add_bidict(unit_bidict);
        }

        for qualitative_measurement_ontology_ref in
            &config.meta_data.qualitative_measurement_resources
        {
            let qual_bidict = resource_factory.build(qualitative_measurement_ontology_ref)?;
            qualitative_measurement_bidict_library.add_bidict(qual_bidict);
        }

        let mut strategy_factory = StrategyFactory::new(resource_factory.into_ontology_factory());
        let phenopacket_builder = PhenopacketBuilder::new(
            config.meta_data.into(),
            Box::new(CachedHGNCClient::default()),
            Box::new(CachedHGVSClient::default()),
            hpo_bidict_library,
            disease_bidict_library,
            unit_bidict_library,
            assay_bidict_library,
            qualitative_measurement_bidict_library,
            //TODO: Add actual bi dicts
            BiDictLibrary::default(),
            BiDictLibrary::default(),
            BiDictLibrary::default(),
        );

        let strategies: Vec<Box<dyn Strategy>> = config
            .transform_strategies
            .iter()
            .map(|strat| strategy_factory.try_from_config(strat))
            .collect::<Result<Vec<_>, _>>()?;

        let tf_module = TransformerModule::new(
            strategies,
            CdfCollectorBroker::with_default_collectors(phenopacket_builder),
        );

        let loader_module = LoaderFactory::try_from_config(config.loader)?;

        Ok(Pipeline::new(tf_module, loader_module))
    }
}

>>>>>>> a6df497 (Implement insert_medical_procedure)
impl PartialEq for Pipeline {
    fn eq(&self, other: &Self) -> bool {
        self.transformer_module == other.transformer_module
            && format!("{:?}", self.loader_module) == format!("{:?}", other.loader_module)
    }
}
