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

impl PartialEq for Pipeline {
    fn eq(&self, other: &Self) -> bool {
        self.transformer_module == other.transformer_module
            && format!("{:?}", self.loader_module) == format!("{:?}", other.loader_module)
    }
}
