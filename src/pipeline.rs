use crate::config::pipeline_config::PipelineConfig;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::extractable::Extractable;
use crate::load::loader_module::Loadable;
use crate::transform::transform_module::TransformerModule;

struct Pipeline {
    transformer_module: TransformerModule,
    loader_module: Box<dyn Loadable>,
}

impl Pipeline {
    fn run_etl(&self, extractables: &mut [impl Extractable]) -> Result<(), anyhow::Error> {
        let mut data: Vec<ContextualizedDataFrame> = extractables
            .iter()
            .flat_map(|ex| ex.extract().unwrap())
            .collect();

        self.run_tl(&mut data)?;
        Ok(())
        // Use the extract function of the Extractable Trait to get tables
        // Next call the run_tl function.
    }

    fn run_tl(&self, tables: &mut [ContextualizedDataFrame]) -> Result<(), anyhow::Error> {
        let phenopackets = self.transformer_module.run(tables)?;

        for phenopacket in phenopackets {
            if let Err(e) = self.loader_module.load(&phenopacket) {
                // TODO: Replace print with logging later
                println!(
                    "Could not save Phenopacket for subject: {}. Error: {:?}",
                    phenopacket.subject_id.as_str(),
                    e
                )
            }
        }

        // Push the Tables through the transformer_module.transform function
        // You should get a vec of phenopackets from the transformer. Use the loader module to store them.
        Ok(())
    }

    fn from_config(config: &PipelineConfig) -> Result<Self, anyhow::Error> {
        // Uses the PipelineConfig object and constructs the pipeline from it
        todo!()
    }
}
