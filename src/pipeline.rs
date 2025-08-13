use crate::config::pipeline_config::PipelineConfig;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::extractable::Extractable;
use crate::load::loader_module::Loadable;
use crate::transform::transform_module::TransformerModule;
use phenopackets::schema::v2::Phenopacket;
#[allow(dead_code)]
struct Pipeline {
    transformer_module: TransformerModule,
    loader_module: Box<dyn Loadable>,
}

impl Pipeline {
    #[allow(dead_code)]
    pub fn run(&self, extractables: &mut [impl Extractable]) -> Result<(), anyhow::Error> {
        let mut data = self.extract(extractables)?;
        let phenopackets = self.transform(data.as_mut_slice())?;
        self.load(phenopackets.as_slice())?;
        Ok(())
    }

    pub fn extract(
        &self,
        extractables: &mut [impl Extractable],
    ) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error> {
        let data: Vec<ContextualizedDataFrame> = extractables
            .iter()
            .flat_map(|ex| ex.extract().unwrap())
            .collect();

        Ok(data)
    }

    pub fn transform(
        &self,
        tables: &mut [ContextualizedDataFrame],
    ) -> Result<Vec<Phenopacket>, anyhow::Error> {
        let phenopackets = self.transformer_module.run(tables)?;
        Ok(phenopackets)
    }

    pub fn load(&self, phenopackets: &[Phenopacket]) -> Result<(), anyhow::Error> {
        for phenopacket in phenopackets {
            if let Err(e) = self.loader_module.load(phenopacket) {
                // TODO: Replace print with logging later
                println!(
                    "Could not save Phenopacket for subject: {}. Error: {:?}",
                    phenopacket.clone().subject.unwrap().id.as_str(),
                    e
                )
            }
        }
        Ok(())
    }

    #[allow(unused)]
    #[allow(dead_code)]
    fn from_config(config: &PipelineConfig) -> Result<Self, anyhow::Error> {
        // Uses the PipelineConfig object and constructs the pipeline from it
        todo!()
    }
}
