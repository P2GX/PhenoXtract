use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::traits::Strategy;
use phenopackets::schema::v2::Phenopacket;

#[allow(dead_code)]
pub struct TransformerModule {
    strategies: Vec<Box<dyn Strategy>>,
    phenopacket_builder: PhenopacketBuilder,
}

impl TransformerModule {
    #[allow(dead_code)]
    pub fn run(
        &self,
        tables: &mut [ContextualizedDataFrame],
    ) -> Result<Vec<Phenopacket>, TransformError> {
        tables.iter_mut().for_each(|table| {
            for strategy in &self.strategies {
                if let Err(_e) = strategy.transform(table) {
                    //TODO: Log error here.
                    continue;
                };
            }
        });
        // The tables should now be in the correct format.
        // Iterate through them to doing a groupby("subject_id") for each and collecting
        // the relevant values per subject with the phenopacket builder.
        let packets = self.phenopacket_builder.build()?;
        Ok(packets)
    }

    pub(crate) fn new(
        strategies: Vec<Box<dyn Strategy>>,
        phenopacket_builder: PhenopacketBuilder,
    ) -> Self {
        TransformerModule {
            strategies,
            phenopacket_builder,
        }
    }
}
