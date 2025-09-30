use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::collector::Collector;
use crate::transform::error::TransformError;
use crate::transform::traits::Strategy;
use phenopackets::schema::v2::Phenopacket;

#[allow(dead_code)]
#[derive(Debug)]
pub struct TransformerModule {
    strategies: Vec<Box<dyn Strategy>>,
    collector: Collector,
}

impl TransformerModule {
    #[allow(dead_code)]
    pub fn run(
        &mut self,
        tables: &mut [ContextualizedDataFrame],
    ) -> Result<Vec<Phenopacket>, TransformError> {
        for table in tables.iter_mut() {
            for strategy in &self.strategies {
                strategy.transform(table)?;
            }
        }

        self.collector.collect(tables)
    }

    pub fn new(strategies: Vec<Box<dyn Strategy>>, collector: Collector) -> Self {
        TransformerModule {
            strategies,
            collector,
        }
    }
}
