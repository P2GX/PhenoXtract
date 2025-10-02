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
        mut data: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, TransformError> {
        let mut tables_refs = data
            .iter_mut()
            .collect::<Vec<&mut ContextualizedDataFrame>>();

        for strategy in &self.strategies {
            strategy.transform(tables_refs.as_mut_slice())?;
        }

        self.collector.collect(data)
    }

    pub fn new(strategies: Vec<Box<dyn Strategy>>, collector: Collector) -> Self {
        TransformerModule {
            strategies,
            collector,
        }
    }
}
