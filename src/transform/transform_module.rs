use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use crate::transform::data_processing::preprocessor::CdfPreprocessor;
use crate::transform::error::TransformError;
use crate::transform::strategies::traits::Strategy;
use phenopackets::schema::v2::Phenopacket;

#[derive(Debug)]
pub struct TransformerModule {
    strategies: Vec<Box<dyn Strategy>>,
    broker: CdfCollectorBroker,
}

impl TransformerModule {
    pub fn new(strategies: Vec<Box<dyn Strategy>>, broker: CdfCollectorBroker) -> Self {
        TransformerModule { strategies, broker }
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy>) {
        self.strategies.push(strategy);
    }
    pub fn insert_strategy(&mut self, idx: usize, strategy: Box<dyn Strategy>) {
        self.strategies.insert(idx, strategy);
    }

    pub fn run(
        &mut self,
        mut data: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, TransformError> {
        let mut tables_refs = data
            .iter_mut()
            .collect::<Vec<&mut ContextualizedDataFrame>>();

        for table in &mut tables_refs {
            CdfPreprocessor::process(table)?
        }

        for strategy in &self.strategies {
            strategy.transform(tables_refs.as_mut_slice())?;
        }

        Ok(self.broker.process(data)?)
    }
}

impl PartialEq for TransformerModule {
    fn eq(&self, other: &Self) -> bool {
        self.broker == other.broker
            && self.strategies.len() == other.strategies.len()
            && self
                .strategies
                .iter()
                .zip(other.strategies.iter())
                .all(|(a, b)| format!("{:?}", a) == format!("{:?}", b))
    }
}
