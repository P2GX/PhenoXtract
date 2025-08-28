use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;

use crate::error::PipelineError;
use crate::transform::traits::Strategy;
use log::{info, warn};

#[allow(dead_code)]
struct BabyPipeline {
    strategies: Vec<Box<dyn Strategy>>,
}

impl BabyPipeline {
    #[allow(dead_code)]
    pub fn run(&self, extractables: &mut [impl Extractable]) -> Result<(), PipelineError> {
        let mut data = self.extract(extractables)?;
        dbg!(data[0].data().column("Sex").unwrap());
        dbg!(data[1].data().column("Infection").unwrap());
        self.transform(data.as_mut_slice())?;
        dbg!(data[0].data().column("Sex").unwrap());
        dbg!(data[1].data().column("Infection").unwrap());
        dbg!(data);
        Ok(())
    }

    pub fn extract(
        &self,
        extractables: &mut [impl Extractable],
    ) -> Result<Vec<ContextualizedDataFrame>, PipelineError> {
        info!("Starting extract");
        let tables: Vec<ContextualizedDataFrame> = extractables
            .iter()
            .flat_map(|ex| ex.extract().unwrap())
            .collect();
        info!("Concluded extraction extracted {:?} tables", tables.len());
        Ok(tables)
    }

    pub fn transform(&self, tables: &mut [ContextualizedDataFrame]) -> Result<(), PipelineError> {
        info!("Starting Transformation");

        tables.iter_mut().for_each(|table| {
            for strategy in &self.strategies {
                let cdf_name = table.context().name.clone();
                if let Err(_e) = strategy.transform(table) {
                    //todo how can I print the strategy name?
                    warn!["Error when applying a strategy to {cdf_name}"];
                    continue;
                };
            }
        });
        info!("Concluded Transformation.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::TableContext;
    use crate::extract::data_source::DataSource;
    use crate::extract::excel_data_source::ExcelDatasource;
    use crate::extract::extraction_config::{ExtractionConfig, PatientOrientation};
    use crate::transform::traits::StringSwap;
    use rstest::rstest;
    use std::path::PathBuf;

    #[rstest]
    fn test_baby_pipeline() {
        let test_tcs = vec![
            TableContext::new("Cohort".to_string(), vec![]),
            TableContext::new("Infections".to_string(), vec![]),
        ];

        let file_path = PathBuf::from("YOUR FILE HERE");
        //todo this is somewhat tedious at the moment - we need to initialise a different ExtractionConfig for each sheet! There should be a way to choose the same setting for all.
        let extraction_configs = vec![
            ExtractionConfig::new(
                "Cohort".to_string(),
                true,
                PatientOrientation::PatientsAreRows,
            ),
            ExtractionConfig::new(
                "Infections".to_string(),
                true,
                PatientOrientation::PatientsAreRows,
            ),
        ];
        let data_source = DataSource::Excel(ExcelDatasource::new(
            file_path,
            test_tcs.clone(),
            extraction_configs.clone(),
        ));

        let male_to_m_strategy = StringSwap {
            input_string: String::from("Male"),
            output_string: String::from("M"),
            table_column_pairs_to_transform: vec![["Cohort".to_string(), "Sex".to_string()]],
        };

        let female_to_f_strategy = StringSwap {
            input_string: String::from("Female"),
            output_string: String::from("F"),
            table_column_pairs_to_transform: vec![["Cohort".to_string(), "Sex".to_string()]],
        };

        let pneumonia_to_hpo_id_strategy = StringSwap {
            input_string: String::from("Pneumonia"),
            output_string: String::from("HP:0002090"),
            table_column_pairs_to_transform: vec![["Infections".to_string(), "Infection".to_string()]],
        };

        let baby_pipeline = BabyPipeline {
            strategies: vec![Box::new(male_to_m_strategy), Box::new(female_to_f_strategy), Box::new(pneumonia_to_hpo_id_strategy)],
        };

        baby_pipeline.run(&mut [data_source.clone()]).unwrap();
    }
}
