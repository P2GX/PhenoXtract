use phenoxtract::Pipeline;
use phenoxtract::config::table_context::{Context, Identifier, SeriesContext, TableContext};
use phenoxtract::extract::DataSource;
use phenoxtract::extract::ExcelDatasource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::resource_references::OntologyRef;

use phenoxtract::error::PipelineError;
use phenoxtract::ontology::traits::HasPrefixId;
use phenoxtract::ontology::{CachedOntologyFactory, HGNCClient};
use phenoxtract::transform::strategies::MultiHPOColExpansionStrategy;
use phenoxtract::transform::strategies::{MappingStrategy, StringCorrectionStrategy};
use phenoxtract::transform::traits::Strategy;
use phenoxtract::transform::{Collector, PhenopacketBuilder, TransformerModule};
use rstest::{fixture, rstest};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use phenoxtract::config::PhenoXtractorConfig;

#[fixture]
fn excel_context() -> TableContext {
    TableContext::new(
        "TestData".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("patient_id".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("sex".to_string()))
                .with_data_context(Context::SubjectSex),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("gene_symbol".to_string()))
                .with_data_context(Context::HgncSymbolOrId)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease".to_string()))
                .with_data_context(Context::OmimLabel)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("dissease_OMIM_id".to_string()))
                .with_data_context(Context::OmimId)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HGVS_1".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HGVS_2".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HPO".to_string()))
                .with_data_context(Context::MultiHpoId),
        ],
    )
}

#[rstest]
fn test_j_data(excel_context: TableContext) -> Result<(), PipelineError> {
    
    let mut config = PhenoXtractorConfig::try_from(PathBuf::from_str("./assets/configs/j_data_config.yaml")).unwrap();
    let mut pipeline = Pipeline::try_from(config.pipeline).unwrap();
    pipeline.run(config.data_sources.as_mut_slice())?;

    Ok(())
}
