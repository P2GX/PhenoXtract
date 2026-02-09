use crate::Pipeline;
use crate::config::datasource_config::{
    AliasMapConfig, CsvConfig, ExcelSheetConfig, ExcelWorkbookConfig, MappingsConfig,
    MappingsCsvConfig, SeriesContextConfig,
};
use crate::config::resource_config_factory::ResourceConfigFactory;
use crate::config::table_context::{AliasMap, SeriesContext};
use crate::config::{
    ConfigLoader, DataSourceConfig, PhenoXtractConfig, PipelineConfig, TableContext,
};
use crate::error::ConstructionError;
use crate::extract::extraction_config::ExtractionConfig;
use crate::extract::{CsvDataSource, DataSource, ExcelDataSource};
use crate::load::loader_factory::LoaderFactory;
use crate::phenoxtract::Phenoxtract;
use crate::transform::bidict_library::BiDictLibrary;
use crate::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use crate::transform::strategies::strategy_factory::StrategyFactory;
use crate::transform::strategies::traits::Strategy;
use crate::transform::{PhenopacketBuilder, TransformerModule};
use crate::utils::get_cache_dir;
use pivot::hgnc::CachedHGNCClient;
use pivot::hgvs::CachedHGVSClient;
use polars::prelude::{CsvReadOptions, SerReader};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

// --- PHENOXTRACT FROM CONFIG ---

impl TryFrom<PhenoXtractConfig> for Phenoxtract {
    type Error = ConstructionError;

    fn try_from(config: PhenoXtractConfig) -> Result<Self, Self::Error> {
        let pipeline = Pipeline::try_from(config.pipeline_config)?;
        let data_sources = config
            .data_sources
            .into_iter()
            .map(DataSource::try_from)
            .collect::<Result<Vec<DataSource>, ConstructionError>>()?;
        Ok(Phenoxtract::new(pipeline, data_sources))
    }
}

impl TryFrom<PathBuf> for PhenoXtractConfig {
    type Error = ConstructionError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Ok(ConfigLoader::load(path)?)
    }
}

// --- PIPELINE FROM CONFIG ---

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

impl TryFrom<PhenoXtractConfig> for Pipeline {
    type Error = ConstructionError;

    fn try_from(config: PhenoXtractConfig) -> Result<Self, Self::Error> {
        Pipeline::try_from(config.pipeline_config)
    }
}

impl TryFrom<PathBuf> for Pipeline {
    type Error = ConstructionError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        if !path.exists() {
            return Err(ConstructionError::NoConfigFileFound(path));
        }
        let config: PhenoXtractConfig = ConfigLoader::load(path.clone())?;

        Pipeline::try_from(config)
    }
}

// --- DATASOURCE FROM CONFIG ---

impl TryFrom<DataSourceConfig> for DataSource {
    type Error = ConstructionError;

    fn try_from(config: DataSourceConfig) -> Result<Self, Self::Error> {
        match config {
            DataSourceConfig::Excel(excel_config) => {
                Ok(DataSource::Excel(ExcelDataSource::try_from(excel_config)?))
            }
            DataSourceConfig::Csv(csv_config) => {
                Ok(DataSource::Csv(CsvDataSource::try_from(csv_config)?))
            }
        }
    }
}

impl TryFrom<ExcelWorkbookConfig> for ExcelDataSource {
    type Error = ConstructionError;

    fn try_from(config: ExcelWorkbookConfig) -> Result<Self, Self::Error> {
        let tcs = config
            .sheets
            .clone()
            .into_iter()
            .map(TableContext::try_from)
            .collect::<Result<Vec<TableContext>, ConstructionError>>()?;

        let ecs = config
            .sheets
            .into_iter()
            .map(|sheet_config| ExtractionConfig {
                name: sheet_config.sheet_name,
                has_headers: sheet_config.has_headers,
                patients_are_rows: sheet_config.patients_are_rows,
            })
            .collect();

        Ok(ExcelDataSource {
            source: config.source,
            contexts: tcs,
            extraction_configs: ecs,
        })
    }
}

impl TryFrom<ExcelSheetConfig> for TableContext {
    type Error = ConstructionError;

    fn try_from(config: ExcelSheetConfig) -> Result<Self, Self::Error> {
        let scs = config
            .contexts
            .into_iter()
            .map(SeriesContext::try_from)
            .collect::<Result<Vec<SeriesContext>, ConstructionError>>()?;

        Ok(TableContext::new(config.sheet_name, scs))
    }
}

impl TryFrom<CsvConfig> for CsvDataSource {
    type Error = ConstructionError;

    fn try_from(config: CsvConfig) -> Result<Self, Self::Error> {
        let scs = config
            .contexts
            .into_iter()
            .map(SeriesContext::try_from)
            .collect::<Result<Vec<SeriesContext>, ConstructionError>>()?;

        let tc = TableContext::new("CsvData".to_string(), scs);

        Ok(CsvDataSource {
            source: config.source,
            separator: config.separator,
            extraction_config: ExtractionConfig {
                name: "CsvData".to_string(),
                has_headers: config.has_headers,
                patients_are_rows: config.patients_are_rows,
            },
            context: tc,
        })
    }
}

impl TryFrom<SeriesContextConfig> for SeriesContext {
    type Error = ConstructionError;

    fn try_from(config: SeriesContextConfig) -> Result<Self, Self::Error> {
        let alias_map = if let Some(am_config) = config.alias_map_config {
            Some(AliasMap::try_from(am_config)?)
        } else {
            None
        };

        Ok(SeriesContext::new(
            config.identifier,
            config.header_context,
            config.data_context,
            config.fill_missing,
            alias_map,
            config.building_block_id,
        ))
    }
}

impl TryFrom<AliasMapConfig> for AliasMap {
    type Error = ConstructionError;

    fn try_from(config: AliasMapConfig) -> Result<Self, Self::Error> {
        let hash_map = match config.mappings {
            MappingsConfig::HashMap(hash_map) => hash_map,
            MappingsConfig::Csv(mappings_csv_config) => HashMap::try_from(mappings_csv_config)?,
        };

        Ok(AliasMap::new(hash_map, config.output_data_type))
    }
}

impl TryFrom<MappingsCsvConfig> for HashMap<String, Option<String>> {
    type Error = ConstructionError;

    fn try_from(config: MappingsCsvConfig) -> Result<Self, Self::Error> {
        let mut csv_read_options = CsvReadOptions::default();

        let alias_df = (|| {
            csv_read_options
                .try_into_reader_with_file_path(Some(config.path.clone()))?
                .finish()
        })()
            .map_err(|err| ConstructionError::LoadingAliases {
                path: config.path,
                err,
            })?;

        // Extract the two columns
        let keys = alias_df.column(config.key_column_name.as_str())?.str()?;
        let aliases = alias_df.column(config.alias_column_name.as_str())?.str()?;

        // Build the HashMap
        let map = keys
            .into_iter()
            .zip(aliases.into_iter())
            .filter_map(|(k, v)| Some((k?.to_string(), v?.to_string())))
            .collect::<HashMap<_, _>>();

        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConfigLoader, PhenoXtractConfig};
    use crate::test_suite::config::get_full_config_bytes;
    use dotenvy::dotenv;
    use rstest::{fixture, rstest};
    use std::fs::File as StdFile;
    use std::io::Write;
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_try_from_pipeline_config(temp_dir: TempDir) {
        dotenv().ok();
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).expect("Failed to create config file");
        file.write_all(get_full_config_bytes().as_slice())
            .expect("Failed to write config file");
        let config: PhenoXtractConfig =
            ConfigLoader::load(file_path.clone()).expect("Failed to load config loader");

        let configs_from_sources = [
            Pipeline::try_from(config.clone()).expect("Failed to convert config from config"),
            Pipeline::try_from(config.pipeline_config.clone())
                .expect("Failed to convert config from pipeline"),
            Pipeline::try_from(file_path).expect("Failed to convert config from path"),
        ];

        let expected_config = configs_from_sources.first().unwrap();

        for config in configs_from_sources.iter() {
            assert_eq!(config, expected_config);
        }
    }
}
