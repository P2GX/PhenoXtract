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
use crate::ontology::CachedOntologyFactory;
use crate::phenoxtract::Phenoxtract;
use crate::transform::bidict_library::BiDictLibrary;
use crate::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use crate::transform::strategies::strategy_factory::StrategyFactory;
use crate::transform::strategies::traits::Strategy;
use crate::transform::{PhenopacketBuilder, TransformerModule};
use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;
use pivot::hgnc::CachedHGNCClient;
use pivot::hgvs::CachedHGVSClient;
use polars::prelude::{CsvReadOptions, SerReader};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
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

impl TryFrom<PathBuf> for Phenoxtract {
    type Error = ConstructionError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let config: PhenoXtractConfig = ConfigLoader::load(path)?;
        Phenoxtract::try_from(config)
    }
}

// --- PIPELINE FROM CONFIG ---

impl TryFrom<PipelineConfig> for Pipeline {
    type Error = ConstructionError;

    fn try_from(config: PipelineConfig) -> Result<Self, Self::Error> {
        let cache_dir = config
            .cache_dir
            .expect("Pipeline config missing cache_dir.");
        let ontology_registry_dir = cache_dir.join("ontology_registry");

        if !ontology_registry_dir.exists() {
            fs::create_dir_all(&ontology_registry_dir)?;
        }

        let ontology_registry = FileSystemOntologyRegistry::new(
            ontology_registry_dir,
            BioRegistryMetadataProvider::default(),
            OboLibraryProvider::default(),
        );

        let mut resource_factory =
            ResourceConfigFactory::new(CachedOntologyFactory::new(ontology_registry));

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
            // TODO: Update with procedure, anatomy and treatment bi dict lib
            BiDictLibrary::default(),
            BiDictLibrary::default(),
            BiDictLibrary::default(),
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

        let path_string = config.source.display().to_string();

        let tc = TableContext::new(path_string.clone(), scs);

        Ok(CsvDataSource {
            source: config.source,
            separator: config.separator,
            extraction_config: ExtractionConfig {
                name: path_string,
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
            config.sub_blocks,
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
        let csv_read_options = CsvReadOptions::default().with_columns(Some(Arc::new([
            config.key_column_name.clone().into(),
            config.alias_column_name.clone().into(),
        ])));

        let alias_df = (|| {
            csv_read_options
                .try_into_reader_with_file_path(Some(config.path.clone()))?
                .finish()
        })()
        .map_err(|err| ConstructionError::LoadingAliases {
            path: config.path.clone(),
            err,
        })?;

        let (keys, aliases) = (|| {
            let keys = alias_df.column(config.key_column_name.as_str())?.str()?;
            let aliases = alias_df.column(config.alias_column_name.as_str())?.str()?;
            Ok((keys, aliases))
        })()
        .map_err(|err| ConstructionError::LoadingAliases {
            path: config.path.clone(),
            err,
        })?;

        let mut hash_map = HashMap::new();

        for (key, alias) in keys.iter().zip(aliases.iter()) {
            if let Some(key) = key {
                hash_map.insert(key.to_string(), alias.map(|a| a.to_string()));
            }
        }

        Ok(hash_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::OutputDataType;
    use crate::config::{ConfigLoader, PhenoXtractConfig};
    use crate::test_suite::config::{
        CSV_DATASOURCE_CONFIG_FILE, EXCEL_DATASOURCE_CONFIG_FILE, PIPELINE_CONFIG_FILE,
        get_full_config_bytes,
    };
    use dotenvy::dotenv;
    use rstest::{fixture, rstest};
    use std::fmt::Write;
    use std::fs::{File as StdFile, File};
    use std::io::Write as StdWrite;
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_try_from_phenoxtract_config(temp_dir: TempDir) {
        dotenv().ok();
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).expect("Failed to create config file");
        file.write_all(get_full_config_bytes().as_slice())
            .expect("Failed to write config file");
        let config: PhenoXtractConfig =
            ConfigLoader::load(file_path.clone()).expect("Failed to load config loader");

        let phenoxtract_from_config =
            Phenoxtract::try_from(config.clone()).expect("Failed to convert config from config");
        let phenoxtract_from_path =
            Phenoxtract::try_from(file_path).expect("Failed to convert config from path");

        assert_eq!(phenoxtract_from_path, phenoxtract_from_config);

        assert_eq!(phenoxtract_from_config.data_sources.len(), 2);
        assert_eq!(
            phenoxtract_from_config
                .pipeline
                .transformer_module
                .strategies
                .len(),
            2
        );
    }

    #[rstest]
    fn test_try_from_pipeline_config(temp_dir: TempDir) {
        dotenv().ok();
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).expect("Failed to create config file");
        file.write_all(PIPELINE_CONFIG_FILE)
            .expect("Failed to write config file");
        let config: PipelineConfig =
            ConfigLoader::load(file_path.clone()).expect("Failed to load config loader");

        let pipeline_from_config =
            Pipeline::try_from(config.clone()).expect("Failed to convert config from config");

        assert_eq!(pipeline_from_config.transformer_module.strategies.len(), 2);
    }

    #[rstest]
    fn test_try_from_csv_datasource_config(temp_dir: TempDir) {
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).expect("Failed to create config file");
        file.write_all(CSV_DATASOURCE_CONFIG_FILE)
            .expect("Failed to write config file");
        let config: DataSourceConfig =
            ConfigLoader::load(file_path.clone()).expect("Failed to load config loader");

        let csv_datasource_from_config =
            DataSource::try_from(config.clone()).expect("Failed to convert config from config");

        match csv_datasource_from_config {
            DataSource::Csv(csv_source) => {
                assert_eq!(csv_source.context.context().len(), 3);
            }
            DataSource::Excel(_) => {
                panic!("Loaded Excel Datasource instead of Csv!")
            }
        }
    }

    #[rstest]
    fn test_try_from_excel_datasource_config(temp_dir: TempDir) {
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).expect("Failed to create config file");
        file.write_all(EXCEL_DATASOURCE_CONFIG_FILE)
            .expect("Failed to write config file");
        let config: DataSourceConfig =
            ConfigLoader::load(file_path.clone()).expect("Failed to load config loader");

        let excel_datasource_from_config =
            DataSource::try_from(config.clone()).expect("Failed to convert config from config");

        match excel_datasource_from_config {
            DataSource::Csv(_) => {
                panic!("Loaded Csv Datasource instead of Excel!")
            }
            DataSource::Excel(excel_source) => {
                assert_eq!(excel_source.contexts.len(), 2)
            }
        }
    }

    #[rstest]
    fn test_try_from_alias_map_config_hash_map(temp_dir: TempDir) {
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).expect("Failed to create config file");
        file.write_all(CSV_DATASOURCE_CONFIG_FILE)
            .expect("Failed to write config file");
        let config: DataSourceConfig =
            ConfigLoader::load(file_path.clone()).expect("Failed to load config loader");

        let csv_datasource_from_config =
            DataSource::try_from(config.clone()).expect("Failed to convert config from config");

        match csv_datasource_from_config {
            DataSource::Csv(csv_source) => {
                let sc = csv_source.context.context().first().unwrap();
                let am = sc.get_alias_map().unwrap();
                assert_eq!(am.get_hash_map().len(), 5);
            }
            DataSource::Excel(_) => {
                panic!("Loaded Excel Datasource instead of Csv!")
            }
        }
    }

    #[fixture]
    fn column_names() -> [&'static str; 3] {
        ["KEYS", "ALIASES", "JUNK"]
    }
    #[fixture]
    fn keys() -> [&'static str; 3] {
        ["k1", "k2", "k3"]
    }

    #[fixture]
    fn aliases() -> [&'static str; 3] {
        ["a1", "", "a3"]
    }

    #[fixture]
    fn junk() -> [&'static str; 3] {
        ["123iuh124", "", "asn"]
    }

    #[fixture]
    fn mappings_csv_data(
        column_names: [&'static str; 3],
        keys: [&'static str; 3],
        aliases: [&'static str; 3],
        junk: [&'static str; 3],
    ) -> Vec<u8> {
        let mut csv_content = column_names.join(",") + "\n";

        for i in 0..keys.len() {
            writeln!(&mut csv_content, "{},{},{}", keys[i], aliases[i], junk[i]).unwrap();
        }

        csv_content.into_bytes()
    }

    #[rstest]
    fn test_try_from_alias_map_config_csv_path(temp_dir: TempDir, mappings_csv_data: Vec<u8>) {
        let mappings_csv_file_path = temp_dir.path().join("csv_data.csv");
        let mut csv_file = File::create(&mappings_csv_file_path).unwrap();
        csv_file.write_all(mappings_csv_data.as_slice()).unwrap();

        let alias_map_config = format!(
            r#"
                output_data_type: String
                mappings:
                  path: "{}"
                  key_column_name: "KEYS"
                  alias_column_name: "ALIASES"
                  "#,
            mappings_csv_file_path.display()
        );

        let config_file_path = temp_dir.path().join("config.yaml");
        let mut config_file = File::create(&config_file_path).unwrap();
        config_file.write_all(alias_map_config.as_bytes()).unwrap();

        let config: AliasMapConfig =
            ConfigLoader::load(config_file_path.clone()).expect("Failed to load config loader");

        let alias_map_from_config =
            AliasMap::try_from(config.clone()).expect("Failed to convert config from config");

        let mut expected_hm = HashMap::new();
        expected_hm.insert("k1".to_string(), Some("a1".to_string()));
        expected_hm.insert("k2".to_string(), None);
        expected_hm.insert("k3".to_string(), Some("a3".to_string()));

        let expected_alias_map = AliasMap::new(expected_hm, OutputDataType::String);

        assert_eq!(alias_map_from_config, expected_alias_map);
    }
}
