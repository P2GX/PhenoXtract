#![allow(clippy::too_many_arguments)]

use dotenvy::dotenv;
use integration_tests::{
    build_hgnc_test_client, build_hgvs_test_client, hp_ref, mondo_ref, ontology_registry_dir,
    temp_dir, tests_assets,
};

use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;

use phenopackets::schema::v2::Phenopacket;
use phenoxtract::Pipeline;
use phenoxtract::config::context::{Context, ContextKind};
use phenoxtract::config::table_context::{Identifier, SeriesContext, TableContext};
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CsvDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::CachedOntologyFactory;
use phenoxtract::ontology::loinc_client::LoincClient;
use phenoxtract::ontology::resource_references::ResourceRef;
use phenoxtract::transform::bidict_library::BiDictLibrary;
use phenoxtract::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use phenoxtract::transform::phenopacket_builder::BuilderMetaData;
use phenoxtract::transform::strategies::traits::Strategy;
use phenoxtract::transform::strategies::{
    AliasMapStrategy, MappingStrategy, MultiHPOColExpansionStrategy, OntologyNormaliserStrategy
};
use phenoxtract::transform::{PhenopacketBuilder, TransformerModule};

use rstest::{fixture, rstest};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

// Skip macro (same spirit as before)
#[macro_export]
macro_rules! skip_in_ci {
    ($test_name:expr) => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", $test_name);
            return;
        }
    };
    () => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", module_path!());
            return;
        }
    };
}

#[fixture]
fn keio_context() -> TableContext {
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
                .with_identifier(Identifier::Regex("gene_id".to_string()))
                .with_data_context(Context::HgncSymbolOrId)
                .with_building_block_id(Some("A".to_string())),

            // keep both
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease".to_string()))
                .with_data_context(Context::DiseaseLabelOrId)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease_OMIM_id".to_string()))
                .with_data_context(Context::DiseaseLabelOrId)
                .with_building_block_id(Some("A".to_string())),

            SeriesContext::default()
                .with_identifier(Identifier::Regex("zygocity".to_string()))
                .with_data_context(Context::Zygosity)
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
                .with_data_context(Context::MultiHpoId)
                .with_building_block_id(Some("B".to_string())),
        ],
    )
}

#[rstest]
fn test_complete_keio(
    keio_context: TableContext,
    temp_dir: TempDir,
    hp_ref: ResourceRef,
    mondo_ref: ResourceRef,
    tests_assets: PathBuf,
) {
    skip_in_ci!();
    dotenv().ok();

    // If you require BioPortal/OMIM resolution and want to skip when missing:
    if std::env::var("BIOPORTAL_API_KEY").is_err() {
        println!("Skipping test_complete_keio: BIOPORTAL_API_KEY not set");
        return;
    }

    // Ontology registry + factory (same pattern as test_pipeline_integration)
    let mut onto_factory = CachedOntologyFactory::new(Box::new(FileSystemOntologyRegistry::new(
        ontology_registry_dir().expect("ontology_registry_dir could not be created"),
        BioRegistryMetadataProvider::default(),
        OboLibraryProvider::default(),
    )));

    let hpo_dict = Box::new(onto_factory.build_bidict(&hp_ref, None).unwrap());
    let mondo_dict = Box::new(onto_factory.build_bidict(&mondo_ref, None).unwrap());

    // OMIM dict via factory (depends on your wiring)
    let omim_ref = ResourceRef::omim();
    let omim_dict = Box::new(onto_factory.build_bidict(&omim_ref, None).unwrap());

    // Input + output paths (this is what you asked for)
    let irud_dir = tests_assets.join("irud");
    let input_csv = irud_dir.join("TestData_20250716.csv");
    let output_dir = irud_dir.join("phenopackets");
    fs::create_dir_all(&output_dir).unwrap();

    // optional: clear old output
    for entry in fs::read_dir(&output_dir).unwrap() {
        let p = entry.unwrap().path();
        if p.extension() == Some(OsStr::new("json")) {
            let _ = fs::remove_file(p);
        }
    }

    // Data source
    let mut data_sources = [DataSource::Csv(CsvDataSource::new(
        input_csv,
        Some(','),
        keio_context,
        ExtractionConfig::new("TestData".to_string(), true, true),
    ))];

    // Strategies: do the old preprocessing here (no csv crate needed)
    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(AliasMapStrategy),
        Box::new(MappingStrategy::default_sex_mapping_strategy()),

        // HGVS: "*" -> ":"
        Box::new(StringCorrectionStrategy::new(
            ContextKind::None,
            ContextKind::Hgvs,
            "*".to_string(),
            ":".to_string(),
        )),

        // Multi-HPO: "HPO" -> "HP"
        Box::new(StringCorrectionStrategy::new(
            ContextKind::None,
            ContextKind::MultiHpoId,
            "HPO".to_string(),
            "HP".to_string(),
        )),

        // Disease label: strip tabs (optional but matches your YAML)
        Box::new(StringCorrectionStrategy::new(
            ContextKind::None,
            ContextKind::DiseaseLabelOrId,
            "\t".to_string(),
            "".to_string(),
        )),

        // Normalise HPO label/id behaviour (same as integration test style)
        Box::new(OntologyNormaliserStrategy::new(
            onto_factory.build_bidict(&hp_ref, None).unwrap(),
            ContextKind::HpoLabelOrId,
        )),

        Box::new(MultiHPOColExpansionStrategy),
    ];

    let cohort_name = "irud_test_cohort".to_string();
    let phenopacket_builder = PhenopacketBuilder::new(
        BuilderMetaData::new(cohort_name, "IRUD KEIO", "AG"),
        Box::new(build_hgnc_test_client(temp_dir.path())),
        Box::new(build_hgvs_test_client(temp_dir.path())),
        BiDictLibrary::new("HPO", vec![hpo_dict]),
        // DISEASE: include MONDO + OMIM so OMIM:xxxx can yield a label
        BiDictLibrary::new("DISEASE", vec![mondo_dict, omim_dict]),
        BiDictLibrary::empty_with_name("UNIT"),
        BiDictLibrary::new("ASSAY", vec![Box::new(LoincClient::default())]),
        BiDictLibrary::empty_with_name("QUAL"),
    );

    let transformer_module = TransformerModule::new(
        strategies,
        CdfCollectorBroker::with_default_collectors(phenopacket_builder),
    );

    let loader = Box::new(FileSystemLoader::new(output_dir.clone(), true));
    let mut pipeline = Pipeline::new(transformer_module, loader);
    pipeline.run(&mut data_sources).unwrap();

    // Assertions
    let expected_ids: HashSet<&str> = HashSet::from([
        "patient_0001",
        "patient_0002",
        "patient_0003",
        "patient_0004",
        "patient_0005",
    ]);

    let mut json_files = vec![];
    for entry in fs::read_dir(&output_dir).unwrap() {
        let p = entry.unwrap().path();
        if p.extension() == Some(OsStr::new("json")) {
            json_files.push(p);
        }
    }
    assert_eq!(json_files.len(), 5, "Expected 5 phenopackets");

    for p in json_files {
        let txt = fs::read_to_string(&p).unwrap();
        assert!(!txt.contains('*'), "Output still contains '*' {:?}", p);
        assert!(txt.contains("HP:"), "No HP: terms found in {:?}", p);

        let pp: Phenopacket = serde_json::from_str(&txt).unwrap();
        let sid = pp.subject.as_ref().map(|s| s.id.as_str()).unwrap_or("");
        assert!(expected_ids.contains(sid), "Unexpected subject id: {}", sid);

        // useful sanity check
        assert!(txt.contains("OMIM:"), "No OMIM ids found in {:?}", p);
    }
}



