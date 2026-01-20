use phenopackets::schema::v2::Phenopacket;
use phenoxtract::Pipeline;
use phenoxtract::config::context::Context;
use phenoxtract::config::table_context::{Identifier, SeriesContext, TableContext};
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CSVDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::CachedOntologyFactory;
use phenoxtract::ontology::resource_references::OntologyRef;
use phenoxtract::ontology::traits::HasPrefixId;
use phenoxtract::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use phenoxtract::transform::strategies::OntologyNormaliserStrategy;
use phenoxtract::transform::strategies::traits::Strategy;
use phenoxtract::transform::strategies::{
    AliasMapStrategy, MappingStrategy, MultiHPOColExpansionStrategy,
};
use phenoxtract::transform::{PhenopacketBuilder, TransformerModule};
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use rstest::{fixture, rstest};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

// Fixtures (rstest)
#[fixture]
fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create temporary directory")
}

// helper functions
fn build_hgnc_test_client(temp_dir: &Path) -> CachedHGNCClient {
    CachedHGNCClient::new(temp_dir.join("test_hgnc_cache"), HGNCClient::default()).unwrap()
}
fn build_hgvs_test_client(temp_dir: &Path) -> CachedHGVSClient {
    CachedHGVSClient::new(temp_dir.join("test_hgvs_cache"), HGVSClient::default()).unwrap()
}

// Macro (skip in CI)
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

// Preprocessing function
fn preprocess_keio_csv(input: &Path, out_dir: &Path) -> PathBuf {
    let out_path = out_dir.join("keio_corrected.csv");

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(input)
        .expect("Failed to open input CSV");

    let headers = rdr.headers().expect("Failed to read headers").clone();

    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(&out_path)
        .expect("Failed to create corrected CSV");

    wtr.write_record(&headers).expect("Failed to write headers");

    // Precompute column indices once (faster and cleaner)
    let idx_hgvs1 = headers.iter().position(|h| h == "HGVS_1");
    let idx_hgvs2 = headers.iter().position(|h| h == "HGVS_2");
    let idx_hpo = headers.iter().position(|h| h == "HPO");
    let idx_dis = headers.iter().position(|h| h == "disease");

    for result in rdr.records() {
        let rec = result.expect("Failed to read record");

        // Build a NEW record with corrected fields
        let mut out_rec = csv::StringRecord::new();

        for (i, field) in rec.iter().enumerate() {
            let mut v = field.to_string();

            if idx_hgvs1 == Some(i) || idx_hgvs2 == Some(i) {
                v = v.replace('*', ":");
            }
            if idx_hpo == Some(i) {
                v = v.replace("HPO", "HP");
            }
            if idx_dis == Some(i) {
                v = v.replace('\t', "");
                v = v.trim().to_string();
            }

            // push_field takes &str, so we push v as a &str
            out_rec.push_field(&v);
        }

        wtr.write_record(&out_rec)
            .expect("Failed to write corrected record");
    }

    wtr.flush().expect("Failed to flush corrected CSV");
    out_path
}

// The actual test
#[rstest]
fn test_complete_keio(temp_dir: TempDir) {
    skip_in_ci!();
    let cohort_name = "irud_test_cohort";
    let mut onto_factory = CachedOntologyFactory::default();
    let hpo_dict = onto_factory
        .build_bidict(&OntologyRef::hp_with_version("2025-09-01"), None)
        .unwrap();

    let mondo_dict = onto_factory
        .build_bidict(&OntologyRef::mondo_with_version("2026-01-06"), None)
        .unwrap();
    let assets_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(PathBuf::from(file!()).parent().unwrap().join("assets"));
    let raw_csv = assets_path.join("irud").join("TestData_20250716.csv");
    let corrected_csv = preprocess_keio_csv(&raw_csv, temp_dir.path());

    let keio_context = TableContext::new(
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
            // --- IMPORTANT --- OMIM IDs like "OMIM:123700" currently cannot be parsed as disease terms
            // OMIM context missing
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease".to_string()))
                .with_data_context(Context::MondoLabelOrId)
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
    );

    // Build the DataSource array

    let mut data_sources = [DataSource::Csv(CSVDataSource::new(
        corrected_csv,
        None,
        keio_context,
        ExtractionConfig::new("TestData".to_string(), true, true),
    ))];

    // Configure strategies (transformations)

    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(AliasMapStrategy),
        Box::new(MappingStrategy::default_sex_mapping_strategy()),
        Box::new(OntologyNormaliserStrategy::new(
            mondo_dict.clone(),
            Context::MondoLabelOrId,
        )),
        Box::new(MultiHPOColExpansionStrategy),
    ];

    // PhenopacketBuilder

    let phenopacket_builder = PhenopacketBuilder::new(
        HashMap::from_iter([
            (hpo_dict.ontology.prefix_id().to_string(), hpo_dict),
            (mondo_dict.ontology.prefix_id().to_string(), mondo_dict),
        ]),
        Box::new(build_hgnc_test_client(temp_dir.path())),  
        Box::new(build_hgvs_test_client(temp_dir.path())),  
    );

    let transformer_module = TransformerModule::new(
        strategies,
        CdfCollectorBroker::with_default_collectors(
            phenopacket_builder, cohort_name.to_owned()),
    );

    // Loader + Pipeline

    let output_dir = assets_path.join("irud").join("phenopackets");
    fs::create_dir_all(&output_dir).unwrap();
    let loader = Box::new(
        FileSystemLoader::new(output_dir.clone(), true));

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
        assert!(
            !txt.contains('*'),
            "Output still contains '*' (HGVS not corrected?)"
        );

        let pp: Phenopacket = serde_json::from_str(&txt).unwrap();
        let sid = pp.subject.as_ref().map(|s| s.id.as_str()).unwrap_or("");
        assert!(expected_ids.contains(sid), "Unexpected subject id: {}", sid);
        assert!(txt.contains("HP:"), "No HP: terms found in output {:?}", p);
    }
}
