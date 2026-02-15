use std::{collections::HashMap, env::home_dir, fs, path::PathBuf};

use phenoxtract::{
    Pipeline,
    config::table_context::{Context, Identifier, SeriesContext, TableContext},
    extract::{DataSource, ExcelDatasource, extraction_config::ExtractionConfig},
    load::FileSystemLoader,
    ontology::{CachedOntologyFactory, OntologyRef, traits::HasPrefixId},
    transform::{
        Collector, PhenopacketBuilder, TransformerModule,
        strategies::{
            AliasMapStrategy, MappingStrategy, MultiHPOColExpansionStrategy,
            OntologyNormaliserStrategy,
        },
        traits::Strategy,
    },
};
use rstest::{fixture, rstest};

#[fixture]
fn usidnet_path() -> PathBuf {
    let homedir = home_dir().unwrap();
    homedir.join("data/usidnet/old_registry_data.xlsx")
}

#[fixture]
fn cohort_sheet_context() -> TableContext {
    TableContext::new(
        "Cohort".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("PatientId".to_string()))
                .with_data_context(Context::SubjectId),
        ],
    )
}

#[rstest]
fn test_cohort_import(usidnet_path: PathBuf, cohort_sheet_context: TableContext) {
    let cohort_name = "USID-Net";

    let mut onto_factory = CachedOntologyFactory::default();

    let hpo_dict = onto_factory
        .build_bidict(&OntologyRef::hp(Some("2025-09-01".to_string())), None)
        .unwrap();

    let mut data_sources = [DataSource::Excel(ExcelDatasource::new(
        usidnet_path,
        vec![cohort_sheet_context],
        vec![ExtractionConfig::new("Cohort".to_string(), true, true)],
    ))];

    //Configure strategies (a.k.a. transformations)
    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(AliasMapStrategy),
        Box::new(OntologyNormaliserStrategy::new(
            hpo_dict.clone(),
            Context::HpoLabelOrId,
        )),
        Box::new(MappingStrategy::default_sex_mapping_strategy()),
        Box::new(MultiHPOColExpansionStrategy),
    ];

    let phenopacket_builder = PhenopacketBuilder::new(HashMap::from_iter([(
        hpo_dict.ontology.prefix_id().to_string(),
        hpo_dict,
    )]));
    //Create the pipeline
    let transformer_module = TransformerModule::new(
        strategies,
        Collector::new(phenopacket_builder, cohort_name.to_owned()),
    );
    let assets_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        PathBuf::from(file!())
            .parent()
            .unwrap()
            .join("usidnet_assets"),
    );
    let output_dir = assets_path.join("do_not_push");
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = FileSystemLoader::new(output_dir.clone());

    let mut pipeline = Pipeline::new(transformer_module, loader);

    //Run the pipeline on the data sources
    let res = pipeline.run(&mut data_sources);
    assert!(res.is_ok());
}
