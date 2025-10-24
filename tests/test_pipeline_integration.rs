use phenopackets::schema::v2::Phenopacket;
use phenoxtract::Pipeline;
use phenoxtract::config::table_context::{
    AliasMap, Context, Identifier, OutputDataType, SeriesContext, TableContext,
};
use phenoxtract::extract::ExcelDatasource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CSVDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::resource_references::OntologyRef;

use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature, Resource, TimeElement};
use phenoxtract::ontology::CachedOntologyFactory;
use phenoxtract::ontology::traits::HasPrefixId;
use phenoxtract::transform::strategies::MappingStrategy;
use phenoxtract::transform::strategies::OntologyNormaliserStrategy;
use phenoxtract::transform::strategies::{AliasMapStrategy, MultiHPOColExpansionStrategy};
use phenoxtract::transform::traits::Strategy;
use phenoxtract::transform::{Collector, PhenopacketBuilder, TransformerModule};
use prost_types::Timestamp;
use rstest::{fixture, rstest};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::path::PathBuf;

#[fixture]
fn vital_status_aliases() -> AliasMap {
    let mut vs_hash_map: HashMap<String, String> = HashMap::default();
    vs_hash_map.insert("Yes".to_string(), "ALIVE".to_string());
    vs_hash_map.insert("No".to_string(), "DECEASED".to_string());
    AliasMap::new(vs_hash_map, OutputDataType::String)
}

#[fixture]
fn csv_context() -> TableContext {
    TableContext::new(
        "CSV_Table".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("0".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("1".to_string()))
                .with_data_context(Context::HpoLabelOrId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("2".to_string()))
                .with_data_context(Context::HpoLabelOrId),
        ],
    )
}

#[fixture]
fn csv_context_2() -> TableContext {
    TableContext::new(
        "CSV_Table_2".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Patient ID".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HP:0012373".to_string()))
                .with_header_context(Context::HpoLabelOrId)
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Rhinorrhea".to_string()))
                .with_header_context(Context::HpoLabelOrId)
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Date of onset".to_string()))
                .with_data_context(Context::OnsetDateTime)
                .with_building_block_id(Some("A".to_string())),
        ],
    )
}

#[fixture]
fn csv_context_3() -> TableContext {
    TableContext::new(
        "CSV_Table_3".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Patient ID".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HPOs".to_string()))
                .with_data_context(Context::MultiHpoId)
                .with_building_block_id(Some("B".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Date of onset".to_string()))
                .with_data_context(Context::OnsetDateTime)
                .with_building_block_id(Some("B".to_string())),
        ],
    )
}

#[fixture]
fn excel_context(vital_status_aliases: AliasMap) -> Vec<TableContext> {
    vec![
        TableContext::new(
            "basic info".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Patient ID".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Sex".to_string()))
                    .with_data_context(Context::SubjectSex),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Living".to_string()))
                    .with_data_context(Context::VitalStatus)
                    .with_alias_map(Some(vital_status_aliases)),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("DOB".to_string()))
                    .with_data_context(Context::DateOfBirth),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Time of death".to_string()))
                    .with_data_context(Context::TimeOfDeath),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex(
                        "Survival time since diagnosis (days)".to_string(),
                    ))
                    .with_data_context(Context::SurvivalTimeDays),
            ],
        ),
        TableContext::new(
            "conditions".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Patient ID".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Phenotypic Features".to_string()))
                    .with_data_context(Context::HpoLabelOrId)
                    .with_building_block_id(Some("C".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Age of onset".to_string()))
                    .with_data_context(Context::OnsetAge)
                    .with_building_block_id(Some("C".to_string())),
            ],
        ),
        TableContext::new(
            "more conditions".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Patient ID".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex(r"Phenotypic Features \d+".to_string()))
                    .with_data_context(Context::HpoLabelOrId),
            ],
        ),
    ]
}

#[rstest]
fn test_pipeline_integration(
    csv_context: TableContext,
    csv_context_2: TableContext,
    csv_context_3: TableContext,
    excel_context: Vec<TableContext>,
) {
    //Set-up
    let cohort_name = "my_cohort";

    let mut onto_factory = CachedOntologyFactory::default();

    let hpo_dict = onto_factory
        .build_bidict(&OntologyRef::hp(Some("2025-09-01".to_string())), None)
        .unwrap();
    let assets_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(PathBuf::from(file!()).parent().unwrap().join("assets"));

    //Configure data sources and contexts
    let csv_path = assets_path.clone().join("csv_data.csv");
    let csv_path_2 = assets_path.clone().join("csv_data_2.csv");
    let csv_path_3 = assets_path.clone().join("csv_data_3.csv");
    let excel_path = assets_path.clone().join("excel_data.xlsx");

    let mut data_sources = [
        DataSource::Csv(CSVDataSource::new(
            csv_path,
            None,
            csv_context,
            ExtractionConfig::new("CSV_Table".to_string(), false, true),
        )),
        DataSource::Csv(CSVDataSource::new(
            csv_path_2,
            None,
            csv_context_2,
            ExtractionConfig::new("CSV_Table_2".to_string(), true, false),
        )),
        DataSource::Csv(CSVDataSource::new(
            csv_path_3,
            None,
            csv_context_3,
            ExtractionConfig::new("CSV_Table_3".to_string(), true, false),
        )),
        DataSource::Excel(ExcelDatasource::new(
            excel_path,
            excel_context,
            vec![
                ExtractionConfig::new("basic info".to_string(), true, true),
                ExtractionConfig::new("conditions".to_string(), true, false),
                ExtractionConfig::new("more conditions".to_string(), true, false),
            ],
        )),
    ];

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

    let output_dir = assets_path.join("do_not_push");
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = FileSystemLoader::new(output_dir.clone());

    let mut pipeline = Pipeline::new(transformer_module, loader);

    //Run the pipeline on the data sources
    let res = pipeline.run(&mut data_sources);

    res.unwrap();

    let expected_phenopackets_files =
        fs::read_dir(assets_path.join("integration_test_expected_phenopackets")).unwrap();

    let mut expected_phenopackets: HashMap<String, Phenopacket> = HashMap::new();
    for expected_pp_file in expected_phenopackets_files {
        let data = fs::read_to_string(expected_pp_file.unwrap().path()).unwrap();
        let expected_pp: Phenopacket = serde_json::from_str(&data).unwrap();
        expected_phenopackets.insert(expected_pp.id.clone(), expected_pp);
    }

    for extracted_pp_file in fs::read_dir(output_dir).unwrap() {
        let data = fs::read_to_string(extracted_pp_file.unwrap().path()).unwrap();
        let extracted_pp: Phenopacket = serde_json::from_str(&data).unwrap();
        let extracted_pp_id = extracted_pp.id.clone();
        assert_pp(
            extracted_pp,
            expected_phenopackets.get(&extracted_pp_id).unwrap().clone(),
        );
    }
}

pub fn assert_pp(is: Phenopacket, expected: Phenopacket) {
    assert_field(&is.id, &expected.id, &is.id, "phenopacket_id");
    assert_individual(&is, &expected);
    assert_phenotypic_feature(&is, &expected);
    assert_meta_data(&is, &expected);
    println!("Its aaaall good, man. With your Phenopacket.")
}
pub fn assert_individual(is: &Phenopacket, expected: &Phenopacket) {
    let pp_id = is.id.clone();
    assert_some(
        is.subject.as_ref(),
        expected.subject.as_ref(),
        &pp_id,
        "subject",
    );

    if let Some(sub_pp) = &is.subject
        && let Some(sub_exp) = &expected.subject
    {
        assert_field(&sub_pp.id, &sub_exp.id, &pp_id, "subject_id");
        assert_field(&sub_pp.sex, &sub_exp.sex, &pp_id, "subject_sex");
        assert_field(&sub_pp.gender, &sub_exp.gender, &pp_id, "subject_gender");
        assert_field(
            &sub_pp.date_of_birth,
            &sub_exp.date_of_birth,
            &pp_id,
            "date_of_birth",
        );
        assert_field(
            &sub_pp.alternate_ids,
            &sub_exp.alternate_ids,
            &pp_id,
            "alternate_ids",
        );
        assert_field(
            &sub_pp.karyotypic_sex,
            &sub_exp.karyotypic_sex,
            &pp_id,
            "karyotypic_sex",
        );
        assert_field(&sub_pp.taxonomy, &sub_exp.taxonomy, &pp_id, "taxonomy");
        assert_field(
            &sub_pp.vital_status,
            &sub_exp.vital_status,
            &pp_id,
            "vital_status",
        );
    }
}

pub fn assert_phenotypic_feature(is: &Phenopacket, expected: &Phenopacket) {
    let mut pf_is = is.phenotypic_features.clone();
    sort_phenotypic_features(pf_is.as_mut());
    let mut pf_exp = expected.phenotypic_features.clone();
    sort_phenotypic_features(pf_exp.as_mut());

    if pf_is.len() != pf_exp.len() {
        get_mismatched_items(&pf_is, &pf_exp, &is.id, "phenotypic_features", |pf| {
            pf.r#type.clone()
        });
    }

    for (pf_is, pf_exp) in pf_is.iter().zip(&pf_exp) {
        assert_field(&pf_is.r#type, &pf_exp.r#type, &is.id, "phenotype");
        assert_field(&pf_is.onset, &pf_exp.onset, &is.id, "onset");

        let is_mod: HashSet<(String, String)> = pf_is
            .modifiers
            .iter()
            .map(|oc| (oc.id.clone(), oc.label.clone()))
            .collect();
        let exp_mod: HashSet<(String, String)> = pf_exp
            .modifiers
            .iter()
            .map(|oc| (oc.id.clone(), oc.label.clone()))
            .collect();
        assert_field(&is_mod, &exp_mod, &is.id, "modifiers");

        assert_field(&pf_is.excluded, &pf_exp.excluded, &is.id, "excluded");
        assert_field(&pf_is.severity, &pf_exp.severity, &is.id, "severity");
        assert_field(&pf_is.resolution, &pf_exp.resolution, &is.id, "resolution");
        assert_field(
            &pf_is.description,
            &pf_exp.description,
            &is.id,
            "description",
        );
        assert_field(&pf_is.evidence, &pf_exp.evidence, &is.id, "evidence");
    }
}

pub fn assert_meta_data(is: &Phenopacket, expected: &Phenopacket) {
    let pp_id = is.id.clone();
    let is_meta_data = is.meta_data.clone();
    let exp_meta_data = expected.meta_data.clone();
    assert_some(
        is_meta_data.as_ref(),
        exp_meta_data.as_ref(),
        &is.id,
        "meta_data",
    );
    if let Some(mut is_md) = is_meta_data
        && let Some(mut exp_md) = exp_meta_data
    {
        assert_field(&is_md.created, &exp_md.created, &pp_id, "created");
        assert_field(
            &is_md.submitted_by,
            &exp_md.submitted_by,
            &pp_id,
            "submitted_by",
        );
        assert_field(&is_md.created_by, &exp_md.created_by, &pp_id, "created_by");
        assert_field(
            &is_md.phenopacket_schema_version,
            &exp_md.phenopacket_schema_version,
            &pp_id,
            "phenopacket_schema_version",
        );
        let is_updates: HashSet<(String, Option<Timestamp>, String)> = is_md
            .updates
            .iter()
            .map(|u| (u.updated_by.clone(), u.timestamp, u.comment.clone()))
            .collect();
        let exp_updates: HashSet<(String, Option<Timestamp>, String)> = exp_md
            .updates
            .iter()
            .map(|u| (u.updated_by.clone(), u.timestamp, u.comment.clone()))
            .collect();
        assert_field(&is_updates, &exp_updates, &pp_id, "updates");

        let is_external_references: HashSet<(String, String, String)> = is_md
            .external_references
            .iter()
            .map(|er| (er.id.clone(), er.description.clone(), er.reference.clone()))
            .collect();
        let exp_external_references: HashSet<(String, String, String)> = is_md
            .external_references
            .iter()
            .map(|er| (er.id.clone(), er.description.clone(), er.reference.clone()))
            .collect();
        assert_field(
            &is_external_references,
            &exp_external_references,
            &pp_id,
            "external_references",
        );

        if is_md.resources.len() != exp_md.resources.len() {
            get_mismatched_items(
                &is_md.resources,
                &exp_md.resources,
                &pp_id,
                "resources",
                |res| res.id.clone(),
            );
        }
        sort_resources_case_insensitive(&mut is_md.resources);
        sort_resources_case_insensitive(&mut exp_md.resources);

        for (is_res, exp_res) in is_md
            .resources
            .iter()
            .zip(&exp_md.resources)
            .collect::<Vec<(&Resource, &Resource)>>()
        {
            assert_resources(is_res, exp_res, &pp_id);
        }
    }
}

pub fn assert_resources(is_resource: &Resource, exp_resource: &Resource, pp_id: &str) {
    assert_field(&is_resource.id, &exp_resource.id, pp_id, "resource id");
    assert_field(
        &is_resource.name,
        &exp_resource.name,
        pp_id,
        "resource name",
    );
    assert_field(
        &is_resource.version,
        &exp_resource.version,
        pp_id,
        "resource version",
    );
    assert_field(&is_resource.url, &exp_resource.url, pp_id, "resource pp_id");
    assert_field(
        &is_resource.iri_prefix,
        &exp_resource.iri_prefix,
        pp_id,
        "resource iri_prefix",
    );
    assert_field(
        &is_resource.namespace_prefix,
        &exp_resource.namespace_prefix,
        pp_id,
        "resource namespace_prefix",
    );
}

fn assert_field<T: Debug + PartialEq>(is: &T, exp: &T, pp_id: &str, field: &str) {
    assert_eq!(is, exp, "For {field} in Phenopacket {pp_id}");
}
fn assert_some<T: Debug>(is: Option<&T>, expected: Option<&T>, pp_id: &str, field: &str) {
    if is.is_some() != expected.is_some() {
        panic!(
            "Expected {:?} for field '{}', but got {:?}. Phenopacket: {}",
            expected, field, is, pp_id
        );
    }
}

pub fn sort_phenotypic_features(features: &mut Vec<PhenotypicFeature>) {
    features.sort_by(|a, b| {
        let type_cmp = match (&a.r#type, &b.r#type) {
            (Some(type_a), Some(type_b)) => type_a.id.cmp(&type_b.id),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };

        if type_cmp != std::cmp::Ordering::Equal {
            return type_cmp;
        }

        // Secondary sort: observed phenotypes before excluded ones
        let excluded_cmp = a.excluded.cmp(&b.excluded);

        if excluded_cmp != std::cmp::Ordering::Equal {
            return excluded_cmp;
        }

        // Tertiary sort: by onset
        let onset_cmp = compare_time_elements(&a.onset, &b.onset);

        if onset_cmp != std::cmp::Ordering::Equal {
            return onset_cmp;
        }

        // Quaternary sort: by modifiers (lexicographically)
        let modifiers_cmp = compare_modifier_lists(&a.modifiers, &b.modifiers);

        if modifiers_cmp != std::cmp::Ordering::Equal {
            return modifiers_cmp;
        }

        // Final sort: by description
        a.description.cmp(&b.description)
    });
}

fn compare_time_elements(a: &Option<TimeElement>, b: &Option<TimeElement>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(time_a), Some(time_b)) => {
            // Compare based on the TimeElement's internal structure
            // This assumes TimeElement has comparable fields - adjust as needed
            format!("{:?}", time_a).cmp(&format!("{:?}", time_b))
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn compare_modifier_lists(a: &[OntologyClass], b: &[OntologyClass]) -> std::cmp::Ordering {
    // First compare by length
    let len_cmp = a.len().cmp(&b.len());
    if len_cmp != std::cmp::Ordering::Equal {
        return len_cmp;
    }

    // Then compare lexicographically by modifier IDs
    for (mod_a, mod_b) in a.iter().zip(b.iter()) {
        let id_cmp = mod_a.id.cmp(&mod_b.id);
        if id_cmp != std::cmp::Ordering::Equal {
            return id_cmp;
        }
    }

    std::cmp::Ordering::Equal
}

fn get_mismatched_items<T, F, E: Debug>(
    actual: &[T],
    expected: &[T],
    id: &str,
    item_name: &str,
    extract_type: F,
) -> !
where
    F: Fn(&T) -> E,
{
    let additional_items: Vec<E> = match actual.len() > expected.len() {
        true => {
            let start = expected.len();
            actual[start..].iter().map(&extract_type).collect()
        }
        false => {
            let start = actual.len();
            expected[start..].iter().map(extract_type).collect()
        }
    };

    panic!(
        "Expected {} {} for Phenopacket {}, got {}. Extra items: {:?}",
        expected.len(),
        item_name,
        id,
        actual.len(),
        additional_items
    );
}

pub fn sort_resources_case_insensitive(resources: &mut [Resource]) {
    resources.sort_by(|a, b| a.id.to_lowercase().cmp(&b.id.to_lowercase()));
}
