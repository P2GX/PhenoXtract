pub(crate) static DATA_SOURCES_CONFIG_FILE: &[u8] = br#"
data_sources:
  - type: "csv"
    source: "./data/example.csv"
    separator: ","
    has_headers: true
    patients_are_rows: true
    contexts:
      - identifier: "patient_id"
        header_context: subject_id
        data_context: hpo_label_or_id
        fill_missing: "Zollinger-Ellison syndrome"
        building_block_id: "block_1"
        alias_map_config:
          output_data_type: String
          mappings:
            "null": null
            "M": "Male"
            "102": "High quantity"
            "169.5": "Very high quantity"
            "true": "smoker"

  - type: "excel"
    source: "./data/example.excel"
    sheets:
      - sheet_name: "Sheet1"
        has_headers: true
        patients_are_rows: true
        contexts:
          - identifier: "lab_result_.*"
            header_context: subject_id
            data_context: hpo_label_or_id
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map_config:
              output_data_type: Float64
              mappings:
                "neoplasma": "4"
                "height": "1.85"

      - sheet_name: "Sheet2"
        has_headers: true
        patients_are_rows: true
        contexts:
          - identifier:
              - "Col_1"
              - "Col_2"
              - "Col_3"
            header_context: subject_id
            data_context: hpo_label_or_id
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map_config:
              output_data_type: Boolean
              mappings:
                "smoker": "true"
"#;
pub(crate) static PIPELINE_CONFIG_FILE: &[u8] = br#"
pipeline_config:
  transform_strategies:
    - "alias_map"
    - "multi_hpo_col_expansion"
  loader:
    file_system:
        output_dir: "some/dir"
        create_dir: true
  meta_data:
    created_by: "PhenoXtract Test Suite"
    submitted_by: "Someone"
    cohort_name: "Cohort-1"
    hp_resource:
      id: "HP"
      version: "2025-09-01"
    unit_resources:
      - id: "UO"
        version: "2026-01-09"
    assay_resources:
      - id: "LOINC"
        version: "2.80"
        secrets:
            user: $LOINC_USERNAME
            password: $LOINC_PASSWORD
"#;

/// combines the DataSource config with the Pipeline config above into a PhenoXtract config.
pub(crate) fn get_full_config_bytes() -> Vec<u8> {
    let data_sources = std::str::from_utf8(DATA_SOURCES_CONFIG_FILE)
        .expect("Invalid UTF-8 in DATA_SOURCES_CONFIG");
    let pipeline =
        std::str::from_utf8(PIPELINE_CONFIG_FILE).expect("Invalid UTF-8 in PIPELINE_CONFIG");

    format!("{}\n{}", data_sources.trim(), pipeline.trim()).into_bytes()
}
