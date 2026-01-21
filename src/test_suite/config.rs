pub(crate) static DATA_SOURCES_CONFIG: &[u8] = br#"
data_sources:
  - type: "csv"
    source: "./data/example.csv"
    separator: ","
    context:
      name: "TestTable"
      context:
        - identifier: "patient_id"
          header_context: subject_id
          data_context: hpo_label_or_id
          fill_missing: "Zollinger-Ellison syndrome"
          alias_map:
            hash_map:
              "null": null
              "M": "Male"
              "102": "High quantity"
              "169.5": "Very high quantity"
              "true": "smoker"
            output_dtype: String
          building_block_id: "block_1"
    extraction_config:
      name: "Sheet1"
      has_headers: true
      patients_are_rows: true

  - type: "excel"
    source: "./data/example.excel"
    contexts:
      - name: "Sheet1"
        context:
          - identifier: "lab_result_.*"
            header_context: subject_id
            data_context: hpo_label_or_id
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              hash_map:
                "neoplasma": "4"
                "height": "1.85"
              output_dtype: Float64
      - name: "Sheet2"
        context:
          - identifier:
              - "Col_1"
              - "Col_2"
              - "Col_3"
            header_context: subject_id
            data_context: hpo_label_or_id
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              hash_map:
                "smoker": "true"
              output_dtype: Boolean
    extraction_configs:
      - name: "Sheet1"
        has_headers: true
        patients_are_rows: true
      - name: "Sheet2"
        has_headers: true
        patients_are_rows: true
"#;

pub(crate) static PIPELINE_CONFIG: &[u8] = br#"
pipeline:
  transform_strategies:
    - "alias_map"
    - "multi_hpo_col_expansion"
  loader:
    file_system:
        output_dir: "some/dir"
        create_dir: true
  meta_data:
    created_by: Rouven Reuter
    submitted_by: Magnus Knut Hansen
    cohort_name: "Arkham Asylum 2025"
    hp_ref:
      version: "2025-09-01"
      prefix_id: "HP"
    unit_refs:
      - version: "2026-01-09"
        prefix_id: "UO"
  credentials:
    loinc:
      username: ${LOINC_USERNAME}
      password: ${LOINC_PASSWORD}
"#;

/// Alternative: Get the combined config as bytes
pub(crate) fn get_full_config_bytes() -> Vec<u8> {
    let data_sources =
        std::str::from_utf8(DATA_SOURCES_CONFIG).expect("Invalid UTF-8 in DATA_SOURCES_CONFIG");
    let pipeline = std::str::from_utf8(PIPELINE_CONFIG).expect("Invalid UTF-8 in PIPELINE_CONFIG");

    format!("{}\n{}", data_sources.trim(), pipeline.trim()).into_bytes()
}
