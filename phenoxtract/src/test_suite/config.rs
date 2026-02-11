pub(crate) static CSV_DATASOURCE_CONFIG_FILE: &[u8] = br#"
type: "csv"
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
"#;
pub(crate) static EXCEL_DATASOURCE_CONFIG_FILE: &[u8] = br#"
type: "excel"
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

/// Combines the configs above to create a PhenoXtract config.
pub(crate) fn get_full_config_bytes() -> Vec<u8> {
    let csv_data_source = std::str::from_utf8(CSV_DATASOURCE_CONFIG_FILE)
        .expect("Invalid UTF-8 in CSV_DATASOURCE_CONFIG_FILE");
    let excel_data_source = std::str::from_utf8(EXCEL_DATASOURCE_CONFIG_FILE)
        .expect("Invalid UTF-8 in EXCEL_DATASOURCE_CONFIG_FILE");
    let pipeline =
        std::str::from_utf8(PIPELINE_CONFIG_FILE).expect("Invalid UTF-8 in PIPELINE_CONFIG_FILE");

    fn indent(s: &str, n: usize) -> String {
        let pad = " ".repeat(n);
        s.lines()
            .map(|line| format!("{pad}{line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn list_item_with_indent(s: &str, n: usize) -> String {
        let first_pad = format!("{}- ", " ".repeat(n));
        let rest_pad = " ".repeat(n + 2);

        let mut lines = s.lines();

        let Some(first) = lines.next() else {
            return String::new();
        };

        let mut out = String::new();
        out.push_str(&format!("{first_pad}{first}"));

        for line in lines {
            out.push('\n');
            out.push_str(&format!("{rest_pad}{line}"));
        }

        out
    }

    let mut full_config = String::new();

    full_config.push_str("data_sources:\n");
    full_config.push_str(&list_item_with_indent(csv_data_source.trim(), 2));
    full_config.push('\n');

    full_config.push_str(&list_item_with_indent(excel_data_source.trim(), 2));
    full_config.push('\n');

    full_config.push_str("pipeline_config:\n");
    full_config.push_str(&indent(pipeline.trim(), 2));

    full_config.into_bytes()
}
