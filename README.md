# PhenoXtract

**PhenoXtract** is a configurable **ETL (Extract-Transform-Load) pipeline and crate** written in Rust for converting structured tabular data sources (CSV, Excel, and potentially others) into **[Phenopackets v2.0](https://phenopacket-schema.readthedocs.io/en/latest/)**.

It provides a flexible, configuration-driven approach to map clinical cohort data into standardized, ontology-aware **Phenopacket JSON objects**, ready for downstream analysis, sharing, or storage. Configuration can be supplied in **YAML, TOML, or JSON** formats.

---

## Features

- **Extract**
    - Supports CSV and Excel (`.xlsx`) files as input.
    - Handles flexible orientations: **patients as rows or patients as columns** (automatic transposition).
    - Automatic casting of column types:
        - `bool` (`true`/`false`)
        - `int`
        - `float`
        - `date` (`YYYY-MM-DD`, `DD-MM-YYYY`, `MM/DD/YYYY`, etc.)
        - `datetime` (ISO8601, RFC 822, RFC3339)
    - Regex and multi-column matching for identifiers.
    - Default or generated headers when missing.

- **Transform**
    - Context-driven table interpretation (`TableContext`, `SeriesContext`).
    - Maps raw values into Phenopacket semantic fields:
        - Subject info (ID, sex, age, living status, weight, smoker, etc.)
        - Phenotypes (`hpo_id`, `hpo_label`, `observation_status`, onset)
    - Transformation strategies such as alias mapping, where cell values are mapped to other aliases (e.g. `"M" -> "Male"`, `"smoker" -> true`, `"neoplasma" -> 4`), and strategies to find HPO synonyms of cell values.
    - Integrated with the HPO via **Ontolius** and the **HPO GitHub registry**.

- **Load**
    - Output Phenopackets (v2.0 JSON) to the filesystem (more loaders can be added later).
    - _Note: default output directory is not fixed yet -- currently determined by the `file_system` loader._

- **Configurable**
    - Single `PhenoXtractorConfig` file (YAML/TOML/JSON/RON) defines:
        - Data sources (CSV/Excel).
        - Table contexts (how to interpret columns/rows).
        - Pipeline behavior (transformation strategies, loader).
        - Meta-data for the resulting phenopackets (`created_by`, `submitted_by`, `cohort_name`).
        - `created_by` is optional and defaults to `"phenoxtract-{crate_version}"`.

- **Validation**
    - Ensures configs are well-formed.
    - Validates data against expected schema contexts.

---

## Configuration

The configuration file can be in **YAML, TOML, or JSON** format.

### Example `config.yaml`

```yaml
data_sources:
 - type: "csv"
   source: "./data/cohort.csv"
   separator: ","
   extraction_config:
     name: "patients"
     has_headers: true
     patients_are_rows: true
     context:
       name: "patient_table"
       context:
         - identifier: "patient_id"
           header_context: subject_id
           data_context: hpo_label
           alias_map:
             "M": "Male"
             "F": "Female"
             "smoker": true
             "neoplasma": 4
             "height": 1.85

pipeline:
 transform_strategies:
   - "alias_mapping"
   - "fill_null"
 loader: "file_system"

meta_data:
 # created_by is optional; defaults to "phenoxtract-{version}" if not provided
 submitted_by: "Dr. Example"
 cohort_name: "Example Cohort 2025"
```

This config defines:
- One CSV data source.
- Patients as rows, headers included.
- Maps patient_id column into subject_id and hpo_label.
- Applies transformation strategies (alias mapping, fill null).
- Saves output phenopackets to disk using the file_system loader.

---

## Getting Started

### Prerequisites
- Rust (stable toolchain recommended)
- Cargo

### Installation
Clone the repo and build:

```bash
git clone https://github.com/P2GX/phenoxtract.git
cd phenoxtract
cargo build --release
```

### Running
Currently, the binary is minimal (`main.rs` just prints "Hello, world!").
The main functionality is exposed as a **library crate** (`phenoxtract`) that you can import

Example usage in Rust:

```rust
use phenoxtract::config::phenoxtracter_config::PhenoXtractorConfig;

fn main() {
    let config = PhenoXtractorConfig::load("config.yaml".into())
        .expect("Invalid configuration");
    if let Some(pipeline_config) = config.pipeline_config() {
        let pipeline = phenoxtract::pipeline::Pipeline::from_config(&pipeline_config)
            .expect("Failed to build pipeline");
        // Run extraction + transform + load
        // pipeline.run(&mut config.data_sources()).unwrap();
   }
}
```

---

## Testing

The project includes extensive unit tests using `rstest` and `tempfile`. Tests cover:
- Loading configs from all supported formats (YAML, TOML, JSON, RON).
- Default and custom metadata.
- Extraction from CSV/Excel in both row- and column-oriented layouts.
- Auto-casting of datatypes.
- Context and alias mapping validation.

Run all tests with:

```bash
cargo nextest run --workspace --lib --all-targets --all-features
```

---

## Output

- Each patient/row in the input is transformed into a **Phenopacket JSON** object (v2.0 schema).
- Metadata (`created_by`, `submitted_by`, `cohort_name`) is automatically included.
- Files are written to the configured output directory (default: `some/dir/` in current version).

---

## Roadmap

- CLI for running pipelines directly with `phenoxtract --config config.yaml`.
- Additional loaders (e.g., database, API).
- Richer transformation strategies (beyond alias mapping and fill-null).
- Expanded ontology support (HPO synonyms, MONDO, etc.).
- Deeper integration with FAIR data standards.

---

## Authors

- Rouven Reuter
- Patrick Simon Nairne
- Peter Robinson
- Varenya Jain

---

## License

MIT - see [LICENSE](https://github.com/P2GX/PhenoXtract/blob/main/LICENSE) for details.
