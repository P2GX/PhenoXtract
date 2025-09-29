# PhenoXtract

**PhenoXtract** is a configurable **ETL (Extract-Transform-Load) pipeline and crate** written in Rust for converting structured tabular data sources (CSV, Excel, and potentially others) into **[Phenopackets v2.0](https://phenopacket-schema.readthedocs.io/en/latest/)**.

It provides a flexible configuration-driven approach to map clinical cohort data into standardized, ontology-aware **Phenopacket JSON objects**, ready for downstream analysis, sharing, or storage.

---

## Features

- **Extract**
 - Supports CSV and Excel (`.xlsx`) files as input.
 - Handles flexible orientations (patients as rows or columns).
 - Automatic casting of column types (bools, ints, floats, dates, datetimes).
 - Regex and multi-column matching for identifiers.
 - Default or generated headers when missing.

- **Transform**
 - Context-driven table interpretation (`TableContext`, `SeriesContext`).
 - Maps raw values into Phenopacket semantic fields (HPO IDs, subject sex, ages, etc.).
 - Transformation strategies such as alias mapping, where cell values are mapped to other aliases (e.g. "M" to "Male"), and strategies to find HPO synonyms of cell values.
 - Integrated with the HPO via **Ontolius** and the **HPO GitHub registry**.

- **Load**
 - Output Phenopackets (v2.0 JSON) to the filesystem (more loaders can be added later).

- **Configurable**
 - Single `PhenoXtractorConfig` YAML/TOML/JSON/RON file defines:
 - Data sources (CSV/Excel).
 - Table contexts (how to interpret columns/rows).
 - Pipeline behavior (transformation strategies, loader).
 - Meta-data for the resulting phenopackets (e.g., `created_by`, `submitted_by`, `cohort_name`).

- **Validation**
 - Ensures configs are well-formed.
 - Validates data against expected schema contexts.

---

## Getting Started

### Prerequisites
- Rust (2024 edition, stable toolchain recommended).
- Cargo (comes with Rust).

### Installation
Clone the repo and build:

```bash
git clone https://github.com/P2GX/phenoxtract.git
cd phenoxtract
cargo build --release
```

### Running
Currently, the binary is minimal (`main.rs` just prints "Hello, world!").
The main functionality is exposed as a **library crate** (`phenoxtract`) that you can import or drive via configuration.

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

## Configuration

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

pipeline:
 transform_strategies:
 - "alias_mapping"
 - "fill_null"
 loader: "file_system"

meta_data:
 submitted_by: "Dr. Example"
 cohort_name: "Example Cohort 2025"
```

This config defines:
- One CSV data source.
- Patients as rows, headers included.
- Maps `patient_id` column into `subject_id` and `hpo_label`.
- Applies transformation strategies.
- Saves output phenopackets to disk.

---

## Output

- Each patient/row in the input is transformed into a **Phenopacket JSON** object (v2.0 schema).
- Metadata (`created_by`, `submitted_by`, `cohort_name`) is automatically included.
- Files are written to the configured output directory (default: `some/dir/` in current version).

---

## Testing

The project includes extensive **unit tests** using [`rstest`](https://crates.io/crates/rstest).

Run tests with:

```bash
cargo nextest run --workspace --lib --all-targets --all-features

---

## Resources

- [Phenopackets Schema v2.0](https://phenopacket-schema.readthedocs.io/en/latest/)
- [HPO (Human Phenotype Ontology)](https://hpo.jax.org/)
- [Polars DataFrame library](https://www.pola.rs/)

---

## Roadmap

- CLI for running pipelines directly with `phenoxtract --config config.yaml`.
- Additional loaders (e.g., database, API).
- Richer transformation strategies.
- Expanded ontology support.
- Integration with FAIR data standards.

---

## Authors

- Rouven Reuter
- Peter Robinson
- Patrick Simon Nairne

---

## License

MIT -- see [LICENSE](LICENSE) for details.
