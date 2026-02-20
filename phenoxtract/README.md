# PhenoXtract

**PhenoXtract** is a configurable **ETL (Extract-Transform-Load) pipeline and crate** written in Rust for converting
tabular data sources (CSV, Excel, and potentially others)
into [Phenopackets v2.0](https://phenopacket-schema.readthedocs.io/en/latest/). The config can be written in  **YAML,
TOML, or JSON** formats. For an explanation of how to write a config.yaml, see here: [YAML_README](YAML_README.md).

## How PhenoXtract works

PhenoXtract begins by extracting the data sources into a [Polars](https://docs.rs/polars/latest/polars/) Dataframes. In
the config file, the user will have specified which Phenopacket elements each column of the data corresponds to.

Once the data has been extracted, "Strategies" are applied, which transform the data into a format that the user can
understand. See here for a list of all current strategies: [Strategies](README.md#strategies). The user can decide
which strategies should be applied in the config file.

After strategies have been applied, the "Collection" stage of the program begins. PhenoXtract creates Phenopackets for
each patient in the data, and then goes through the data cell-by-cell and inserts the data into the correct Phenopacket.

Finally, the Phenopackets are loaded to .json files in a directory of the user's choice.

## What format does PhenoXtract expect data to be in?

Before the Collection stage of the program, the data must be in a certain format so that it can be understood by
PhenoXtract. How each column should look will be explained in the sections:

* [Extracting Individual Data](#extracting-individual-data)
* [Extracting Phenotypes](#extracting-phenotypes)
* [Extracting Diseases](#extracting-diseases)
* [Extracting Interpretations](#extracting-interpretations)
* [Extracting Measurements](#extracting-measurements)
* [Extracting Medical Actions](#extracting-medical-actions)

## Running PhenoXtract:

Once a config file has been written (see [YAML_README](YAML_README.md) for information on how to write a config.yaml),
PhenoXtract can be run as follows:

```rust
use std::path::PathBuf;
use phenoxtract::phenoxtract::Phenoxtract;

fn test_i_data() {
    let config_path = PathBuf::from(
        "path/to/config.yaml",
    );
    let mut phenoxtract = Phenoxtract::try_from(config_path).unwrap();
    phenoxtract.run().unwrap();
}
```

## Extracting Individual Data

(TODO)

## Extracting Phenotypes

(TODO)

## Extracting Diseases

(TODO)

## Extracting Interpretations

(TODO)

## Extracting Measurements

(TODO)

## Extracting Medical Actions

(TODO)

## Contexts

In order for PhenoXtract to understand what is inside a column, the user must specify a "Series Context" for that
column. For each Series Context, the user can specify a `header_context`, which describes what is in the header of the
column, and a `data_context` which describes what is in the cells of the column. How one configures a Series Context for
a column (or multiple) is described in [YAML_README](YAML_README.md).

Here is the list of possible values that `header_context` or `data_context` can take:

**Individual data**

- SubjectId
- SubjectSex
- DateOfBirth
- VitalStatus
- TimeAtLastEncounter(TimeElementType)
- TimeOfDeath(TimeElementType)
- CauseOfDeath
- SurvivalTimeDays

**Phenotypes and Disease**

- Hpo
- Disease
- MultiHpoId
- Onset(TimeElementType)

**Genetics**

- Hgvs
- Hgnc

**Measurements**

- QuantitativeMeasurement (assay_id: String, unit_ontology_id: String)
- QualitativeMeasurement (assay_id: String)
- TimeOfMeasurement(TimeElementType)
- ReferenceRange(Boundary)

**Medical Actions**

- TreatmentTarget
- TreatmentIntent
- ResponseToTreatment
- TreatmentTerminationReason
- ProcedureLabelOrId
- ProcedureBodySite
- TimeOfProcedure(TimeElementType)

- ObservationStatus
- None

In the above, TimeElementType can currently be one of

- Date
- Age

and Boundary can be one of

- Lower
- Upper

(TODO: Make clearer what the list above means)

## Strategies

Here is a list of the strategies currently supported by PhenoXtract:

- AgeToIso8601
- AliasMap
- DateToAge
- Mapping
- MultiHpoColExpansion
- OntologyNormaliser

(TODO: Documentation for each Strategy)

## Authors

- Rouven Reuter
- Patrick Simon Nairne
- Adam Graefe
- Varenya Jain
- Peter Robinson