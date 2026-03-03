# PhenoXtract

**PhenoXtract** is a configurable **ETL (Extract-Transform-Load) pipeline and crate** written in Rust for converting
tabular data sources (e.g. CSV or Excel)
into [Phenopackets v2.0](https://phenopacket-schema.readthedocs.io/en/latest/). The config can be written in  **YAML,
TOML, or JSON** formats. For an explanation of how to write a config.yaml, see
here: [YAML_README](CONFIG_YAML_README.md).

<!-- TOC -->

* [PhenoXtract](#phenoxtract)
    * [How PhenoXtract works](#how-phenoxtract-works)
    * [What format does PhenoXtract expect data to be in?](#what-format-does-phenoxtract-expect-data-to-be-in)
    * [Running PhenoXtract in Rust](#running-phenoxtract-in-rust)
    * [Extracting Individual Data](#extracting-individual-data)
    * [Extracting Phenotypes](#extracting-phenotypes)
    * [Extracting Diseases](#extracting-diseases)
    * [Extracting Interpretations](#extracting-interpretations)
    * [Extracting Measurements](#extracting-measurements)
    * [Extracting Medical Actions](#extracting-medical-actions)
    * [Contexts](#contexts)
    * [Strategies](#strategies)
    * [Authors](#authors)

<!-- TOC -->

## How PhenoXtract works

PhenoXtract begins by extracting the data sources into a [Polars](https://docs.rs/polars/latest/polars/) Dataframes. In
the config file, the user will have specified which Phenopacket elements each column of the data corresponds to. This is
done by providing a `SeriesContext` for each column. See [Contexts](#contexts)
and [series_contexts](CONFIG_YAML_README.md#series_contexts) for more information on Series Contexts.

Once the data has been extracted, `Strategies` are applied, which transform the data into a format that the application
can
understand. See here for a list of all current strategies: [Strategies](README.md#strategies). The user can decide
which strategies should be applied in the config file.

After strategies have been applied, the `Collection` stage of the program begins. PhenoXtract creates Phenopackets for
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

## Running PhenoXtract in Rust

Once a config file has been written (see [YAML_README](CONFIG_YAML_README.md) for information on how to write a
config.yaml),
PhenoXtract can be run as follows:

```rust
use std::path::PathBuf;
use phenoxtract::phenoxtract::Phenoxtract;

fn main() -> Result<(), PipelineError> {
    let config_path = PathBuf::from(
        "path/to/config.yaml",
    );
    let mut phenoxtract = Phenoxtract::try_from(config_path).unwrap();
    phenoxtract.run()?;
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

In order for PhenoXtract to understand what is inside a column, the user must specify a `SeriesContext` for that
column. For each `SeriesContext`, the user can specify a `header_context`, which describes what is in the header of the
column, and a `data_context` which describes what is in the cells of the column. How one configures a `SeriesContext`
for
a column (or multiple) is described in [YAML_README](CONFIG_YAML_README.md).

Here is the list of possible values that `header_context` or `data_context` can take:

**Individual data**

- subject_id
- subject_sex
- date_of_birth
- vital_status
- time_at_last_encounter: time_element_type
- time_of_death: time_element_type
- cause_of_death
- survival_time_days

**Phenotypes and Disease**

- hpo
- disease
- multi_hpo_id
- onset: time_element_type

**Genetics**

- hgvs
- hgnc

**Measurements**

- quantitative_measurement (assay_id: String, unit_ontology_id: String)
- qualitative_measurement (assay_id: String)
- time_of_measurement: time_element_type
- reference_range: boundary

**Medical Actions**

- treatment_target
- treatment_intent
- response_to_treatment
- treatment_termination_reason
- procedure
- procedure_body_site
- time_of_procedure: time_element_type

- observation_status
- None

In the above, TimeElementType can currently be one of

- date
- age

and Boundary can be one of

- lower
- upper

(TODO: Make clearer what the list above means)

## Strategies

Here is a list of the strategies currently supported by PhenoXtract:

#### age_to_iso8601

Given a column whose cells contains ages (e.g. subject age, age of death, age of onset) this strategy converts integer
entries to ISO8601 durations: 47 -> P47Y
NOTE: the integers must be between 0 and 150.

If an entry is already in ISO8601 duration format, it will be left unchanged.
If there are cell values which are neither ISO8601 durations nor integers an error will be returned.

#### alias_map

This strategy will apply all the aliases found in the `SeriesContexts`.
For example if a ContextualisedDataframe has a SeriesContext consisting of a SubjectSex column and a ToString AliasMap
which converts "M" to "Male" and "F" to "Female" then the strategy will apply those aliases to each cell.

NOTE

- This does not transform the headers of the Dataframe.
- Only non-null cells may be aliased.
- Non-null cells may be aliased to null

#### date_to_age

This strategy finds columns whose cells contain dates, and converts these dates to a certain age of the patient, by
leveraging the patient's date of birth.

If there is no data on a certain patient's date of birth, yet there is a date corresponding to this patient, then an
error will be thrown.

#### mapping

A strategy for mapping string values to standardized terms using a synonym dictionary.

`MappingStrategy` transforms data by replacing cell values with their corresponding mapped values from a synonym map.
It's commonly used for data normalization tasks such as standardizing gender/sex values, categorical data, or controlled
vocabulary.

#### multi_hpo_col_expansion

A strategy for converting columns whose cells contain HPO IDs into several columns whose headers are exactly those HPO
IDs and whose cells contain the ObservationStatus for each patient.
The columns are created on a "block by block" basis so that building blocks are preserved after the transformation.
A new SeriesContext will be added for each block of new columns. The old columns and contexts will be removed.

#### ontology_normaliser

A strategy that converts ontology labels in cells (or synonyms of them) to the corresponding IDs. It is
case-insensitive.

This strategy processes string columns in data tables by looking up values in an ontology bidirectional dictionary and
replacing labels with their corresponding IDs. It only operates on columns that have no header context and match the
specified data context.

#### hpo_disease_splitter_strategy

This strategy will find every column whose context is hpo_or_disease and split it into two separate columns: a Hpo
column and a disease column.

Hpo is prioritised: the strategy will find all Hpo labels and IDs, and then put them into the
Hpo column. All other cells will be assumed to refer to disease.

## Authors

- Rouven Reuter
- Patrick Simon Nairne
- Adam Graefe
- Varenya Jain
- Peter Robinson