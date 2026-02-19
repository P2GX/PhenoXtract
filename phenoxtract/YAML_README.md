# Writing a PhenoXtract config.yaml file

In this README it will be explained how to write a `config.yaml` file for PhenoXtract. 

#TODO: Contents!

Suppose we have the following Excel Workbook with two sheets called `Basic Info` and `Phenotypes and Diseases`.

![Basic Info Sheet](readme_assets/basic_info_sheet.png)

![Phenotypes and Diseases Sheet](readme_assets/phenotypes_and_diseases.png)

Here is what a typical PhenoXtract `config.yaml` file for this data might look like:

```yaml
data_sources:
  - type: "excel"
    source: "./data/example.xlsx"
    sheets:
      - sheet_name: "Basic Info"
        has_headers: true
        patients_are_rows: true
        series_contexts:
          - identifier: "Patient ID"
            header_context: subject_id
          - identifier: "Year of birth"
            data_context: date_of_birth
          - identifier: "Sex"
            data_context: subject_sex
            alias_map:
              output_data_type: String
              mappings:
                "M": "Male"
                "F": "Female"
                "No data": null            
      - sheet_name: "Phenotypes and Diseases"
        has_headers: true
        patients_are_rows: true
        series_contexts:
          - identifier: "Patient ID"
            header_context: subject_id
          - identifier: "Phenotypes"
            data_context: hpo
            building_block_id: "P"
          - identifier: "Phenotype onset age"
            data_context:
              onset: age
            building_block_id: "P"
          - identifier: "Diseases"
            data_context: disease
            building_block_id: "D"
          - identifier: "Disease onset date"
            data_context: 
              onset: date
            building_block_id: "D"
pipeline:
  strategies:
    - "alias_map"
    - "date_to_age"
    - "age_to_iso8601"
  loader:
    file_system:
      output_dir: "./data/phenopackets"
      create_dir: true
  meta_data:
    created_by: "someone"
    submitted_by: "someone"
    cohort_name: "my_cohort"
    hpo_resource:
      id: "HP"
      version: "2025-09-01"
    disease_resources:
      - id: "OMIM"
        secrets:
          user: "my_bioportal_username"
          password: "my_bioportal_password"
```

Not all data needs to be extracted; note that the "Name" column in the data is ignored by this config.yaml.

## Overview

The `config.yaml` has two main sections: `data_sources` and `pipeline`. 

`data_sources` contains information on the tabular data (which may be CSV or Excel) being extracted. The user must input
the location of the file, and specify whether the table has headers, and whether rows or columns correspond to patients.
For each table, the user must specify a list of `series_contexts`. A Series Context is an association between a column
in the data, and a concept that PhenoXtract can understand. The two most important fields of a Series Context are
`identifier` and `data_context`. For example, this:

```yaml
          - identifier: "Year of birth"
            data_context: date_of_birth
```

tells PhenoXtract that the data in the column named "Year of birth" corresponds to the concept `DateOfBirth` that is
understood by PhenoXtract. See [CONTEXTS TODO] below for a list of all contexts understood by PhenoXtract.

`pipeline` consists of three fields `strategies`, `loader` and `meta_data`. `strategies` is a list of Strategies, 
which are applied to the data before any Collection occurs (see [HOW PHENOX WORKS]). `loader` specifies how the
extracted Phenopackets should be outputted; currently the only option is `file_system`. In the `meta_data` field, 
the user can input details about themselves and the cohort (this data will be put into the MetaData [link] section of 
the Phenopackets), and also which resources they would like to use for processing ontology and database classes. There
are currently five types of resource that can be specified: 

- a `hpo_resource`
- a list of `disease_resources` (for example OMIM and MONDO)
- a list of `assay_resources` (for example LOINC)
- a list of `unit_resources` (for example UO)
- a list of `qualitative_measurement_resources` (for example PATO)

If the user only has phenotype and disease data, then only `hpo_resource` and `disease_resources` are relevant. The 
resources provided by the user are used by PhenoXtract in order to validate the data, and to find labels corresponding
to IDs in the data, and vice-versa. The resources are either downloaded locally, or otherwise an API is used.

## data_sources

The field `data_sources` is a list of data sources, which can either have `type` "excel" or "csv". `data_sources` can 
feature both CSV and Excel data sources. For example:

```yaml
data_sources:
- type: "excel"
  source: "./data/example.xlsx"
  sheets:
    - sheet_name: "Basic Info"
      has_headers: true
      patients_are_rows: true
      series_contexts:
        - identifier: "Patient ID"
          header_context: subject_id
        - identifier: "Year of birth"
          data_context: date_of_birth
        - sheet_name: "Phenotypes"
          has_headers: true
          patients_are_rows: true
          series_contexts:
            - identifier: "Patient ID"
              header_context: subject_id
            - identifier: "Phenotypes"
              data_context: hpo
- type: "csv"
  source: "./data/example.csv"
  separator: ","
  has_headers: true
  patients_are_rows: true
  series_contexts:
    - identifier: "Patient ID"
      header_context: subject_id
    - identifier: "Diseases"
      data_context: disease
```

would be the `data_sources` entries for data consisting of an Excel Workbook with two sheets, and a CSV file.

### Excel data source

An Excel data source has three fields: `type` (which is always "excel"), `source` (the path to the .xlsx file) and 
`sheets` which is a list. The config for each sheet has the fields `sheet_name`, `has_headers`, `patients_are_rows`
and `series_contexts` which is a list of configs for a Series Context. More detail on the concepts will be provided
below.

### CSV data source

An CSV data source has six fields: `type` (which is always "csv"), `source` (the path to the .csv file), `separator`,
`has_headers`, `patients_are_rows` and `series_contexts`.

### has_headers

This is either `true` or `false`. If it is `true`, each column has a name in the data. The `identifier` of the corresponding
Series Context must be the same as this. If `has_headers` is `false` then these columns will be indexed as integers 
starting from 0. In this case, the `identifier` of the Series Context must be the corresponding integer.

### patients_are_rows

This is either `true` or `false`. If it is `true` then each row of the data corresponds to a single patient. If it is
`false` then each column of the data corresponds to a single patient (for example, if the first row of the data
contained just Patient IDs).

### series_contexts

`series_contexts` is a list of configs for a Series Context. The context for a Series Context consists of 



## pipeline