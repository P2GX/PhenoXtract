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
          - identifier: "Phenotype onset age"
            data_context:
              onset: age
          - identifier: "Diseases"
            data_context: disease
          - identifier: "Disease onset date"
            data_context: 
              onset: date
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

`pipeline`


Below, it will be made clear how both
of these are written, what fields they can have, and what format they should take.

## data_sources

The field `data_sources` is a list of data sources, which can either have `type` "excel" or "csv". `data_sources` can feature both CSV and Excel data sources. For example:

### Excel data source

### CSV data source

## pipeline