use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;
use chrono::{NaiveDate, NaiveDateTime};
use polars::io::SerReader;
use polars::prelude::{CsvReadOptions, DataFrame, DataType, NamedFrom, Series};
use std::fs::File;
use std::io::BufReader;

use crate::extract::error::ExtractionError;
use crate::extract::excel_data_source::ExcelDatasource;
use crate::extract::traits::Extractable;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use crate::extract::excel_range_reader::ExcelRangeReader;
use crate::extract::utils::generate_default_column_names;
use calamine::{Reader, Xlsx, open_workbook};
use either::Either;

use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use std::sync::Arc;
use validator::{Validate, ValidationErrors};

/// An enumeration of all supported data source types.
///
/// This enum uses a `tag` to differentiate between source types (e.g., "csv", "excel")
/// when deserializing from a configuration file.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum DataSource {
    Csv(CSVDataSource),
    Excel(ExcelDatasource),
}

impl Validate for DataSource {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            DataSource::Csv(csv) => csv.validate()?,
            DataSource::Excel(excel) => excel.validate()?,
        };
        Ok(())
    }
}
impl DataSource {
    fn conditional_transpose(
        mut cdf: ContextualizedDataFrame,
        patients_are_rows: &bool,
        has_header: &bool,
    ) -> Result<ContextualizedDataFrame, ExtractionError> {
        if !patients_are_rows {
            let mut column_names = None;

            if *has_header {
                // Assuming, that the headers are in the first column of the dataframe
                let index_column =
                    cdf.data
                        .get_columns()
                        .first()
                        .ok_or(ExtractionError::VectorIndexing(
                            "No columns in DataFrame".to_string(),
                        ))?;

                column_names = Some(Either::Right(index_column
                .str()
                .into_iter()
                .flatten()
                .map(|s| s.expect("Unable to cast column name into string, when transposing DataFrame. If your data is oriented horizontally make sure the identifiers are located in the first column.").to_string())
                .collect()));

                cdf.data = cdf.data.drop(index_column.name())?;
            }
            cdf.data = cdf.data.transpose(None, column_names.clone())?;
        }

        Ok(cdf)
    }
    //TODO: Probably move this to transform later.
    fn polars_column_string_cast(data: &mut DataFrame) -> Result<(), ExtractionError> {
        let col_names: Vec<String> = data
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        for col_name in col_names {
            let column = data.column(col_name.as_str())?;

            debug!("Try casting Column: {col_name}");
            if column.dtype() != DataType::String.as_ref() {
                debug!("Skipped column {col_name}. Not of string type.");
                continue;
            }

            if let Some(bools) = column
                .str()?
                .iter()
                .map(|opt| {
                    opt.as_ref().and_then(|s| match s.to_lowercase().as_str() {
                        "true" => Some(true),
                        "false" => Some(false),
                        _ => None,
                    })
                })
                .collect::<Option<Vec<bool>>>()
            {
                debug!("Casted column: {col_name} to bool.");
                let s: Series = Series::new(col_name.clone().into(), bools);
                data.replace(col_name.as_str(), s.clone())?;
                continue;
            }

            if let Ok(mut casted) = column.strict_cast(&DataType::Int64) {
                debug!("Casted column: {col_name} to i64.");
                data.replace(
                    col_name.as_str(),
                    casted.into_materialized_series().to_owned(),
                )?;
                continue;
            }

            if let Ok(mut casted) = column.strict_cast(&DataType::Float64) {
                debug!("Casted column: {col_name} to f64.");
                data.replace(
                    col_name.as_str(),
                    casted.into_materialized_series().to_owned(),
                )?;
                continue;
            }

            if let Some(dates) = column
                .str()?
                .iter()
                .map(|s| s.and_then(try_parse_string_date))
                .collect::<Option<Vec<NaiveDate>>>()
            {
                debug!("Casted column: {col_name} to date.");
                let s: Series = Series::new(col_name.clone().into(), dates);
                data.replace(col_name.as_str(), s.clone())?;
                continue;
            }

            if let Some(dates) = column
                .str()?
                .iter()
                .map(|s| s.and_then(try_parse_string_datetime))
                .collect::<Option<Vec<NaiveDateTime>>>()
            {
                debug!("Casted column: {col_name} to datetime.");
                let s: Series = Series::new(col_name.clone().into(), dates);
                data.replace(col_name.as_str(), s.clone())?;
                continue;
            }
        }
        Ok(())
    }
}

impl Extractable for DataSource {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, ExtractionError> {
        match self {
            DataSource::Csv(csv_source) => {
                info!(
                    "Attempting to extract CSV data from: {}",
                    csv_source.source.display()
                );

                let mut csv_read_options = CsvReadOptions::default().with_has_header(
                    csv_source.extraction_config.patients_are_rows
                        && csv_source.extraction_config.has_headers,
                );

                if let Some(sep) = csv_source.separator {
                    let new_parse_options = (*csv_read_options.parse_options)
                        .clone()
                        .with_separator(sep as u8);
                    csv_read_options.parse_options = Arc::from(new_parse_options);
                }
                let csv_data = csv_read_options
                    .try_into_reader_with_file_path(Some(csv_source.source.clone()))?
                    .finish()?;

                let mut cdf = DataSource::conditional_transpose(
                    ContextualizedDataFrame::new(csv_source.context.clone(), csv_data),
                    &csv_source.extraction_config.patients_are_rows,
                    &csv_source.extraction_config.has_headers,
                )?;

                if !csv_source.extraction_config.has_headers {
                    let default_column_names =
                        generate_default_column_names(cdf.data.width() as i64);
                    let current_column_names: Vec<String> = cdf
                        .data
                        .get_column_names()
                        .iter()
                        .map(|s| s.to_string())
                        .collect();

                    for (col_name, new_col_name) in
                        current_column_names.iter().zip(default_column_names)
                    {
                        cdf.data.rename(col_name.as_str(), new_col_name.into())?;
                    }
                }

                DataSource::polars_column_string_cast(&mut cdf.data)?;
                info!("Extracted CSV data from {}", csv_source.source.display());
                Ok(vec![cdf])
            }
            DataSource::Excel(excel_source) => {
                let mut cdf_vec = Vec::new();

                info!(
                    "Attempting to extract Excel data from: {}",
                    excel_source.source.display()
                );

                let mut workbook: Xlsx<BufReader<File>> =
                    open_workbook(excel_source.source.clone())?;

                let extraction_configs = &excel_source.extraction_configs;

                if extraction_configs.len() < workbook.sheet_names().len() {
                    warn!("Warning: fewer ExtractionConfigs than Excel Worksheets.");
                } else if extraction_configs.len() > workbook.sheet_names().len() {
                    warn!("Warning: more ExtractionConfigs than Excel Worksheets.");
                }

                for extraction_config in extraction_configs {
                    let sheet_name = &extraction_config.name;
                    let sheet_context = excel_source
                        .contexts
                        .iter()
                        .find(|context| &context.name == sheet_name)
                        .ok_or(ExtractionError::UnableToFindTableContext(format!(
                            "Can't find table context with name {sheet_name}"
                        )))?;

                    let range = match workbook.worksheet_range(sheet_name) {
                        Ok(r) => r,
                        Err(_) => {
                            warn!(
                                "Could not find Excel Worksheet with the name {sheet_name}! No dataframe extracted."
                            );
                            continue;
                        }
                    };

                    let excel_range_reader =
                        ExcelRangeReader::new(range, extraction_config.clone());

                    let sheet_data = excel_range_reader.extract_to_df()?;

                    let mut cdf = ContextualizedDataFrame::new(sheet_context.clone(), sheet_data);
                    DataSource::polars_column_string_cast(&mut cdf.data)?;
                    cdf_vec.push(cdf);
                    info!(
                        "Extracted data from Excel Worksheet {} in Excel Workbook {}",
                        sheet_name,
                        excel_source.source.display()
                    );
                }

                Ok(cdf_vec)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::Identifier::Regex;
    use crate::config::table_context::{Context, SeriesContext, TableContext};
    use crate::extract::extraction_config::ExtractionConfig;
    use polars::df;
    use polars::prelude::DataFrame;
    use polars::prelude::TimeUnit;
    use rstest::{fixture, rstest};
    use rust_xlsxwriter::{ColNum, ExcelDateTime, Format, IntoCustomDateTime, RowNum, Workbook};
    use std::f64;
    use std::fmt::Write;
    use std::fs::File;
    use std::io::Write as StdWrite;
    use tempfile::TempDir;

    #[fixture]
    fn column_names() -> [&'static str; 4] {
        ["patient_id", "hpo_id", "disease_id", "sex"]
    }
    #[fixture]
    fn patient_ids() -> [&'static str; 4] {
        ["P001", "P002", "P003", "P004"]
    }

    #[fixture]
    fn hpo_ids() -> [&'static str; 4] {
        ["HP:0000054", "HP:0000046", "HP:0000040", "HP:0030265"]
    }

    #[fixture]
    fn disease_ids() -> [&'static str; 4] {
        [
            "MONDO:100100",
            "MONDO:100200",
            "MONDO:100300",
            "MONDO:100400",
        ]
    }

    #[fixture]
    fn subject_sexes() -> [&'static str; 4] {
        ["Male", "Female", "Male", "Female"]
    }

    #[fixture]
    fn row_names() -> [&'static str; 4] {
        ["time_of_birth", "age", "weight", "smokes"]
    }

    #[fixture]
    fn times_of_birth() -> [ExcelDateTime; 4] {
        [
            ExcelDateTime::from_ymd(1960, 1, 25)
                .unwrap()
                .and_hms(12, 30, 5)
                .unwrap(),
            ExcelDateTime::from_ymd(2020, 4, 28)
                .unwrap()
                .and_hms(23, 11, 15)
                .unwrap(),
            ExcelDateTime::from_ymd(1928, 11, 9)
                .unwrap()
                .and_hms(15, 32, 13)
                .unwrap(),
            ExcelDateTime::from_ymd(1998, 10, 4)
                .unwrap()
                .and_hms(11, 59, 59)
                .unwrap(),
        ]
    }

    #[fixture]
    fn ages() -> [i32; 4] {
        [41, 29, 53, 101]
    }

    #[fixture]
    fn weights() -> [f64; 4] {
        [100.5, 70.3, 95.8, 40.2]
    }

    #[fixture]
    fn smoker_bools() -> [bool; 4] {
        [false, true, false, true]
    }

    #[fixture]
    fn csv_data(
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
    ) -> Vec<u8> {
        let mut csv_content = column_names.join(",") + "\n";

        for i in 0..patient_ids.len() {
            writeln!(
                &mut csv_content,
                "{},{},{},{}",
                patient_ids[i], hpo_ids[i], disease_ids[i], subject_sexes[i]
            )
            .unwrap();
        }

        csv_content.into_bytes()
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn extraction_config_headers_patients_in_rows() -> ExtractionConfig {
        ExtractionConfig::new("first_sheet".to_string(), true, true)
    }

    #[fixture]
    fn extract_config_headers_patients_in_columns() -> ExtractionConfig {
        ExtractionConfig::new("second_sheet".to_string(), true, false)
    }

    #[fixture]
    fn extraction_config_no_headers_patients_in_rows() -> ExtractionConfig {
        ExtractionConfig::new("third_sheet".to_string(), false, true)
    }
    #[fixture]
    fn extraction_config_no_headers_patients_in_columns() -> ExtractionConfig {
        ExtractionConfig::new("fourth_sheet".to_string(), false, false)
    }
    #[fixture]
    fn table_context_column_wise_header() -> TableContext {
        TableContext::new(
            "first_sheet".to_string(),
            vec![SeriesContext::new(
                Regex("patient_id".to_string()),
                Context::None,
                Context::None,
                None,
                None,
                Some("Block_1".to_string()),
            )],
        )
    }

    #[fixture]
    fn table_context_row_wise_header() -> TableContext {
        TableContext::new(
            "second_sheet".to_string(),
            vec![SeriesContext::new(
                Regex("age".to_string()),
                Context::None,
                Context::None,
                None,
                None,
                Some("Block_2".to_string()),
            )],
        )
    }

    #[fixture]
    fn table_context_column_wise_no_header(
        table_context_column_wise_header: TableContext,
    ) -> TableContext {
        let mut test_tc3 = table_context_column_wise_header.clone();
        test_tc3.name = "third_sheet".to_string();
        test_tc3
    }

    #[fixture]
    fn table_context_row_wise_no_header(
        table_context_row_wise_header: TableContext,
    ) -> TableContext {
        let mut test_tc4 = table_context_row_wise_header.clone();
        test_tc4.name = "fourth_sheet".to_string();
        test_tc4
    }

    #[fixture]
    fn table_contexts(
        table_context_column_wise_header: TableContext,
        table_context_row_wise_header: TableContext,
        table_context_column_wise_no_header: TableContext,
        table_context_row_wise_no_header: TableContext,
    ) -> Vec<TableContext> {
        vec![
            table_context_column_wise_header,
            table_context_row_wise_header,
            table_context_column_wise_no_header,
            table_context_row_wise_no_header,
        ]
    }

    #[fixture]
    fn extraction_configs(
        extraction_config_headers_patients_in_rows: ExtractionConfig,
        extract_config_headers_patients_in_columns: ExtractionConfig,
        extraction_config_no_headers_patients_in_rows: ExtractionConfig,
        extraction_config_no_headers_patients_in_columns: ExtractionConfig,
    ) -> Vec<ExtractionConfig> {
        vec![
            extraction_config_headers_patients_in_rows,
            extract_config_headers_patients_in_columns,
            extraction_config_no_headers_patients_in_rows,
            extraction_config_no_headers_patients_in_columns,
        ]
    }

    #[allow(clippy::too_many_arguments)]
    #[rstest]
    fn test_extract_csv(
        temp_dir: TempDir,
        csv_data: Vec<u8>,
        table_context_column_wise_header: TableContext,
        extraction_config_headers_patients_in_rows: ExtractionConfig,
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
    ) {
        let file_path = temp_dir.path().join("csv_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(csv_data.as_slice()).unwrap();

        let data_source = DataSource::Csv(CSVDataSource::new(
            file_path,
            Some(','),
            table_context_column_wise_header.clone(),
            extraction_config_headers_patients_in_rows.clone(),
        ));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().expect("No data");

        assert_eq!(context_df.context(), &table_context_column_wise_header);

        let expected_data: [&[&str]; 4] = [&patient_ids, &hpo_ids, &disease_ids, &subject_sexes];
        let extracted_data = context_df.data;

        let expected_data_pairs: Vec<(&str, &[&str])> = column_names
            .iter()
            .zip(expected_data.iter())
            .map(|(&col_name, &col_data)| (col_name, col_data))
            .collect();

        for (col_name, expected_values) in expected_data_pairs.iter() {
            let extracted_col = extracted_data
                .column(col_name)
                .expect("Failed to load column")
                .str()
                .unwrap();

            for (i, extracted_value) in extracted_col.iter().enumerate() {
                assert_eq!(expected_values[i], extracted_value.unwrap());
            }
        }
    }

    #[rstest]
    fn test_extract_csv_no_heads_patients_in_columns(
        temp_dir: TempDir,
        extraction_config_no_headers_patients_in_columns: ExtractionConfig,
    ) {
        let test_data = r#"
PID_1,PID_2,PID_3
54,55,56
M,F,M
18,27,89"#;

        let table_context = TableContext::default();
        let file_path = temp_dir.path().join("test_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(test_data.as_bytes()).unwrap();

        let data_source = DataSource::Csv(CSVDataSource::new(
            file_path,
            Some(','),
            table_context.clone(),
            extraction_config_no_headers_patients_in_columns.clone(),
        ));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().expect("No data");
        assert_eq!(context_df.context(), &table_context);

        let expected_df = df![
            "0" => &["PID_1", "PID_2", "PID_3"],
            "1" => &[54, 55, 56],
            "2" => &["M", "F", "M"],
            "3" => &[18, 27, 89]
        ]
        .unwrap();
        assert_eq!(expected_df, context_df.data)
    }

    #[rstest]
    fn test_extract_csv_no_headers_patients_in_rows(
        temp_dir: TempDir,
        extraction_config_no_headers_patients_in_rows: ExtractionConfig,
    ) {
        let test_data = br#"
PID_1,54,M,18
PID_2,55,F,27
PID_3,56,M,89"#;

        let table_context = TableContext::default();
        let file_path = temp_dir.path().join("test_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(test_data).unwrap();

        let data_source = DataSource::Csv(CSVDataSource::new(
            file_path,
            Some(','),
            table_context.clone(),
            extraction_config_no_headers_patients_in_rows.clone(),
        ));

        let mut data_frames = data_source.extract().unwrap();
        let cdf = data_frames.pop().expect("No data");
        assert_eq!(cdf.context(), &table_context);

        let expected_df: DataFrame = df![
            "0" => &["PID_1", "PID_2", "PID_3"],
            "1" => &[54, 55,56],
            "2" => &["M", "F", "M"],
            "3" => &[18, 27, 89]
        ]
        .unwrap();

        assert_eq!(expected_df, cdf.data);
    }

    #[rstest]
    fn test_extract_csv_headers_patients_in_rows(
        temp_dir: TempDir,
        extraction_config_headers_patients_in_rows: ExtractionConfig,
    ) {
        let test_data = br#"
Patient_IDs,HPO_IDs,SEX,AGE
PID_1,54,M,18
PID_2,55,F,27
PID_3,56,M,89"#;

        let table_context = TableContext::default();
        let file_path = temp_dir.path().join("test_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(test_data).unwrap();

        let data_source = DataSource::Csv(CSVDataSource::new(
            file_path,
            Some(','),
            table_context.clone(),
            extraction_config_headers_patients_in_rows.clone(),
        ));

        let mut data_frames = data_source.extract().unwrap();
        let cdf = data_frames.pop().expect("No data");
        assert_eq!(cdf.context(), &table_context);

        let expected_df: DataFrame = df![
            "Patient_IDs" => &["PID_1", "PID_2", "PID_3"],
            "HPO_IDs" => &[54, 55,56],
            "SEX" => &["M", "F", "M"],
            "AGE" => &[18, 27, 89]
        ]
        .unwrap();

        assert_eq!(expected_df, cdf.data);
    }

    #[rstest]
    fn test_extract_csv_extract_config_headers_patient_in_columns(
        temp_dir: TempDir,
        extract_config_headers_patients_in_columns: ExtractionConfig,
    ) {
        let test_data = br#"
Patient_IDs,PID_1,PID_2,PID_3
HPO_IDs,54,55,56
SEX,M,F,M
AGE,18,27,89"#;

        let table_context = TableContext::default();
        let file_path = temp_dir.path().join("test_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(test_data).unwrap();

        let data_source = DataSource::Csv(CSVDataSource::new(
            file_path,
            Some(','),
            table_context.clone(),
            extract_config_headers_patients_in_columns.clone(),
        ));

        let mut data_frames = data_source.extract().unwrap();
        let cdf = data_frames.pop().expect("No data");
        assert_eq!(cdf.context(), &table_context);

        let expected_df: DataFrame = df![
            "Patient_IDs" => &["PID_1", "PID_2", "PID_3"],
            "HPO_IDs" => &[54, 55,56],
            "SEX" => &["M", "F", "M"],
            "AGE" => &[18, 27, 89]
        ]
        .unwrap();

        assert_eq!(expected_df, cdf.data);
    }

    #[allow(clippy::too_many_arguments)]
    #[rstest]
    fn test_extract_excel(
        table_contexts: Vec<TableContext>,
        extraction_configs: Vec<ExtractionConfig>,
        temp_dir: TempDir,
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
        row_names: [&'static str; 4],
        times_of_birth: [ExcelDateTime; 4],
        ages: [i32; 4],
        weights: [f64; 4],
        smoker_bools: [bool; 4],
    ) {
        //Write desired data to an Excel file
        let mut workbook = Workbook::new();

        workbook.add_worksheet().set_name("first_sheet").unwrap();
        workbook.add_worksheet().set_name("second_sheet").unwrap();
        workbook.add_worksheet().set_name("third_sheet").unwrap();
        workbook.add_worksheet().set_name("fourth_sheet").unwrap();

        for worksheet in workbook.worksheets_mut() {
            let mut offset_due_to_header = 0;

            if worksheet.name() == "first_sheet" {
                worksheet.write_row(0, 0, column_names).unwrap();
                offset_due_to_header = 1;
            }

            if worksheet.name() == "second_sheet" {
                worksheet.write_column(0, 0, row_names).unwrap();
                offset_due_to_header = 1;
            }

            if worksheet.name() == "first_sheet" || worksheet.name() == "third_sheet" {
                worksheet
                    .write_column(offset_due_to_header as RowNum, 0, patient_ids)
                    .unwrap();
                worksheet
                    .write_column(offset_due_to_header as RowNum, 1, hpo_ids)
                    .unwrap();
                worksheet
                    .write_column(offset_due_to_header as RowNum, 2, disease_ids)
                    .unwrap();
                worksheet
                    .write_column(offset_due_to_header as RowNum, 3, subject_sexes)
                    .unwrap();
            }

            if worksheet.name() == "second_sheet" || worksheet.name() == "fourth_sheet" {
                worksheet
                    .write_row_with_format(
                        0,
                        offset_due_to_header as ColNum,
                        &times_of_birth,
                        &Format::new().set_num_format("yyyy-mm-dd hh:mm:ss"),
                    )
                    .unwrap();
                worksheet
                    .write_row(1, offset_due_to_header as ColNum, ages)
                    .unwrap();
                worksheet
                    .write_row(2, offset_due_to_header as ColNum, weights)
                    .unwrap();
                worksheet
                    .write_row(3, offset_due_to_header as ColNum, smoker_bools)
                    .unwrap();
            }
        }

        let file_path = temp_dir.path().join("test_excel.xlsx");
        workbook.save(file_path.clone()).unwrap();

        //Now we test the extraction
        let data_source = DataSource::Excel(ExcelDatasource::new(
            file_path,
            table_contexts.clone(),
            extraction_configs.clone(),
        ));

        let data_frames = data_source.extract().unwrap();
        for data_frame in data_frames {
            let extracted_data = data_frame.data.clone();

            if data_frame.context().name == "first_sheet" {
                assert_eq!(extracted_data.get_column_names(), column_names);
            } else if data_frame.context().name == "second_sheet" {
                assert_eq!(extracted_data.get_column_names(), row_names);
            } else {
                assert_eq!(extracted_data.get_column_names(), ["0", "1", "2", "3"]);
            }

            let extracted_col0 = extracted_data.select_at_idx(0).unwrap();
            let extracted_col1 = extracted_data.select_at_idx(1).unwrap();
            let extracted_col2 = extracted_data.select_at_idx(2).unwrap();
            let extracted_col3 = extracted_data.select_at_idx(3).unwrap();

            if data_frame.context().name == "first_sheet"
                || data_frame.context().name == "third_sheet"
            {
                let extracted_patient_ids: Vec<_> =
                    extracted_col0.str().unwrap().into_no_null_iter().collect();
                let extracted_hpo_ids: Vec<_> =
                    extracted_col1.str().unwrap().into_no_null_iter().collect();
                let extracted_disease_ids: Vec<_> =
                    extracted_col2.str().unwrap().into_no_null_iter().collect();
                let extracted_subject_sexes: Vec<_> =
                    extracted_col3.str().unwrap().into_no_null_iter().collect();
                assert_eq!(extracted_patient_ids, patient_ids);
                assert_eq!(extracted_hpo_ids, hpo_ids);
                assert_eq!(extracted_disease_ids, disease_ids);
                assert_eq!(extracted_subject_sexes, subject_sexes);
            }

            if data_frame.context().name == "second_sheet"
                || data_frame.context().name == "fourth_sheet"
            {
                let extracted_times_of_birth = extracted_col0
                    .datetime()
                    .unwrap()
                    .to_string("%Y-%m-%dT%H:%M:%SZ")
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let expected_times_of_birth = times_of_birth
                    .iter()
                    .map(|dt| dt.utc_datetime())
                    .collect::<Vec<String>>();
                assert_eq!(extracted_times_of_birth, expected_times_of_birth);

                let extracted_ages: Vec<_> =
                    extracted_col1.f64().unwrap().into_no_null_iter().collect();
                assert_eq!(
                    extracted_ages,
                    ages.iter().map(|&v| v as f64).collect::<Vec<f64>>()
                );

                let extracted_weights: Vec<_> =
                    extracted_col2.f64().unwrap().into_no_null_iter().collect();
                assert_eq!(extracted_weights, weights);

                let extracted_smoker_bools: Vec<_> =
                    extracted_col3.bool().unwrap().into_no_null_iter().collect();
                assert_eq!(extracted_smoker_bools, smoker_bools);
            }
        }
    }

    #[test]
    fn test_polars_column_string_cast() {
        let mut df = df![
            "int_col" => &["1", "2", "3"],
            "float_col" => &["1.5", "2.5", "3.5"],
            "bool_col" => &["True", "False", "True"],
            "date_col" => &["2023-01-01", "2023-01-02", "2023-01-03"],
            "datetime_col" => &["2023-01-01T12:00:00", "2023-01-02T13:30:00", "2023-01-03T15:45:00"],
            "string_col" => &["hello", "world", "test"]
        ].unwrap();

        let result = DataSource::polars_column_string_cast(&mut df);
        assert!(result.is_ok());
        assert_eq!(df.column("int_col").unwrap().dtype(), &DataType::Int64);
        assert_eq!(df.column("float_col").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("bool_col").unwrap().dtype(), &DataType::Boolean);
        assert_eq!(df.column("date_col").unwrap().dtype(), &DataType::Date);
        assert_eq!(
            df.column("datetime_col").unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(df.column("string_col").unwrap().dtype(), &DataType::String);
    }

    fn create_test_cdf() -> ContextualizedDataFrame {
        let data = df![
            "id" => &["patient1", "patient2"],
            "value1" => &[1, 2],
            "value2" => &[3, 4]
        ]
        .unwrap();
        let context = TableContext::new("".to_string(), vec![]);
        ContextualizedDataFrame::new(context, data)
    }

    #[rstest]
    fn test_no_transpose_when_patients_are_rows() {
        let cdf = create_test_cdf();
        let result = DataSource::conditional_transpose(cdf.clone(), &true, &true).unwrap();

        assert_eq!(result.data.shape(), cdf.data.shape());
    }

    #[rstest]
    fn test_transpose_with_header() {
        let cdf = create_test_cdf();
        let result = DataSource::conditional_transpose(cdf.clone(), &false, &true).unwrap();

        assert_eq!(result.data.shape().0, cdf.data.width() - 1);
        assert_eq!(result.data.shape().1, cdf.data.height());

        assert_eq!(
            result
                .data
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            vec!["patient1", "patient2"]
        );
    }
}
