use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;
use polars::io::SerReader;
use polars::prelude::CsvReadOptions;
use std::fs::File;
use std::io::BufReader;

use crate::extract::error::ExtractionError;
use crate::extract::excel_data_source::ExcelDatasource;
use crate::extract::traits::Extractable;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::extract::excel_range_reader::ExcelRangeReader;
use calamine::{Reader, Xlsx, open_workbook};

/// An enumeration of all supported data source types.
///
/// This enum uses a `tag` to differentiate between source types (e.g., "csv", "excel")
/// when deserializing from a configuration file.
#[derive(Debug, Deserialize, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum DataSource {
    Csv(CSVDataSource),
    Excel(ExcelDatasource),
}

impl Extractable for DataSource {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, ExtractionError> {
        match self {
            DataSource::Csv(csv_source) => {
                info!(
                    "Attempting to extract CSV data from: {}",
                    csv_source.source.display()
                );

                let mut csv_read_options = CsvReadOptions::default()
                    .with_has_header(csv_source.extraction_config.has_headers);

                if let Some(sep) = csv_source.separator {
                    let new_parse_options = (*csv_read_options.parse_options)
                        .clone()
                        .with_separator(sep as u8);
                    csv_read_options.parse_options = Arc::from(new_parse_options);
                }
                let csv_data = csv_read_options
                    .try_into_reader_with_file_path(Some(csv_source.source.clone()))?
                    .finish()?;

                info!("Extracted CSV data from {}", csv_source.source.display());
                Ok(vec![ContextualizedDataFrame::new(
                    csv_source.context.clone(),
                    csv_data,
                )])
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
                        .find(|context| &context.name == sheet_name).expect("Table context sheet names do no correspond to extraction config sheet names.");

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

                    let cdf = ContextualizedDataFrame::new(sheet_context.clone(), sheet_data);
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
    use crate::config::table_context::{
        CellContext, Context, SeriesContext, SingleSeriesContext, TableContext,
    };
    use crate::extract::extraction_config::ExtractionConfig;
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

    //column-wise data with headers
    #[fixture]
    fn test_tc1() -> TableContext {
        TableContext::new(
            "first_sheet".to_string(),
            vec![SeriesContext::Single(SingleSeriesContext::new(
                "patient_id".to_string(),
                Context::None,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                vec!["disease_id".to_string()],
            ))],
        )
    }

    #[fixture]
    fn test_ec1() -> ExtractionConfig {
        ExtractionConfig::new("first_sheet".to_string(), true, true)
    }

    //row-wise data with headers
    #[fixture]
    fn test_tc2() -> TableContext {
        TableContext::new(
            "second_sheet".to_string(),
            vec![SeriesContext::Single(SingleSeriesContext::new(
                "age".to_string(),
                Context::None,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                vec!["weight".to_string()],
            ))],
        )
    }

    #[fixture]
    fn test_ec2() -> ExtractionConfig {
        ExtractionConfig::new("second_sheet".to_string(), true, false)
    }

    //column-wise data without headers
    #[fixture]
    fn test_tc3(test_tc1: TableContext) -> TableContext {
        let mut test_tc3 = test_tc1.clone();
        test_tc3.name = "third_sheet".to_string();
        test_tc3
    }

    #[fixture]
    fn test_ec3() -> ExtractionConfig {
        ExtractionConfig::new("third_sheet".to_string(), false, true)
    }

    //row-wise data without headers
    #[fixture]
    fn test_tc4(test_tc2: TableContext) -> TableContext {
        let mut test_tc4 = test_tc2.clone();
        test_tc4.name = "fourth_sheet".to_string();
        test_tc4
    }

    #[fixture]
    fn test_ec4() -> ExtractionConfig {
        ExtractionConfig::new("fourth_sheet".to_string(), false, false)
    }

    #[fixture]
    fn test_tcs(
        test_tc1: TableContext,
        test_tc2: TableContext,
        test_tc3: TableContext,
        test_tc4: TableContext,
    ) -> Vec<TableContext> {
        vec![test_tc1, test_tc2, test_tc3, test_tc4]
    }

    #[fixture]
    fn test_ecs(
        test_ec1: ExtractionConfig,
        test_ec2: ExtractionConfig,
        test_ec3: ExtractionConfig,
        test_ec4: ExtractionConfig,
    ) -> Vec<ExtractionConfig> {
        vec![test_ec1, test_ec2, test_ec3, test_ec4]
    }

    #[allow(clippy::too_many_arguments)]
    #[rstest]
    fn test_extract_csv(
        temp_dir: TempDir,
        csv_data: Vec<u8>,
        test_tc1: TableContext,
        test_ec1: ExtractionConfig,
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
            test_tc1.clone(),
            test_ec1.clone(),
        ));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().expect("No data");

        assert_eq!(context_df.context(), &test_tc1);

        let expected_data: [&[&str]; 4] = [&patient_ids, &hpo_ids, &disease_ids, &subject_sexes];
        let extracted_data = context_df.data();

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

    #[allow(clippy::too_many_arguments)]
    #[rstest]
    fn test_extract_excel(
        test_tcs: Vec<TableContext>,
        test_ecs: Vec<ExtractionConfig>,
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
            test_tcs.clone(),
            test_ecs.clone(),
        ));

        let data_frames = data_source.extract().unwrap();
        for data_frame in data_frames {
            let extracted_data = data_frame.data();

            if data_frame.context().name == "first_sheet" {
                assert_eq!(extracted_data.get_column_names(), column_names);
            } else if data_frame.context().name == "second_sheet" {
                assert_eq!(extracted_data.get_column_names(), row_names);
            } else {
                assert_eq!(
                    extracted_data.get_column_names(),
                    ["column_1", "column_2", "column_3", "column_4"]
                );
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
}
