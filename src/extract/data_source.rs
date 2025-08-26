use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;
use polars::io::SerReader;
use polars::prelude::{AnyValue, Column, CsvReadOptions, IntoColumn, NamedFrom, Series};
use std::fs::File;
use std::io::BufReader;

use crate::extract::error::ExtractionError;
use crate::extract::excel_data_source::ExcelDatasource;
use crate::extract::traits::Extractable;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::table_context::PatientOrientation;
use calamine::Data;
use calamine::{Reader, Xlsx, open_workbook};
use polars::frame::DataFrame;

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

                let mut csv_read_options =
                    CsvReadOptions::default().with_has_header(csv_source.context.has_headers);

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

                let sheet_contexts = excel_source.contexts.clone();

                if sheet_contexts.len() < workbook.sheet_names().len() {
                    warn!("Warning: fewer Table Contexts than Excel Worksheets.");
                } else if sheet_contexts.len() > workbook.sheet_names().len() {
                    warn!("Warning: more Table Contexts than Excel Worksheets.");
                }

                for sheet_context in sheet_contexts {
                    //this makes the search for the appropriate sheets not case sensitive.
                    // todo we might need to validate to makes sure the user doesn't do something silly like have two table contexts called ASheet and asheet
                    let sheet_context_name_lowercase = sheet_context.name.clone().to_lowercase();
                    let sheet_name = match workbook
                        .sheet_names()
                        .iter()
                        .find(|name| name.to_lowercase() == sheet_context_name_lowercase)
                    {
                        Some(r) => r.clone(),
                        None => {
                            warn!(
                                "Could not find Excel Worksheet with the name {sheet_context_name_lowercase}!"
                            );
                            continue;
                        }
                    };

                    let range = match workbook.worksheet_range(&sheet_name) {
                        Ok(r) => r,
                        Err(_) => {
                            warn!(
                                "The Calamine .worksheet_range method could not find a sheet with the name {sheet_name}!"
                            );
                            continue;
                        }
                    };

                    let no_of_vectors = match sheet_context.patient_orientation {
                        PatientOrientation::PatientsAreRows => range.get_size().1,
                        PatientOrientation::PatientsAreColumns => range.get_size().0,
                    };

                    let mut vectors: Vec<Vec<AnyValue>> =
                        (0..no_of_vectors).map(|_| Vec::new()).collect();

                    for (row_index, row) in range.rows().enumerate() {
                        for (col_index, cell_data) in row.iter().enumerate() {
                            let index_to_load = match sheet_context.patient_orientation {
                                PatientOrientation::PatientsAreRows => col_index,
                                PatientOrientation::PatientsAreColumns => row_index,
                            };

                            //todo I am writing this code to avoid panicking if we have indexing errors. Uncertain if that is the right thing to do.
                            let vector_to_load = vectors
                                .get_mut(index_to_load)
                                .ok_or(ExtractionError::ExcelIndexing)?;

                            match *cell_data {
                                Data::Empty => vector_to_load.push(AnyValue::Null),
                                Data::Int(ref i) => vector_to_load.push(AnyValue::Int64(*i)),
                                Data::Bool(ref b) => vector_to_load.push(AnyValue::Boolean(*b)),
                                //todo something appropriate for Error types
                                Data::Error(ref _e) => {
                                    vector_to_load.push(AnyValue::String("ERROR!!!!!"))
                                }
                                Data::Float(ref f) => vector_to_load.push(AnyValue::Float64(*f)),
                                //todo something appropriate for DateTime types
                                Data::DateTime(ref d) => {
                                    vector_to_load.push(AnyValue::Float64(d.as_f64()))
                                }
                                Data::String(ref s)
                                | Data::DateTimeIso(ref s)
                                | Data::DurationIso(ref s) => {
                                    vector_to_load.push(AnyValue::String(s))
                                }
                            }
                        }
                    }

                    let columnify_result: Result<Vec<Column>, ExtractionError> = vectors
                        .iter()
                        .enumerate()
                        .map(|(i, vec)| {
                            let header;
                            let data;

                            if sheet_context.has_headers {
                                let h = vec.first().ok_or(ExtractionError::VectorIndexing)?;
                                header = h.get_str().ok_or(ExtractionError::NoStringInHeader)?.to_string();
                                data = vec.get(1..).ok_or(ExtractionError::VectorIndexing)?;
                            } else {
                                header = format!("{i}");
                                data = vec;
                            }

                            let series_result =
                                Series::from_any_values(header.clone().into(), data, false);

                            //if the from_any_values function fails to convert the values to a single type
                            //we stringify the data to create the series
                            let series = match series_result {
                                Ok(s) => s,
                                Err(_) => {
                                    info!("Column/row {} in Excel Worksheet {} of Excel Workbook {} contained multiple data types. These have been turned into strings.", header,sheet_name,excel_source.source.display());
                                    let stringified_col_data: Vec<String> =
                                        data.iter().map(|d| d.to_string()).collect();
                                    Series::new(header.into(), stringified_col_data)
                                }
                            };

                            Ok(series.into_column())
                        })
                        .collect();

                    let columns = columnify_result?;

                    let sheet_data = DataFrame::new(columns)?;
                    let cdf = ContextualizedDataFrame::new(sheet_context, sheet_data);
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
    use rstest::{fixture, rstest};
    use rust_xlsxwriter::{ColNum, RowNum, Workbook};
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
        ["patient_id", "age", "weight", "smokes"]
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
            true,
            PatientOrientation::PatientsAreRows,
        )
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
            true,
            PatientOrientation::PatientsAreColumns,
        )
    }

    //column-wise data without headers
    #[fixture]
    fn test_tc3(test_tc1: TableContext) -> TableContext {
        let mut test_tc3 = test_tc1.clone();
        test_tc3.name = "third_sheet".to_string();
        test_tc3.has_headers = false;
        test_tc3
    }

    //row-wise data without headers
    #[fixture]
    fn test_tc4(test_tc2: TableContext) -> TableContext {
        let mut test_tc4 = test_tc2.clone();
        test_tc4.name = "fourth_sheet".to_string();
        test_tc4.has_headers = false;
        test_tc4
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

    #[allow(clippy::too_many_arguments)]
    #[rstest]
    fn test_extract_csv(
        temp_dir: TempDir,
        csv_data: Vec<u8>,
        test_tc1: TableContext,
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
    ) {
        let file_path = temp_dir.path().join("csv_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(csv_data.as_slice()).unwrap();

        let data_source =
            DataSource::Csv(CSVDataSource::new(file_path, Some(','), test_tc1.clone()));

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
        temp_dir: TempDir,
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
        row_names: [&'static str; 4],
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
                    .write_row(0, offset_due_to_header as ColNum, patient_ids)
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
        let data_source = DataSource::Excel(ExcelDatasource::new(file_path, test_tcs.clone()));

        let data_frames = data_source.extract().unwrap();

        for data_frame in data_frames {
            let extracted_data = data_frame.data();

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
                let extracted_patient_ids: Vec<_> =
                    extracted_col0.str().unwrap().into_no_null_iter().collect();
                let extracted_ages: Vec<_> =
                    extracted_col1.f64().unwrap().into_no_null_iter().collect();
                let extracted_weights: Vec<_> =
                    extracted_col2.f64().unwrap().into_no_null_iter().collect();
                let extracted_smoker_bools: Vec<_> =
                    extracted_col3.bool().unwrap().into_no_null_iter().collect();
                assert_eq!(extracted_patient_ids, patient_ids);
                assert_eq!(
                    extracted_ages,
                    ages.iter().map(|&v| v as f64).collect::<Vec<f64>>()
                );
                assert_eq!(extracted_weights, weights);
                assert_eq!(extracted_smoker_bools, smoker_bools);
            }
        }
    }
}
