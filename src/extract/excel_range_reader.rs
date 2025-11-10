use crate::extract::error::ExtractionError;
use crate::extract::extraction_config::ExtractionConfig;
use crate::extract::utils::generate_default_column_names;
use calamine::{Data, Range};
use log::{info, warn};
use polars::datatypes::AnyValue;
use polars::frame::DataFrame;
use polars::prelude::TimeUnit::Milliseconds;
use polars::prelude::{Column, IntoColumn, NamedFrom, Series};

pub struct ExcelRangeReader {
    pub range: Range<Data>,
    pub extraction_config: ExtractionConfig,
}

impl ExcelRangeReader {
    pub(crate) fn new(range: Range<Data>, extraction_config: ExtractionConfig) -> Self {
        ExcelRangeReader {
            range,
            extraction_config,
        }
    }
    pub fn extract_to_df(self) -> Result<DataFrame, ExtractionError> {
        let mut loading_vectors = self.create_loading_vectors();
        self.load_data_to_vectors(&mut loading_vectors)?;
        let columns_result = self.convert_vectors_to_columns(loading_vectors);
        let columns = columns_result?;
        let dataframe = DataFrame::new(columns)?;
        Ok(dataframe)
    }

    fn create_loading_vectors(&'_ self) -> Vec<Vec<AnyValue<'_>>> {
        let number_of_vecs;
        let loading_vector_capacity;
        if self.extraction_config.patients_are_rows {
            number_of_vecs = self.range.width();
            loading_vector_capacity = self.range.height();
        } else {
            number_of_vecs = self.range.height();
            loading_vector_capacity = self.range.width();
        }
        (0..number_of_vecs)
            .map(|_| Vec::with_capacity(loading_vector_capacity))
            .collect()
    }

    fn load_data_to_vectors<'a>(
        &'a self,
        loading_vectors: &mut Vec<Vec<AnyValue<'a>>>,
    ) -> Result<(), ExtractionError> {
        let sheet_name = self.extraction_config.name.as_str();
        for (row_index, row) in self.range.rows().enumerate() {
            for (col_index, cell_data) in row.iter().enumerate() {
                let index_to_load = if self.extraction_config.patients_are_rows {
                    col_index
                } else {
                    row_index
                };

                let vector_to_load = loading_vectors.get_mut(index_to_load).ok_or(
                    ExtractionError::ExcelIndexing(index_to_load, sheet_name.to_string()),
                )?;

                match *cell_data {
                    Data::Empty => vector_to_load.push(AnyValue::Null),
                    Data::Int(ref i) => vector_to_load.push(AnyValue::Int64(*i)),
                    Data::Bool(ref b) => vector_to_load.push(AnyValue::Boolean(*b)),
                    Data::Error(ref e) => {
                        warn!(
                            "An error {e} in Excel Worksheet {sheet_name} was found at row {row_index}, column {col_index}."
                        );
                        vector_to_load.push(AnyValue::Null)
                    }
                    Data::Float(ref f) => vector_to_load.push(AnyValue::Float64(*f)),
                    Data::DateTime(ref d) => {
                        let fallback = || {
                            warn!(
                                "Could not interpret Excel DateTime in worksheet {sheet_name} at row {row_index}, column {col_index}. Entry converted to f64."
                            );
                            AnyValue::Float64(d.as_f64())
                        };

                        let time_val = if d.is_datetime() {
                            if let Some(dt) = d.as_datetime() {
                                AnyValue::Datetime(
                                    dt.and_utc().timestamp_millis(),
                                    Milliseconds,
                                    None,
                                )
                            } else {
                                fallback()
                            }
                        } else if d.is_duration() {
                            if let Some(dur) = d.as_duration() {
                                AnyValue::Duration(dur.num_milliseconds(), Milliseconds)
                            } else {
                                fallback()
                            }
                        } else {
                            fallback()
                        };

                        vector_to_load.push(time_val);
                    }
                    Data::String(ref s) | Data::DateTimeIso(ref s) | Data::DurationIso(ref s) => {
                        vector_to_load.push(AnyValue::String(s))
                    }
                }
            }
        }

        Ok(())
    }

    fn convert_vectors_to_columns(
        &self,
        loading_vectors: Vec<Vec<AnyValue>>,
    ) -> Result<Vec<Column>, ExtractionError> {
        let default_column_names = generate_default_column_names(loading_vectors.len() as i64);
        loading_vectors
            .iter()
            .enumerate()
            .map(|(i, vec)| {
                let header;
                let data;

                if self.extraction_config.has_headers {
                    let h = vec.first().ok_or(ExtractionError::EmptyVector)?;
                    header = h.get_str().ok_or(ExtractionError::NoStringInHeader)?.to_string();
                    data = vec.get(1..).ok_or(ExtractionError::EmptyVector)?;
                } else {
                    header = default_column_names.get(i).ok_or(ExtractionError::VectorIndexing(i,default_column_names.len()))?.to_string();
                    data = vec;
                }

                let series_result =
                    Series::from_any_values(header.clone().into(), data, false);

                //if the from_any_values function fails to convert the values to a single type
                //we stringify the data to create the series
                let series = series_result.unwrap_or_else(|_| {
                    info!("Column/row {} in Excel Worksheet {} contained multiple data types. These have been turned into strings.", header,self.extraction_config.name);
                    let stringified_col_data: Vec<String> =
                        data.iter().map(|d| d.to_string()).collect();
                    Series::new(header.into(), stringified_col_data)
                });

                Ok(series.into_column())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::extraction_config::ExtractionConfig;
    use calamine::{Reader, Xlsx, open_workbook};
    use rstest::{fixture, rstest};
    use rust_xlsxwriter::Workbook;
    use std::fs::File;
    use std::io::BufReader;
    use std::vec::Vec;
    use tempfile::TempDir;

    #[fixture]
    fn patient_id_col() -> (&'static str, [&'static str; 4]) {
        ("patient_id", ["P001", "P002", "P003", "P004"])
    }

    #[fixture]
    fn age_col() -> (&'static str, [i64; 4]) {
        ("age", [41, 29, 53, 101])
    }

    #[fixture]
    fn weight_col() -> (&'static str, [f64; 4]) {
        ("weight", [100.5, 70.3, 95.8, 40.2])
    }

    #[fixture]
    fn smoker_col() -> (&'static str, [bool; 4]) {
        ("smokes", [false, true, false, true])
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn excel_range_reader_with_headers(
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
        temp_dir: TempDir,
    ) -> ExcelRangeReader {
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet().set_name("worksheet").unwrap();

        worksheet.write(0, 0, patient_id_col.0).unwrap();
        worksheet.write(0, 1, age_col.0).unwrap();
        worksheet.write(0, 2, weight_col.0).unwrap();
        worksheet.write(0, 3, smoker_col.0).unwrap();

        worksheet.write_column(1, 0, patient_id_col.1).unwrap();
        worksheet.write_column(1, 1, age_col.1).unwrap();
        worksheet.write_column(1, 2, weight_col.1).unwrap();
        worksheet.write_column(1, 3, smoker_col.1).unwrap();

        let file_path = temp_dir.path().join("test_excel.xlsx");
        workbook.save(file_path.clone()).unwrap();

        let mut workbook: Xlsx<BufReader<File>> = open_workbook(file_path).unwrap();
        let range = workbook.worksheet_range("worksheet").unwrap();
        let ec = ExtractionConfig::new("first_sheet".to_string(), true, true);
        ExcelRangeReader::new(range, ec)
    }

    #[fixture]
    fn excel_range_reader_no_headers(
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
        temp_dir: TempDir,
    ) -> ExcelRangeReader {
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet().set_name("worksheet").unwrap();

        worksheet.write_column(0, 0, patient_id_col.1).unwrap();
        worksheet.write_column(0, 1, age_col.1).unwrap();
        worksheet.write_column(0, 2, weight_col.1).unwrap();
        worksheet.write_column(0, 3, smoker_col.1).unwrap();

        let file_path = temp_dir.path().join("test_excel.xlsx");
        workbook.save(file_path.clone()).unwrap();

        let mut workbook: Xlsx<BufReader<File>> = open_workbook(file_path).unwrap();
        let range = workbook.worksheet_range("worksheet").unwrap();
        let ec = ExtractionConfig::new("first_sheet".to_string(), false, true);
        ExcelRangeReader::new(range, ec)
    }

    #[fixture]
    fn full_vecs_no_headers(
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
    ) -> Vec<Vec<AnyValue<'static>>> {
        let vec1 = patient_id_col
            .1
            .iter()
            .map(|s| AnyValue::String(s))
            .collect();
        let vec2 = age_col
            .1
            .iter()
            .map(|i| AnyValue::Float64(*i as f64))
            .collect();
        let vec3 = weight_col.1.iter().map(|f| AnyValue::Float64(*f)).collect();
        let vec4 = smoker_col.1.iter().map(|b| AnyValue::Boolean(*b)).collect();
        vec![vec1, vec2, vec3, vec4]
    }

    #[fixture]
    fn full_vecs_with_headers(
        full_vecs_no_headers: Vec<Vec<AnyValue<'static>>>,
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
    ) -> Vec<Vec<AnyValue<'static>>> {
        let mut vec1 = vec![AnyValue::String(patient_id_col.0)];
        vec1.extend(full_vecs_no_headers[0].clone());

        let mut vec2 = vec![AnyValue::String(age_col.0)];
        vec2.extend(full_vecs_no_headers[1].clone());

        let mut vec3 = vec![AnyValue::String(weight_col.0)];
        vec3.extend(full_vecs_no_headers[2].clone());

        let mut vec4 = vec![AnyValue::String(smoker_col.0)];
        vec4.extend(full_vecs_no_headers[3].clone());
        vec![vec1, vec2, vec3, vec4]
    }

    #[rstest]
    fn test_create_loading_vectors_with_headers(excel_range_reader_with_headers: ExcelRangeReader) {
        let empty_vecs = excel_range_reader_with_headers.create_loading_vectors();
        assert_eq!(empty_vecs.len(), 4);
        for vec in empty_vecs {
            assert_eq!(vec.capacity(), 5);
            assert_eq!(vec, vec![]);
        }
    }

    #[rstest]
    fn test_create_loading_vectors_no_headers(excel_range_reader_no_headers: ExcelRangeReader) {
        let empty_vecs = excel_range_reader_no_headers.create_loading_vectors();
        assert_eq!(empty_vecs.len(), 4);
        for vec in empty_vecs {
            assert_eq!(vec.capacity(), 4);
            assert_eq!(vec, vec![]);
        }
    }

    #[rstest]
    fn test_load_data_to_vectors_with_headers(
        excel_range_reader_with_headers: ExcelRangeReader,
        full_vecs_with_headers: Vec<Vec<AnyValue<'static>>>,
    ) {
        let empty_vecs: Vec<Vec<AnyValue>> = (0..4).map(|_| Vec::with_capacity(5)).collect();

        let vecs_ref = &mut empty_vecs.clone();
        excel_range_reader_with_headers
            .load_data_to_vectors(vecs_ref)
            .unwrap();
        assert_eq!(vecs_ref[0], full_vecs_with_headers[0]);
        assert_eq!(vecs_ref[1], full_vecs_with_headers[1]);
        assert_eq!(vecs_ref[2], full_vecs_with_headers[2]);
        assert_eq!(vecs_ref[3], full_vecs_with_headers[3]);
    }

    #[rstest]
    fn test_load_data_to_vectors_no_headers(
        excel_range_reader_no_headers: ExcelRangeReader,
        full_vecs_no_headers: Vec<Vec<AnyValue<'static>>>,
    ) {
        let empty_vecs: Vec<Vec<AnyValue>> = (0..4).map(|_| Vec::with_capacity(4)).collect();

        let vecs_ref = &mut empty_vecs.clone();
        excel_range_reader_no_headers
            .load_data_to_vectors(vecs_ref)
            .unwrap();
        assert_eq!(vecs_ref[0], full_vecs_no_headers[0]);
        assert_eq!(vecs_ref[1], full_vecs_no_headers[1]);
        assert_eq!(vecs_ref[2], full_vecs_no_headers[2]);
        assert_eq!(vecs_ref[3], full_vecs_no_headers[3]);
    }

    #[rstest]
    fn test_convert_vectors_to_columns_with_headers(
        excel_range_reader_with_headers: ExcelRangeReader,
        full_vecs_with_headers: Vec<Vec<AnyValue>>,
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
    ) {
        let cols = excel_range_reader_with_headers
            .convert_vectors_to_columns(full_vecs_with_headers)
            .unwrap();
        assert_eq!(cols[0].name().to_string(), "patient_id");
        assert_eq!(cols[1].name().to_string(), "age");
        assert_eq!(cols[2].name().to_string(), "weight");
        assert_eq!(cols[3].name().to_string(), "smokes");

        let extracted_patient_ids: Vec<_> = cols[0].str().unwrap().into_no_null_iter().collect();
        let extracted_ages: Vec<_> = cols[1].f64().unwrap().into_no_null_iter().collect();
        let extracted_weights: Vec<_> = cols[2].f64().unwrap().into_no_null_iter().collect();
        let extracted_smoker_bools: Vec<_> = cols[3].bool().unwrap().into_no_null_iter().collect();
        assert_eq!(extracted_patient_ids, patient_id_col.1);
        assert_eq!(
            extracted_ages,
            age_col.1.iter().map(|&v| v as f64).collect::<Vec<f64>>()
        );
        assert_eq!(extracted_weights, weight_col.1);
        assert_eq!(extracted_smoker_bools, smoker_col.1);
    }

    #[rstest]
    fn test_convert_vectors_to_columns_no_headers(
        excel_range_reader_no_headers: ExcelRangeReader,
        full_vecs_no_headers: Vec<Vec<AnyValue>>,
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
    ) {
        let cols = excel_range_reader_no_headers
            .convert_vectors_to_columns(full_vecs_no_headers)
            .unwrap();
        assert_eq!(cols[0].name().to_string(), "0");
        assert_eq!(cols[1].name().to_string(), "1");
        assert_eq!(cols[2].name().to_string(), "2");
        assert_eq!(cols[3].name().to_string(), "3");

        let extracted_patient_ids: Vec<_> = cols[0].str().unwrap().into_no_null_iter().collect();
        let extracted_ages: Vec<_> = cols[1].f64().unwrap().into_no_null_iter().collect();
        let extracted_weights: Vec<_> = cols[2].f64().unwrap().into_no_null_iter().collect();
        let extracted_smoker_bools: Vec<_> = cols[3].bool().unwrap().into_no_null_iter().collect();
        assert_eq!(extracted_patient_ids, patient_id_col.1);
        assert_eq!(
            extracted_ages,
            age_col.1.iter().map(|&v| v as f64).collect::<Vec<f64>>()
        );
        assert_eq!(extracted_weights, weight_col.1);
        assert_eq!(extracted_smoker_bools, smoker_col.1);
    }

    #[rstest]
    fn test_extract_to_df_with_headers(
        excel_range_reader_with_headers: ExcelRangeReader,
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
    ) {
        let df = excel_range_reader_with_headers.extract_to_df().unwrap();
        assert_eq!(
            df.get_column_names(),
            ["patient_id", "age", "weight", "smokes"]
        );
        let extracted_patient_ids: &Vec<_> = &df["patient_id"]
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let extracted_ages: &Vec<_> = &df["age"].f64().unwrap().into_no_null_iter().collect();
        let extracted_weights: &Vec<_> = &df["weight"].f64().unwrap().into_no_null_iter().collect();
        let extracted_smoker_bools: &Vec<_> =
            &df["smokes"].bool().unwrap().into_no_null_iter().collect();
        assert_eq!(extracted_patient_ids, &patient_id_col.1);
        assert_eq!(
            extracted_ages,
            &age_col.1.iter().map(|&v| v as f64).collect::<Vec<f64>>()
        );
        assert_eq!(extracted_weights, &weight_col.1);
        assert_eq!(extracted_smoker_bools, &smoker_col.1);
    }

    #[rstest]
    fn test_extract_to_df_no_headers(
        excel_range_reader_no_headers: ExcelRangeReader,
        patient_id_col: (&'static str, [&'static str; 4]),
        age_col: (&'static str, [i64; 4]),
        weight_col: (&'static str, [f64; 4]),
        smoker_col: (&'static str, [bool; 4]),
    ) {
        let df = excel_range_reader_no_headers.extract_to_df().unwrap();
        assert_eq!(df.get_column_names(), ["0", "1", "2", "3"]);
        let extracted_patient_ids: &Vec<_> = &df["0"].str().unwrap().into_no_null_iter().collect();
        let extracted_ages: &Vec<_> = &df["1"].f64().unwrap().into_no_null_iter().collect();
        let extracted_weights: &Vec<_> = &df["2"].f64().unwrap().into_no_null_iter().collect();
        let extracted_smoker_bools: &Vec<_> =
            &df["3"].bool().unwrap().into_no_null_iter().collect();
        assert_eq!(extracted_patient_ids, &patient_id_col.1);
        assert_eq!(
            extracted_ages,
            &age_col.1.iter().map(|&v| v as f64).collect::<Vec<f64>>()
        );
        assert_eq!(extracted_weights, &weight_col.1);
        assert_eq!(extracted_smoker_bools, &smoker_col.1);
    }
}
