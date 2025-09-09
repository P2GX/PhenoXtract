use crate::extract::error::ExtractionError;
use crate::extract::extraction_config::ExtractionConfig;
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

    fn create_loading_vectors<'a>(&'a self) -> Vec<Vec<AnyValue<'a>>> {
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
        let sheet_name = &self.extraction_config.name;
        for (row_index, row) in self.range.rows().enumerate() {
            for (col_index, cell_data) in row.iter().enumerate() {
                let index_to_load = if self.extraction_config.patients_are_rows {
                    col_index
                } else {
                    row_index
                };

                let vector_to_load = loading_vectors
                    .get_mut(index_to_load)
                    .ok_or(ExtractionError::ExcelIndexing(
                    format!(
                        "Out of bounds index when loading vector {index_to_load} in {sheet_name}."
                    )
                    .to_string(),
                ))?;

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
        loading_vectors
            .iter()
            .enumerate()
            .map(|(i, vec)| {
                let header;
                let data;

                if self.extraction_config.has_headers {
                    let h = vec.first().ok_or(ExtractionError::VectorIndexing("Empty vector.".to_string()))?;
                    header = h.get_str().ok_or(ExtractionError::NoStringInHeader("Header string was empty.".to_string()))?.to_string();
                    data = vec.get(1..).ok_or(ExtractionError::VectorIndexing("No data contained in vector.".to_string()))?;
                } else {
                    header = format!("column_{}",i+1);
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
    use tempfile::TempDir;

    #[fixture]
    fn patient_id_col() -> [&'static str; 5] {
        ["patient_id", "P001", "P002", "P003", "P004"]
    }

    #[fixture]
    fn hpo_id_col() -> [&'static str; 5] {
        [
            "hpo_id",
            "HP:0000054",
            "HP:0000046",
            "HP:0000040",
            "HP:0030265",
        ]
    }

    #[fixture]
    fn disease_id_col() -> [&'static str; 5] {
        [
            "disease_id",
            "MONDO:100100",
            "MONDO:100200",
            "MONDO:100300",
            "MONDO:100400",
        ]
    }

    #[fixture]
    fn subject_sex_col() -> [&'static str; 5] {
        ["sex", "Male", "Female", "Male", "Female"]
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory.")
    }

    #[fixture]
    fn test_range(
        patient_id_col: [&'static str; 5],
        hpo_id_col: [&'static str; 5],
        disease_id_col: [&'static str; 5],
        subject_sex_col: [&'static str; 5],
        temp_dir: TempDir,
    ) -> Range<Data> {
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet().set_name("worksheet").unwrap();

        worksheet.write_column(0, 0, patient_id_col).unwrap();
        worksheet.write_column(0, 1, hpo_id_col).unwrap();
        worksheet.write_column(0, 2, disease_id_col).unwrap();
        worksheet.write_column(0, 3, subject_sex_col).unwrap();

        let file_path = temp_dir.path().join("test_excel.xlsx");
        workbook.save(file_path.clone()).unwrap();

        let mut workbook: Xlsx<BufReader<File>> = open_workbook(file_path).unwrap();
        workbook.worksheet_range("worksheet").unwrap()
    }

    #[fixture]
    fn test_ec() -> ExtractionConfig {
        ExtractionConfig::new("first_sheet".to_string(), true, true)
    }

    #[fixture]
    fn test_excel_range_reader(
        test_range: Range<Data>,
        test_ec: ExtractionConfig,
    ) -> ExcelRangeReader {
        ExcelRangeReader::new(test_range, test_ec)
    }

    #[fixture]
    fn empty_vecs() -> Vec<Vec<AnyValue<'static>>> {
        (0..4).map(|_| Vec::with_capacity(5)).collect()
    }

    #[fixture]
    fn full_vecs(
        patient_id_col: [&'static str; 5],
        hpo_id_col: [&'static str; 5],
        disease_id_col: [&'static str; 5],
        subject_sex_col: [&'static str; 5],
    ) -> Vec<Vec<AnyValue<'static>>> {
        let vec1 = patient_id_col.iter().map(|s| AnyValue::String(s)).collect();
        let vec2 = hpo_id_col.iter().map(|s| AnyValue::String(s)).collect();
        let vec3 = disease_id_col.iter().map(|s| AnyValue::String(s)).collect();
        let vec4 = subject_sex_col
            .iter()
            .map(|s| AnyValue::String(s))
            .collect();
        vec![vec1, vec2, vec3, vec4]
    }

    #[rstest]
    fn test_create_loading_vectors(test_excel_range_reader: ExcelRangeReader) {
        let empty_vecs = test_excel_range_reader.create_loading_vectors();
        assert_eq!(empty_vecs.len(), 4);
        for vec in empty_vecs {
            assert_eq!(vec.capacity(), 5);
            assert_eq!(vec, vec![]);
        }
    }

    #[rstest]
    fn test_load_data_to_vectors(
        test_excel_range_reader: ExcelRangeReader,
        empty_vecs: Vec<Vec<AnyValue>>,
        patient_id_col: [&'static str; 5],
        hpo_id_col: [&'static str; 5],
        disease_id_col: [&'static str; 5],
        subject_sex_col: [&'static str; 5],
    ) {
        let vecs_ref = &mut empty_vecs.clone();
        test_excel_range_reader
            .load_data_to_vectors(vecs_ref)
            .unwrap();
        assert_eq!(
            vecs_ref[0]
                .iter()
                .map(|val| val.str_value())
                .collect::<Vec<_>>(),
            patient_id_col
        );
        assert_eq!(
            vecs_ref[1]
                .iter()
                .map(|val| val.str_value())
                .collect::<Vec<_>>(),
            hpo_id_col
        );
        assert_eq!(
            vecs_ref[2]
                .iter()
                .map(|val| val.str_value())
                .collect::<Vec<_>>(),
            disease_id_col
        );
        assert_eq!(
            vecs_ref[3]
                .iter()
                .map(|val| val.str_value())
                .collect::<Vec<_>>(),
            subject_sex_col
        );
    }

    #[rstest]
    fn test_convert_vectors_to_columns(
        test_excel_range_reader: ExcelRangeReader,
        full_vecs: Vec<Vec<AnyValue>>,
        patient_id_col: [&'static str; 5],
        hpo_id_col: [&'static str; 5],
        disease_id_col: [&'static str; 5],
        subject_sex_col: [&'static str; 5],
    ) {
        let cols = test_excel_range_reader
            .convert_vectors_to_columns(full_vecs)
            .unwrap();
        assert_eq!(cols[0].name().to_string(), "patient_id");
        assert_eq!(cols[1].name().to_string(), "hpo_id");
        assert_eq!(cols[2].name().to_string(), "disease_id");
        assert_eq!(cols[3].name().to_string(), "sex");

        let extracted_patient_ids: Vec<_> = cols[0].str().unwrap().into_no_null_iter().collect();
        let extracted_hpo_ids: Vec<_> = cols[1].str().unwrap().into_no_null_iter().collect();
        let extracted_disease_ids: Vec<_> = cols[2].str().unwrap().into_no_null_iter().collect();
        let extracted_subject_sexes: Vec<_> = cols[3].str().unwrap().into_no_null_iter().collect();
        assert_eq!(extracted_patient_ids, patient_id_col[1..]);
        assert_eq!(extracted_hpo_ids, hpo_id_col[1..]);
        assert_eq!(extracted_disease_ids, disease_id_col[1..]);
        assert_eq!(extracted_subject_sexes, subject_sex_col[1..]);
    }

    #[rstest]
    fn test_extract_to_df(
        test_excel_range_reader: ExcelRangeReader,
        patient_id_col: [&'static str; 5],
        hpo_id_col: [&'static str; 5],
        disease_id_col: [&'static str; 5],
        subject_sex_col: [&'static str; 5],
    ) {
        let df = test_excel_range_reader.extract_to_df().unwrap();
        assert_eq!(
            df.get_column_names(),
            ["patient_id", "hpo_id", "disease_id", "sex"]
        );
        let extracted_patient_ids: &Vec<_> = &df["patient_id"]
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let extracted_hpo_ids: &Vec<_> = &df["hpo_id"].str().unwrap().into_no_null_iter().collect();
        let extracted_disease_ids: &Vec<_> = &df["disease_id"]
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let extracted_subject_sexes: &Vec<_> =
            &df["sex"].str().unwrap().into_no_null_iter().collect();
        assert_eq!(extracted_patient_ids, &patient_id_col[1..]);
        assert_eq!(extracted_hpo_ids, &hpo_id_col[1..]);
        assert_eq!(extracted_disease_ids, &disease_id_col[1..]);
        assert_eq!(extracted_subject_sexes, &subject_sex_col[1..]);
    }
}
