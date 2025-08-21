use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;
use polars::io::SerReader;
use polars::prelude::{AnyValue, Column, CsvReadOptions};

use std::sync::Arc;

use crate::extract::excel_data_source::{ExcelDatasource, PatientOrientation};
use crate::extract::traits::Extractable;
use serde::{Deserialize, Serialize};

use crate::config::table_context::TableContext;
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
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error> {
        match self {
            DataSource::Csv(csv_source) => {
                let mut csv_read_options =
                    CsvReadOptions::default().with_has_header(csv_source.has_column_headers);

                if let Some(sep) = csv_source.separator {
                    let new_parse_options = (*csv_read_options.parse_options)
                        .clone()
                        .with_separator(sep as u8);
                    csv_read_options.parse_options = Arc::from(new_parse_options);
                }
                let csv_data = csv_read_options
                    .try_into_reader_with_file_path(Some(csv_source.source.clone()))?
                    .finish()?;

                Ok(vec![ContextualizedDataFrame::new(
                    csv_source.context.clone(),
                    csv_data,
                )])
            }
            DataSource::Excel(excel_source) => {
                let mut cdf_vec = Vec::new();

                let mut workbook: Xlsx<_> = open_workbook(excel_source.source.clone())?;
                let sheet_names = workbook.sheet_names();

                for sheet_name in sheet_names {
                    let range = workbook.worksheet_range(&sheet_name)?;
                    let no_of_cols = range.get_size().1;

                    let mut vectors: Vec<Vec<AnyValue>> =
                        (0..no_of_cols).map(|_| Vec::new()).collect();

                    //Don't ask me why, but Calamine only allows you to iterate on the rows. This explains why the vectors have been created in this way.
                    for (row_index, row) in range.rows().enumerate() {
                        for (col_index, cell_data) in row.iter().enumerate() {
                            let index_to_load = match excel_source.patient_orientation {
                                PatientOrientation::PatientsAreRows => col_index,
                                PatientOrientation::PatientsAreColumns => row_index,
                            };

                            match *cell_data {
                                Data::Empty => vectors[index_to_load].push(AnyValue::Null),
                                Data::Int(ref i) => {
                                    vectors[index_to_load].push(AnyValue::Int64(*i))
                                }
                                Data::Bool(ref b) => {
                                    vectors[index_to_load].push(AnyValue::Boolean(*b))
                                }
                                //todo something appropriate for Error types
                                Data::Error(ref _e) => {
                                    vectors[index_to_load].push(AnyValue::String("ERROR!!!!!"))
                                }
                                Data::Float(ref f) => {
                                    vectors[index_to_load].push(AnyValue::Float64(*f))
                                }
                                //todo something appropriate for DateTime types
                                Data::DateTime(ref d) => {
                                    vectors[index_to_load].push(AnyValue::Float64(d.as_f64()))
                                }
                                Data::String(ref s)
                                | Data::DateTimeIso(ref s)
                                | Data::DurationIso(ref s) => {
                                    vectors[index_to_load].push(AnyValue::String(s))
                                }
                            }
                        }
                    }

                    let columns: Vec<Column> = if excel_source.has_headers {
                        vectors
                            .iter()
                            .map(|vec| {
                                let header = vec[0].to_string();
                                Column::new(header.into(), vec[1..].to_vec())
                            })
                            .collect()
                    } else {
                        vectors
                            .iter()
                            .enumerate()
                            .map(|(i, vec)| {
                                let header = format!("column {i}");
                                Column::new(header.into(), vec)
                            })
                            .collect()
                    };

                    let sheet_data = DataFrame::new(columns)?;

                    //todo we are enforcing here that the name of the table contexts must be the same as the worksheet names. Is that what we want?
                    //todo we are also enforcing that every sheet has a table context.
                    //todo at what point do we enforce validation of the ExcelDataSource?

                    let sheet_context_opt = excel_source
                        .contexts
                        .iter()
                        .find(|context| context.name == sheet_name.as_str());

                    let sheet_context = match sheet_context_opt {
                        Some(context) => context.clone(),
                        None => TableContext::new(sheet_name, vec![], vec![]),
                    };

                    let cdf = ContextualizedDataFrame::new(sheet_context, sheet_data);
                    cdf_vec.push(cdf);
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
        CellContext, Context, Identifier, SeriesContext, SingleSeriesContext, TableContext,
    };
    use rstest::{fixture, rstest};
    use std::fmt::Write;
    use std::fs::File;
    use std::io::Write as StdWrite;
    use std::path::PathBuf;
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
    fn column_names_excel_sheet_2() -> [&'static str; 4] {
        ["patient_id", "ages", "weight", "smokes"]
    }

    #[fixture]
    fn ages() -> [&'static i32; 4] {
        [&41, &29, &53, &101]
    }

    #[fixture]
    fn weight() -> [&'static f64; 4] {
        [&100.5, &70.3, &95.8, &40.2]
    }

    #[fixture]
    fn smokes() -> [&'static bool; 4] {
        [&false, &true, &false, &true]
    }

    #[fixture]
    fn excel_data(
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
        column_names_excel_sheet_2: [&'static str; 4],
        ages: [&'static i32; 4],
        weight: [&'static f64; 4],
        smokes: [&'static bool; 4],
    ) -> (Vec<u8>, Vec<u8>) {
        //todo write an excel file for the test
        (vec![], vec![])
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
    fn test_tc() -> TableContext {
        TableContext::new(
            "first_sheet".to_string(),
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("patient_id".to_string()),
                Context::None,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                vec![Identifier::Name("disease_id".to_string())],
            ))],
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("test_row".to_string()),
                Context::None,
                Some(CellContext::new(Context::None, None, Default::default())),
                vec![Identifier::Name("another_row".to_string())],
            ))],
        )
    }

    #[fixture]
    fn test_tcs(test_tc: TableContext) -> Vec<TableContext> {
        let test_tc2 = TableContext::new(
            "second_sheet".to_string(),
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("phenotypes".to_string()),
                Context::None,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                vec![Identifier::Name("patient_id".to_string())],
            ))],
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("test_row_2".to_string()),
                Context::None,
                Some(CellContext::new(Context::None, None, Default::default())),
                vec![Identifier::Name("another_row_2".to_string())],
            ))],
        );
        vec![test_tc, test_tc2]
    }

    #[rstest]
    fn test_extract_csv(
        temp_dir: TempDir,
        csv_data: Vec<u8>,
        test_tc: TableContext,
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
            test_tc.clone(),
            true,
        ));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().expect("No data");

        assert_eq!(context_df.context(), &test_tc);

        let column_data: [&[&str]; 4] = [&patient_ids, &hpo_ids, &disease_ids, &subject_sexes];

        let column_data_pairs: Vec<(&str, &[&str])> = column_names
            .iter()
            .zip(column_data.iter())
            .map(|(&col_name, &col_data)| (col_name, col_data))
            .collect();

        for (col_name, expected_values) in column_data_pairs.iter() {
            let loaded_data = context_df.data();
            let col_content = loaded_data
                .column(col_name)
                .expect("Failed to load column")
                .str()
                .unwrap();

            for (i, value) in col_content.iter().enumerate() {
                let unwrapped_value = value.unwrap();
                assert_eq!(expected_values[i], unwrapped_value);
            }
        }
    }

    #[rstest]
    fn test_extract_excel(test_tcs: Vec<TableContext>) {
        let file_path = PathBuf::from("/Users/patrick/RustroverProjects/PhenoXtrackt/src/extract/test_excel.xlsx");

        let data_source =
            DataSource::Excel(ExcelDatasource::new(file_path, test_tcs.clone(), true,PatientOrientation::PatientsAreRows));

        let data_frames = data_source.extract().unwrap();
        dbg!(data_frames);

        //todo DEAL WITH CASE WHERE COLUMNS CONTAIN E.G. FLOATS AND STRINGS
        //todo why are integers being interpreted as floats?
    }
}
