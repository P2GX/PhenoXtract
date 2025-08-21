use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::csv_data_source::CSVDataSource;
use polars::io::SerReader;
use polars::prelude::{AnyValue, Column, CsvReadOptions};

use std::sync::Arc;

use crate::extract::excel_data_source::ExcelDatasource;
use crate::extract::traits::Extractable;
use serde::{Deserialize, Serialize};

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

                    let mut col_vectors: Vec<Vec<AnyValue>> =
                        (0..no_of_cols).map(|_| Vec::new()).collect();

                    //Don't ask me why, but Calamine only allows you to iterate on the rows. This explains why the vectors have been created in this way.
                    for row in range.rows() {
                        for (col_index, cell_data) in row.iter().enumerate() {
                            match *cell_data {
                                Data::Empty => col_vectors[col_index].push(AnyValue::Null),
                                Data::Int(ref i) => {
                                    col_vectors[col_index].push(AnyValue::Int64(*i))
                                }
                                Data::Bool(ref b) => {
                                    col_vectors[col_index].push(AnyValue::Boolean(*b))
                                }
                                //todo something appropriate for Error types
                                Data::Error(ref _e) => {
                                    col_vectors[col_index].push(AnyValue::String("ERROR!!!!!"))
                                }
                                Data::Float(ref f) => {
                                    col_vectors[col_index].push(AnyValue::Float64(*f))
                                }
                                //todo something appropriate for DateTime types
                                Data::DateTime(ref d) => {
                                    col_vectors[col_index].push(AnyValue::Float64(d.as_f64()))
                                }
                                Data::String(ref s)
                                | Data::DateTimeIso(ref s)
                                | Data::DurationIso(ref s) => {
                                    col_vectors[col_index].push(AnyValue::String(s))
                                }
                            }
                        }
                    }

                    //todo I'm not sure how I feel about doing this as part of the load stage. Same goes for considering the CSV headers.
                    let columns: Vec<Column> = if excel_source.has_column_headers {
                        col_vectors
                            .iter()
                            .map(|col_vec| {
                                //todo how can we be certain that the AnyValue implements to_string so that this makes sense?
                                let col_header = col_vec[0].to_string();
                                Column::new(col_header.into(), col_vec[1..].to_vec())
                            })
                            .collect()
                    } else {
                        col_vectors
                            .iter()
                            .enumerate()
                            .map(|(i, col_vec)| {
                                let col_header = format!("column {i}");
                                Column::new(col_header.into(), col_vec)
                            })
                            .collect()
                    };

                    let sheet_data = DataFrame::new(columns)?;

                    //todo we are enforcing here that the name of the table contexts must be the same as the worksheet names. Is that what we want?
                    //todo we are also enforcing that every sheet has a table context.
                    //todo at what point do we enforce validation of the ExcelDataSource?
                    let sheet_context = excel_source
                        .contexts
                        .iter()
                        .find(|context| context.name == sheet_name.as_str())
                        .expect("One of the Excel Worksheet names was missing from the Table Context names.")
                        .clone();
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

    #[rstest]
    fn test_load_csv(
        temp_dir: TempDir,
        csv_data: Vec<u8>,
        column_names: [&'static str; 4],
        patient_ids: [&'static str; 4],
        hpo_ids: [&'static str; 4],
        disease_ids: [&'static str; 4],
        subject_sexes: [&'static str; 4],
    ) {
        let file_path = temp_dir.path().join("csv_data.csv");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(csv_data.as_slice()).unwrap();

        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("patient_id".to_string()),
                Context::None,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                vec![Identifier::Name("HP:0000054".to_string())],
            ))],
            vec![SeriesContext::Single(SingleSeriesContext::new(
                Identifier::Name("test_row".to_string()),
                Context::None,
                Some(CellContext::new(Context::None, None, Default::default())),
                vec![Identifier::Name("HP:0000054".to_string())],
            ))],
        );

        let data_source = DataSource::Csv(CSVDataSource::new(
            file_path,
            Some(','),
            table_context.clone(),
            true,
        ));

        let mut data_frames = data_source.extract().unwrap();
        let context_df = data_frames.pop().expect("No data");

        assert_eq!(context_df.context(), &table_context);

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
}
