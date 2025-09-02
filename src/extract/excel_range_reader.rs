use crate::extract::error::ExtractionError;
use crate::extract::extraction_config::ExtractionConfig;
use calamine::{Data, Range};
use log::{info, warn};
use polars::datatypes::AnyValue;
use polars::frame::DataFrame;
use polars::prelude::{Column, IntoColumn, NamedFrom, Series};

pub struct ExcelRangeReader {
    pub range: Range<Data>,
    pub extraction_config: ExtractionConfig,
}

impl ExcelRangeReader {
    pub(crate) fn new(range: Range<Data>, extraction_config: &ExtractionConfig) -> Self {
        ExcelRangeReader {
            range,
            extraction_config: extraction_config.clone(),
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

    fn create_loading_vectors(&self) -> Vec<Vec<AnyValue>> {
        let no_of_loading_vectors;
        let loading_vector_capacity;
        if self.extraction_config.patients_are_rows {
            no_of_loading_vectors = self.range.width();
            loading_vector_capacity = self.range.height();
        } else {
            no_of_loading_vectors = self.range.height();
            loading_vector_capacity = self.range.width();
        }
        (0..no_of_loading_vectors)
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
                            "The error {e} in Excel Worksheet {sheet_name} was found at row {row_index}, column {col_index}."
                        );
                        vector_to_load.push(AnyValue::Null)
                    }
                    Data::Float(ref f) => vector_to_load.push(AnyValue::Float64(*f)),
                    Data::DateTime(ref d) => {
                        vector_to_load.push(AnyValue::StringOwned(d.to_string().into()))
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
                    header = format!("{i}");
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
