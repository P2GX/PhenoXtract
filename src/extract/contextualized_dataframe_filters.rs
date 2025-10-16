use crate::config::table_context::{CellValue, Context, Identifier, SeriesContext};
use crate::extract::ContextualizedDataFrame;
use polars::prelude::{Column, DataType};
use serde::Deserialize;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum Filter<T> {
    Any,
    Is(T),
    IsNot(T),
    IsSome,
}

pub struct SeriesContextFilter<'a> {
    items: Vec<&'a SeriesContext>,
    identifier: Filter<&'a Identifier>,
    building_block: Filter<Option<&'a str>>,
    header_context: Filter<&'a Context>,
    data_context: Filter<&'a Context>,
    fill_missing: Filter<Option<&'a CellValue>>,
}

impl<'a> SeriesContextFilter<'a> {
    pub(crate) fn new(items: &'a [SeriesContext]) -> Self {
        Self {
            items: items.iter().collect(),
            identifier: Filter::Any,
            building_block: Filter::Any,
            header_context: Filter::Any,
            data_context: Filter::Any,
            fill_missing: Filter::Any,
        }
    }

    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.identifier = identifier;
        self
    }

    pub fn where_building_block(mut self, building_block: Filter<Option<&'a str>>) -> Self {
        self.building_block = building_block;
        self
    }

    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.header_context = header_context;
        self
    }

    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.data_context = data_context;
        self
    }
    pub fn where_fill_missing(mut self, fill_missing: Filter<Option<&'a CellValue>>) -> Self {
        self.fill_missing = fill_missing;
        self
    }

    pub fn collect(self) -> Vec<&'a SeriesContext> {
        self.items
            .into_iter()
            .filter(|sc| {
                [
                    match &self.identifier {
                        Filter::Any => true,
                        Filter::Is(val) => sc.get_identifier() == *val,
                        Filter::IsNot(val) => sc.get_identifier() != *val,

                        Filter::IsSome => true,
                    },
                    match &self.building_block {
                        Filter::Any => true,
                        Filter::Is(bb_id) => sc.get_building_block_id() == bb_id.as_deref(),
                        Filter::IsNot(bb_id) => sc.get_building_block_id() != *bb_id,
                        Filter::IsSome => sc.get_building_block_id().is_some(),
                    },
                    match &self.header_context {
                        Filter::Any => true,
                        Filter::Is(c) => sc.get_header_context() == *c,
                        Filter::IsNot(c) => sc.get_header_context() != *c,
                        Filter::IsSome => true,
                    },
                    match &self.data_context {
                        Filter::Any => true,
                        Filter::Is(c) => sc.get_data_context() == *c,
                        Filter::IsNot(c) => sc.get_data_context() != *c,
                        Filter::IsSome => true,
                    },
                    match &self.fill_missing {
                        Filter::Any => true,
                        Filter::Is(fill) => sc.get_fill_missing() == *fill,
                        Filter::IsNot(fill) => sc.get_fill_missing() != *fill,
                        Filter::IsSome => sc.get_fill_missing().is_some(),
                    },
                ]
                .into_iter()
                .all(|b| b)
            })
            .collect()
    }
}

pub struct ColumnFilter<'a> {
    items: &'a ContextualizedDataFrame,
    series_filter: SeriesContextFilter<'a>,
    dtype: Filter<&'a DataType>,
}

impl<'a> ColumnFilter<'a> {
    pub(crate) fn new(items: &'a ContextualizedDataFrame) -> Self {
        Self {
            items,
            series_filter: SeriesContextFilter::new(items.context().context.deref()),
            dtype: Filter::Any,
        }
    }

    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.series_filter.identifier = identifier;
        self
    }

    pub fn where_building_block(mut self, building_block: Filter<Option<&'a str>>) -> Self {
        self.series_filter.building_block = building_block;
        self
    }

    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.series_filter.header_context = header_context;
        self
    }

    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.series_filter.data_context = data_context;
        self
    }
    pub fn where_fill_missing(mut self, fill_missing: Filter<Option<&'a CellValue>>) -> Self {
        self.series_filter.fill_missing = fill_missing;
        self
    }
    pub fn where_dtype(mut self, data_type: Filter<&'a DataType>) -> Self {
        self.dtype = data_type;
        self
    }

    pub fn collect(self) -> Vec<&'a Column> {
        let scs = self.series_filter.collect();
        scs.iter()
            .flat_map(|sc| {
                self.items
                    .get_columns(sc.get_identifier())
                    .into_iter()
                    .filter(|col| match self.dtype {
                        Filter::Any => true,
                        Filter::Is(dtype) => dtype == col.dtype(),
                        Filter::IsNot(dtype) => dtype != col.dtype(),
                        Filter::IsSome => true,
                    })
                    .collect::<Vec<&Column>>()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::{CellValue, Context, Identifier, SeriesContext};
    use rstest::rstest;

    #[rstest]
    fn test_filter_by_identifier() {
        let id1 = Identifier::Regex("id1".to_string());
        let id2 = Identifier::Regex("id2".to_string());

        let series = vec![
            SeriesContext::default().with_identifier(id1.clone()),
            SeriesContext::default().with_identifier(id2),
            SeriesContext::default().with_identifier(id1.clone()),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_identifier(Filter::Is(&id1))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_identifier() == &id1));
    }

    #[rstest]
    fn test_filter_by_building_block_some() {
        let series = vec![
            SeriesContext::default().with_building_block_id(Some("bb1".to_string())),
            SeriesContext::default().with_identifier(Identifier::Regex("id2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id3".to_string()))
                .with_building_block_id(Some("bb3".to_string())),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_building_block(Filter::IsSome)
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_building_block_id().is_some()));
    }

    #[rstest]
    fn test_filter_by_building_block_value() {
        let series = vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id1".to_string()))
                .with_building_block_id(Some("bb1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id2".to_string()))
                .with_building_block_id(Some("bb2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id3".to_string()))
                .with_building_block_id(Some("bb1".to_string())),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_building_block(Filter::Is(Some("bb1")))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .all(|s| s.get_building_block_id() == Some("bb1"))
        );
    }

    #[rstest]
    fn test_filter_by_building_block_none() {
        let series = vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id1".to_string()))
                .with_building_block_id(Some("bb1".to_string())),
            SeriesContext::default().with_identifier(Identifier::Regex("id2".to_string())),
            SeriesContext::default().with_identifier(Identifier::Regex("id3".to_string())),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_building_block(Filter::Is(None))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_building_block_id().is_none()));
    }

    #[test]
    fn test_filter_by_header_context() {
        let ctx1 = Context::HpoLabel;
        let ctx2 = Context::HpoId;

        let series = vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id1".to_string()))
                .with_header_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id2".to_string()))
                .with_header_context(ctx2.clone()),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id3".to_string()))
                .with_header_context(ctx1.clone()),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_header_context(Filter::Is(&ctx1))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_header_context() == &ctx1));
    }

    #[rstest]
    fn test_filter_by_data_context() {
        let ctx1 = Context::HpoLabel;
        let ctx2 = Context::HpoId;

        let series = vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id1".to_string()))
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id2".to_string()))
                .with_data_context(ctx2.clone()),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id3".to_string()))
                .with_data_context(ctx1.clone()),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_data_context(Filter::Is(&ctx1))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_data_context() == &ctx1));
    }

    #[rstest]
    fn test_filter_by_fill_missing_some() {
        let fill_val = CellValue::String("default".to_string());

        let series = vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id1".to_string()))
                .with_fill_missing(Some(fill_val.clone())),
            SeriesContext::default().with_identifier(Identifier::Regex("id2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id3".to_string()))
                .with_fill_missing(Some(fill_val.clone())),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_fill_missing(Filter::IsSome)
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_fill_missing().is_some()));
    }

    #[rstest]
    fn test_filter_by_fill_missing_value() {
        let fill_val = CellValue::String("default".to_string());
        let other_val = CellValue::String("other".to_string());

        let series = vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id1".to_string()))
                .with_fill_missing(Some(fill_val.clone())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id2".to_string()))
                .with_fill_missing(Some(other_val.clone())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id3".to_string()))
                .with_fill_missing(Some(fill_val.clone())),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_fill_missing(Filter::Is(Some(&fill_val)))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .all(|s| s.get_fill_missing() == Some(&fill_val))
        );
    }

    #[rstest]
    fn test_filter_multiple_conditions() {
        let id1 = Identifier::Regex("id1".to_string());
        let ctx1 = Context::HpoLabel;

        let series = vec![
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_building_block_id(Some("bb1".to_string()))
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id2".to_string()))
                .with_building_block_id(Some("bb1".to_string()))
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_building_block_id(Some("bb1".to_string())),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_identifier(Filter::Is(&id1))
            .where_building_block(Filter::IsSome)
            .where_data_context(Filter::Is(&ctx1))
            .collect();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_identifier(), &id1);
        assert!(result[0].get_building_block_id().is_some());
        assert_eq!(result[0].get_data_context(), &ctx1);
    }

    #[rstest]
    fn test_filter_no_matches() {
        let id1 = Identifier::Regex("id1".to_string());
        let id_nonexistent = Identifier::Regex("nonexistent".to_string());

        let series = vec![
            SeriesContext::default().with_identifier(id1.clone()),
            SeriesContext::default().with_identifier(Identifier::Regex("id2".to_string())),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_identifier(Filter::Is(&id_nonexistent))
            .collect();

        assert_eq!(result.len(), 0);
    }

    #[rstest]
    fn test_filter_empty_input() {
        let series: Vec<SeriesContext> = vec![];

        let result = SeriesContextFilter::new(&series).collect();

        assert_eq!(result.len(), 0);
    }

    #[rstest]
    fn test_filter_chain_order_independence() {
        let id1 = Identifier::Regex("id1".to_string());
        let ctx1 = Context::HpoLabel;

        let series = vec![
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_building_block_id(Some("bb1".to_string()))
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("id2".to_string()))
                .with_building_block_id(Some("bb1".to_string()))
                .with_data_context(ctx1.clone()),
        ];

        let result1 = SeriesContextFilter::new(&series)
            .where_identifier(Filter::Is(&id1))
            .where_data_context(Filter::Is(&ctx1))
            .collect();

        let result2 = SeriesContextFilter::new(&series)
            .where_data_context(Filter::Is(&ctx1))
            .where_identifier(Filter::Is(&id1))
            .collect();

        assert_eq!(result1.len(), result2.len());
        assert_eq!(result1.len(), 1);
    }
}
