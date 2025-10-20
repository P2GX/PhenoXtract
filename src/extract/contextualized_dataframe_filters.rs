use crate::config::table_context::{CellValue, Context, Identifier, SeriesContext};
use crate::extract::ContextualizedDataFrame;
use polars::prelude::{Column, DataType};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum Filter<T> {
    Is(T),
    IsNot(T),
    IsSome,
    IsNone,
}

pub struct SeriesContextFilter<'a> {
    items: Vec<&'a SeriesContext>,
    identifier: Vec<Filter<&'a Identifier>>,
    building_block: Vec<Filter<&'a str>>,
    header_context: Vec<Filter<&'a Context>>,
    data_context: Vec<Filter<&'a Context>>,
    fill_missing: Vec<Filter<&'a CellValue>>,
}

impl<'a> SeriesContextFilter<'a> {
    pub(crate) fn new(items: &'a [SeriesContext]) -> Self {
        Self {
            items: items.iter().collect(),
            identifier: Vec::new(),
            building_block: Vec::new(),
            header_context: Vec::new(),
            data_context: Vec::new(),
            fill_missing: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.identifier.push(identifier);
        self
    }
    #[allow(dead_code)]
    pub fn where_building_block(mut self, building_block: Filter<&'a str>) -> Self {
        self.building_block.push(building_block);
        self
    }

    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.header_context.push(header_context);
        self
    }

    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.data_context.push(data_context);
        self
    }
    #[allow(dead_code)]
    pub fn where_fill_missing(mut self, fill_missing: Filter<&'a CellValue>) -> Self {
        self.fill_missing.push(fill_missing);
        self
    }

    pub fn collect(self) -> Vec<&'a SeriesContext> {
        self.items
            .into_iter()
            .filter(|sc| {
                let identifier_match = self.identifier.is_empty()
                    || self.identifier.iter().any(|f| match f {
                        Filter::Is(val) => sc.get_identifier() == *val,
                        Filter::IsNot(val) => sc.get_identifier() != *val,
                        Filter::IsSome => true,
                        Filter::IsNone => false,
                    });

                let building_block_match = self.building_block.is_empty()
                    || self.building_block.iter().any(|f| match f {
                        Filter::Is(bb_id) => sc.get_building_block_id() == Some(*bb_id),
                        Filter::IsNot(bb_id) => sc.get_building_block_id() != Some(*bb_id),
                        Filter::IsSome => sc.get_building_block_id().is_some(),
                        Filter::IsNone => sc.get_building_block_id().is_none(),
                    });

                let header_context_match = self.header_context.is_empty()
                    || self.header_context.iter().any(|f| match f {
                        Filter::Is(c) => sc.get_header_context() == *c,
                        Filter::IsNot(c) => sc.get_header_context() != *c,
                        Filter::IsSome => sc.get_header_context() != &Context::None,
                        Filter::IsNone => sc.get_header_context() == &Context::None,
                    });

                let data_context_match = self.data_context.is_empty()
                    || self.data_context.iter().any(|f| match f {
                        Filter::Is(c) => sc.get_data_context() == *c,
                        Filter::IsNot(c) => sc.get_data_context() != *c,
                        Filter::IsSome => sc.get_data_context() != &Context::None,
                        Filter::IsNone => sc.get_data_context() == &Context::None,
                    });

                let fill_missing_match = self.fill_missing.is_empty()
                    || self.fill_missing.iter().any(|f| match f {
                        Filter::Is(fill) => sc.get_fill_missing() == Some(*fill),
                        Filter::IsNot(fill) => sc.get_fill_missing() != Some(*fill),
                        Filter::IsSome => sc.get_fill_missing().is_some(),
                        Filter::IsNone => sc.get_fill_missing().is_none(),
                    });

                // Combine all field checks with AND logic.
                identifier_match
                    && building_block_match
                    && header_context_match
                    && data_context_match
                    && fill_missing_match
            })
            .collect()
    }
}

pub struct ColumnFilter<'a> {
    items: &'a ContextualizedDataFrame,
    series_filter: SeriesContextFilter<'a>,
    dtype: Vec<Filter<&'a DataType>>,
}

impl<'a> ColumnFilter<'a> {
    pub(crate) fn new(items: &'a ContextualizedDataFrame) -> Self {
        Self {
            items,
            series_filter: SeriesContextFilter::new(items.series_contexts()),
            dtype: Vec::new(),
        }
    }

    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.series_filter.identifier.push(identifier);
        self
    }

    pub fn where_building_block(mut self, building_block: Filter<&'a str>) -> Self {
        self.series_filter.building_block.push(building_block);
        self
    }

    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.series_filter.header_context.push(header_context);
        self
    }

    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.series_filter.data_context.push(data_context);
        self
    }
    pub fn where_fill_missing(mut self, fill_missing: Filter<&'a CellValue>) -> Self {
        self.series_filter.fill_missing.push(fill_missing);
        self
    }
    pub fn where_dtype(mut self, data_type: Filter<&'a DataType>) -> Self {
        self.dtype.push(data_type);
        self
    }

    pub fn collect(self) -> Vec<&'a Column> {
        let scs = self.series_filter.collect();
        scs.iter()
            .flat_map(|sc| {
                self.items
                    .get_columns(sc.get_identifier())
                    .into_iter()
                    .filter(|col| {
                        self.dtype.is_empty()
                            || self.dtype.iter().any(|f| match f {
                                Filter::Is(dtype) => *dtype == col.dtype(),
                                Filter::IsNot(dtype) => *dtype != col.dtype(),
                                Filter::IsSome => true, // Assuming col.dtype() is not an Option
                                Filter::IsNone => false, // Assuming col.dtype() is not an Option
                            })
                    })
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
            .where_building_block(Filter::Is("bb1"))
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
            .where_building_block(Filter::IsNone)
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
            .where_fill_missing(Filter::Is(&fill_val))
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
