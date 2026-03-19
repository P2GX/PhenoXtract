use crate::config::context::{Context, ContextKind};
use crate::config::table_context::{CellValue, Identifier, SeriesContext};
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

impl<T: std::fmt::Debug> std::fmt::Display for Filter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Filter::Is(value) => write!(f, "== {:?}", value),
            Filter::IsNot(value) => write!(f, "!= {:?}", value),
            Filter::IsSome => write!(f, "is some"),
            Filter::IsNone => write!(f, "is none"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SeriesContextFilterConfig<'a> {
    identifier: Vec<Filter<&'a Identifier>>,
    building_block: Vec<Filter<&'a str>>,
    header_context: Vec<Filter<&'a Context>>,
    data_context: Vec<Filter<&'a Context>>,
    header_context_kind: Vec<Filter<&'a ContextKind>>,
    data_context_kind: Vec<Filter<&'a ContextKind>>,
    fill_missing: Vec<Filter<&'a CellValue>>,
}

fn fmt_vec<T: std::fmt::Display>(
    f: &mut std::fmt::Formatter<'_>,
    name: &str,
    values: &[T],
) -> std::fmt::Result {
    if values.is_empty() {
        return Ok(());
    }

    write!(f, "{}=[", name)?;
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", v)?;
    }
    write!(f, "] ")
}

impl<'a> std::fmt::Display for SeriesContextFilterConfig<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeriesContextFilterConfig{{ ")?;

        fmt_vec(f, "identifier", &self.identifier)?;
        fmt_vec(f, "building_block", &self.building_block)?;
        fmt_vec(f, "header_context", &self.header_context)?;
        fmt_vec(f, "data_context", &self.data_context)?;
        fmt_vec(f, "header_context_kind", &self.header_context_kind)?;
        fmt_vec(f, "data_context_kind", &self.data_context_kind)?;
        fmt_vec(f, "fill_missing", &self.fill_missing)?;

        write!(f, "}}")
    }
}

impl<'a> SeriesContextFilterConfig<'a> {
    pub(crate) fn new() -> Self {
        Self {
            identifier: Vec::new(),
            building_block: Vec::new(),
            header_context: Vec::new(),
            data_context: Vec::new(),
            header_context_kind: Vec::new(),
            data_context_kind: Vec::new(),
            fill_missing: Vec::new(),
        }
    }

    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.identifier.push(identifier);
        self
    }
    pub fn where_building_block(mut self, building_block: Filter<&'a str>) -> Self {
        self.building_block.push(building_block);
        self
    }

    #[allow(dead_code)]
    pub fn where_building_blocks_are(mut self, building_blocks: &'a [&str]) -> Self {
        for building_block in building_blocks.iter() {
            self.building_block.push(Filter::Is(building_block));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.header_context.push(header_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.header_context.push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.data_context.push(data_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.data_context.push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kind(
        mut self,
        header_context_kind: Filter<&'a ContextKind>,
    ) -> Self {
        self.header_context_kind.push(header_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.header_context_kind.push(Filter::Is(context_kind));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kind(mut self, data_context_kind: Filter<&'a ContextKind>) -> Self {
        self.data_context_kind.push(data_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.data_context_kind.push(Filter::Is(context_kind));
        }
        self
    }

    pub fn where_fill_missing(mut self, fill_missing: Filter<&'a CellValue>) -> Self {
        self.fill_missing.push(fill_missing);
        self
    }

    #[allow(dead_code)]
    pub fn where_fill_missings_are(mut self, fill_missings: &'a [CellValue]) -> Self {
        for fill_missing in fill_missings.iter() {
            self.fill_missing.push(Filter::Is(fill_missing));
        }
        self
    }
}

#[derive(Clone, Debug)]
pub struct SeriesContextFilter<'a> {
    items: Vec<&'a SeriesContext>,
    filters: SeriesContextFilterConfig<'a>,
}

impl<'a> SeriesContextFilter<'a> {
    pub(crate) fn new(items: &'a [SeriesContext]) -> Self {
        Self {
            items: items.iter().collect(),
            filters: SeriesContextFilterConfig::new(),
        }
    }

    pub(crate) fn new_with_filters(
        items: &'a [SeriesContext],
        filters: SeriesContextFilterConfig<'a>,
    ) -> Self {
        Self {
            items: items.iter().collect(),
            filters,
        }
    }

    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.filters.identifier.push(identifier);
        self
    }
    pub fn where_building_block(mut self, building_block: Filter<&'a str>) -> Self {
        self.filters.building_block.push(building_block);
        self
    }

    #[allow(dead_code)]
    pub fn where_building_blocks_are(mut self, building_blocks: &'a [&str]) -> Self {
        for building_block in building_blocks.iter() {
            self.filters.building_block.push(Filter::Is(building_block));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.filters.header_context.push(header_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.filters.header_context.push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.filters.data_context.push(data_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.filters.data_context.push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kind(
        mut self,
        header_context_kind: Filter<&'a ContextKind>,
    ) -> Self {
        self.filters.header_context_kind.push(header_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.filters
                .header_context_kind
                .push(Filter::Is(context_kind));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kind(mut self, data_context_kind: Filter<&'a ContextKind>) -> Self {
        self.filters.data_context_kind.push(data_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.filters
                .data_context_kind
                .push(Filter::Is(context_kind));
        }
        self
    }

    pub fn where_fill_missing(mut self, fill_missing: Filter<&'a CellValue>) -> Self {
        self.filters.fill_missing.push(fill_missing);
        self
    }

    #[allow(dead_code)]
    pub fn where_fill_missings_are(mut self, fill_missings: &'a [CellValue]) -> Self {
        for fill_missing in fill_missings.iter() {
            self.filters.fill_missing.push(Filter::Is(fill_missing));
        }
        self
    }

    pub fn collect(self) -> Vec<&'a SeriesContext> {
        self.items
            .into_iter()
            .filter(|sc| {
                let identifier_match = self.filters.identifier.is_empty()
                    || self.filters.identifier.iter().any(|f| match f {
                        Filter::Is(val) => sc.get_identifier() == *val,
                        Filter::IsNot(val) => sc.get_identifier() != *val,
                        Filter::IsSome => true,
                        Filter::IsNone => false,
                    });

                let building_block_match = self.filters.building_block.is_empty()
                    || self.filters.building_block.iter().any(|f| match f {
                        Filter::Is(bb_id) => sc.get_building_block_id() == Some(*bb_id),
                        Filter::IsNot(bb_id) => sc.get_building_block_id() != Some(*bb_id),
                        Filter::IsSome => sc.get_building_block_id().is_some(),
                        Filter::IsNone => sc.get_building_block_id().is_none(),
                    });

                let header_context_match = self.filters.header_context.is_empty()
                    || self.filters.header_context.iter().any(|f| match f {
                        Filter::Is(c) => sc.get_header_context() == *c,
                        Filter::IsNot(c) => sc.get_header_context() != *c,
                        Filter::IsSome => sc.get_header_context() != &Context::None,
                        Filter::IsNone => sc.get_header_context() == &Context::None,
                    });

                let data_context_match = self.filters.data_context.is_empty()
                    || self.filters.data_context.iter().any(|f| match f {
                        Filter::Is(c) => sc.get_data_context() == *c,
                        Filter::IsNot(c) => sc.get_data_context() != *c,
                        Filter::IsSome => sc.get_data_context() != &Context::None,
                        Filter::IsNone => sc.get_data_context() == &Context::None,
                    });

                let header_context_kind_match = self.filters.header_context_kind.is_empty()
                    || self.filters.header_context_kind.iter().any(|f| match f {
                        Filter::Is(c) => ContextKind::from(sc.get_header_context()) == **c,
                        Filter::IsNot(c) => ContextKind::from(sc.get_header_context()) != **c,
                        Filter::IsSome => {
                            ContextKind::from(sc.get_header_context()) != ContextKind::None
                        }
                        Filter::IsNone => {
                            ContextKind::from(sc.get_header_context()) == ContextKind::None
                        }
                    });

                let data_context_kind_match = self.filters.data_context_kind.is_empty()
                    || self.filters.data_context_kind.iter().any(|f| match f {
                        Filter::Is(c) => ContextKind::from(sc.get_data_context()) == **c,
                        Filter::IsNot(c) => ContextKind::from(sc.get_data_context()) != **c,
                        Filter::IsSome => {
                            ContextKind::from(sc.get_data_context()) != ContextKind::None
                        }
                        Filter::IsNone => {
                            ContextKind::from(sc.get_data_context()) == ContextKind::None
                        }
                    });

                let fill_missing_match = self.filters.fill_missing.is_empty()
                    || self.filters.fill_missing.iter().any(|f| match f {
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
                    && data_context_kind_match
                    && header_context_kind_match
                    && fill_missing_match
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct ColumnFilterConfig<'a> {
    data_type: Vec<Filter<&'a DataType>>,
}

impl<'a> std::fmt::Display for ColumnFilterConfig<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ColumnFilterConfig{{ ")?;
        fmt_vec(f, "data_type", &self.data_type)?;
        write!(f, "}}")
    }
}

impl<'a> ColumnFilterConfig<'a> {
    pub(crate) fn new() -> Self {
        Self {
            data_type: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct ColumnFilter<'a> {
    items: &'a ContextualizedDataFrame,
    series_filter: SeriesContextFilter<'a>,
    filters: ColumnFilterConfig<'a>,
}

impl<'a> ColumnFilter<'a> {
    pub(crate) fn new(items: &'a ContextualizedDataFrame) -> Self {
        Self {
            items,
            series_filter: SeriesContextFilter::new(items.series_contexts()),
            filters: ColumnFilterConfig::new(),
        }
    }

    pub(crate) fn new_with_filters(
        items: &'a ContextualizedDataFrame,
        sc_filter_config: SeriesContextFilterConfig<'a>,
        column_filter_config: ColumnFilterConfig<'a>,
    ) -> Self {
        Self {
            items,
            series_filter: SeriesContextFilter::new_with_filters(
                items.series_contexts(),
                sc_filter_config,
            ),
            filters: column_filter_config,
        }
    }

    #[allow(dead_code)]
    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.series_filter.filters.identifier.push(identifier);
        self
    }

    #[allow(dead_code)]
    pub fn where_identifiers_are(mut self, identifiers: &'a [&Identifier]) -> Self {
        for identifier in identifiers.iter() {
            self.series_filter
                .filters
                .identifier
                .push(Filter::Is(identifier));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_building_block(mut self, building_block: Filter<&'a str>) -> Self {
        self.series_filter
            .filters
            .building_block
            .push(building_block);
        self
    }

    #[allow(dead_code)]
    pub fn where_building_blocks_are(mut self, building_blocks: &'a [&str]) -> Self {
        for building_block in building_blocks.iter() {
            self.series_filter
                .filters
                .building_block
                .push(Filter::Is(building_block));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.series_filter
            .filters
            .header_context
            .push(header_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.series_filter
                .filters
                .header_context
                .push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.series_filter.filters.data_context.push(data_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.series_filter
                .filters
                .data_context
                .push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kind(
        mut self,
        header_context_kind: Filter<&'a ContextKind>,
    ) -> Self {
        self.series_filter
            .filters
            .header_context_kind
            .push(header_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.series_filter
                .filters
                .header_context_kind
                .push(Filter::Is(context_kind));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kind(mut self, data_context_kind: Filter<&'a ContextKind>) -> Self {
        self.series_filter
            .filters
            .data_context_kind
            .push(data_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.series_filter
                .filters
                .data_context_kind
                .push(Filter::Is(context_kind));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_fill_missing(mut self, fill_missing: Filter<&'a CellValue>) -> Self {
        self.series_filter.filters.fill_missing.push(fill_missing);
        self
    }

    #[allow(dead_code)]
    pub fn where_fill_missings_are(mut self, fill_missings: &'a [CellValue]) -> Self {
        for fill_missing in fill_missings.iter() {
            self.series_filter
                .filters
                .fill_missing
                .push(Filter::Is(fill_missing));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_type(mut self, data_type: Filter<&'a DataType>) -> Self {
        self.filters.data_type.push(data_type);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_types_are(mut self, data_types: &'a [DataType]) -> Self {
        for data_type in data_types.iter() {
            self.filters.data_type.push(Filter::Is(data_type));
        }
        self
    }

    pub fn collect(self) -> Vec<&'a Column> {
        let scs = self.series_filter.collect();
        scs.iter()
            .flat_map(|sc| {
                self.items
                    .identify_columns(sc.get_identifier())
                    .into_iter()
                    .filter(|col| {
                        self.filters.data_type.is_empty()
                            || self.filters.data_type.iter().any(|f| match f {
                                Filter::Is(dtype) => *dtype == col.dtype(),
                                Filter::IsNot(dtype) => *dtype != col.dtype(),
                                Filter::IsSome => true, // Assuming col.dtype() is not an Option
                                Filter::IsNone => false, // Assuming col.dtype() is not an Option
                            })
                    })
            })
            .collect()
    }

    pub fn collect_owned_names(self) -> Vec<String> {
        self.collect()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::{ContextKind, TimeElementType};
    use crate::config::table_context::{CellValue, Identifier, SeriesContext};
    use crate::config::traits::SeriesContextBuilding;
    use rstest::rstest;

    #[rstest]
    fn test_filter_by_identifier() {
        let id1 = Identifier::from("id1");
        let id2 = Identifier::from("id2");

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
            SeriesContext::default().with_building_block_id("bb1"),
            SeriesContext::default().with_identifier("id2"),
            SeriesContext::default()
                .with_identifier("id3")
                .with_building_block_id("bb3"),
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
                .with_identifier("id1")
                .with_building_block_id("bb1"),
            SeriesContext::default()
                .with_identifier("id2")
                .with_building_block_id("bb2"),
            SeriesContext::default()
                .with_identifier("id3")
                .with_building_block_id("bb1"),
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
                .with_identifier("id1")
                .with_building_block_id("bb1"),
            SeriesContext::default().with_identifier("id2"),
            SeriesContext::default().with_identifier("id3"),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_building_block(Filter::IsNone)
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_building_block_id().is_none()));
    }

    #[rstest]
    fn test_filter_by_header_context() {
        let ctx1 = Context::SubjectId;
        let ctx2 = Context::Hpo;

        let series = vec![
            SeriesContext::default()
                .with_identifier("id1")
                .with_header_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier("id2")
                .with_header_context(ctx2.clone()),
            SeriesContext::default()
                .with_identifier("id3")
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
        let ctx1 = Context::SubjectId;
        let ctx2 = Context::Hpo;

        let series = vec![
            SeriesContext::from_identifier("id1").with_data_context(ctx1.clone()),
            SeriesContext::from_identifier("id2").with_data_context(ctx2.clone()),
            SeriesContext::from_identifier("id3").with_data_context(ctx1.clone()),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_data_context(Filter::Is(&ctx1))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|s| s.get_data_context() == &ctx1));
    }

    #[rstest]
    fn test_filter_by_data_context_is_not() {
        let series = vec![
            SeriesContext::default().with_data_context(Context::SubjectId),
            SeriesContext::default().with_data_context(Context::Hpo),
            SeriesContext::default()
                .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Age)),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_data_context(Filter::IsNot(&Context::SubjectId))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .all(|s| s.get_data_context() != &Context::SubjectId)
        );
    }

    #[rstest]
    fn test_where_data_contexts_are() {
        let series = vec![
            SeriesContext::from_identifier("id1".to_string()).with_data_context(Context::SubjectId),
            SeriesContext::from_identifier("id2".to_string()).with_data_context(Context::Hpo),
            SeriesContext::from_identifier("id3".to_string()).with_data_context(Context::SubjectId),
            SeriesContext::from_identifier("id4".to_string())
                .with_data_context(Context::VitalStatus.clone()),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_data_contexts_are(&[Context::SubjectId, Context::Hpo])
            .collect();

        assert_eq!(result.len(), 3);
        assert!(
            result
                .iter()
                .all(|s| s.get_data_context() == &Context::SubjectId
                    || s.get_data_context() == &Context::Hpo)
        );
    }

    #[rstest]
    fn test_filter_by_header_context_kind() {
        let series = vec![
            SeriesContext::from_identifier("id1".to_string()).with_header_context(
                Context::QuantitativeMeasurement {
                    assay_id: "LOINC:12345-6".to_string(),
                    unit_ontology_id: "NCIT:12345".to_string(),
                },
            ),
            SeriesContext::from_identifier("id1".to_string()).with_header_context(
                Context::QuantitativeMeasurement {
                    assay_id: "LOINC:98765-6".to_string(),
                    unit_ontology_id: "NCIT:9876".to_string(),
                },
            ),
            SeriesContext::from_identifier("id3".to_string()).with_header_context(Context::Hpo),
            SeriesContext::from_identifier("id4".to_string()).with_header_context(Context::Disease),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_header_context_kind(Filter::Is(&ContextKind::QuantitativeMeasurement))
            .collect();

        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .all(|s| ContextKind::from(s.get_header_context())
                    == ContextKind::QuantitativeMeasurement)
        );
    }

    #[rstest]
    fn test_where_header_context_kinds_are() {
        let series = vec![
            SeriesContext::from_identifier("id1".to_string()).with_header_context(
                Context::QuantitativeMeasurement {
                    assay_id: "LOINC:12345-6".to_string(),
                    unit_ontology_id: "NCIT:12345".to_string(),
                },
            ),
            SeriesContext::from_identifier("id1".to_string()).with_header_context(
                Context::QuantitativeMeasurement {
                    assay_id: "LOINC:98765-6".to_string(),
                    unit_ontology_id: "NCIT:9876".to_string(),
                },
            ),
            SeriesContext::from_identifier("id3".to_string()).with_header_context(Context::Hpo),
            SeriesContext::from_identifier("id4".to_string()).with_header_context(Context::Disease),
        ];

        let result = SeriesContextFilter::new(&series)
            .where_header_context_kinds_are(&[
                ContextKind::Hpo,
                ContextKind::QuantitativeMeasurement,
            ])
            .collect();

        assert_eq!(result.len(), 3);
        assert!(
            result
                .iter()
                .all(|s| ContextKind::from(s.get_header_context())
                    == ContextKind::QuantitativeMeasurement
                    || ContextKind::from(s.get_header_context()) == ContextKind::Hpo)
        );
    }

    #[rstest]
    fn test_filter_by_fill_missing_some() {
        let fill_val = CellValue::String("default".to_string());

        let series = vec![
            SeriesContext::default()
                .with_identifier("id1")
                .with_fill_missing(fill_val.clone()),
            SeriesContext::default().with_identifier("id2"),
            SeriesContext::default()
                .with_identifier("id3")
                .with_fill_missing(fill_val),
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
                .with_identifier("id1")
                .with_fill_missing(fill_val.clone()),
            SeriesContext::default()
                .with_identifier("id2")
                .with_fill_missing(other_val),
            SeriesContext::default()
                .with_identifier("id3")
                .with_fill_missing(fill_val.clone()),
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
        let id1 = Identifier::from("id1");
        let ctx1 = Context::Hpo;

        let series = vec![
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_building_block_id("bb1")
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier("id2")
                .with_building_block_id("bb1")
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_building_block_id("bb1"),
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
        let id1 = Identifier::from("id1");
        let id_nonexistent = Identifier::from("nonexistent");

        let series = vec![
            SeriesContext::default().with_identifier(id1.clone()),
            SeriesContext::default().with_identifier("id2"),
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
        let id1 = Identifier::from("id1");
        let ctx1 = Context::Hpo;

        let series = vec![
            SeriesContext::default()
                .with_identifier(id1.clone())
                .with_building_block_id("bb1")
                .with_data_context(ctx1.clone()),
            SeriesContext::default()
                .with_identifier("id2")
                .with_building_block_id("bb1")
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
