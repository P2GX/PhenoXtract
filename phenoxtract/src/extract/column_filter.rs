use crate::config::context::{Context, ContextKind};
use crate::config::table_context::{CellValue, Identifier};
use crate::extract::ContextualizedDataFrame;
use crate::extract::enums::Filter;
use crate::extract::series_context_filter::{SeriesContextFilter, SeriesContextFilterConfig};
use crate::extract::utils::fmt_vec;
use polars::datatypes::DataType;
use polars::prelude::Column;

#[derive(Clone, Debug, Default)]
pub struct ColumnFilterConfig<'a> {
    identifier: Vec<Filter<&'a Identifier>>,
    building_block: Vec<Filter<&'a str>>,
    header_context: Vec<Filter<&'a Context>>,
    data_context: Vec<Filter<&'a Context>>,
    header_context_kind: Vec<Filter<&'a ContextKind>>,
    data_context_kind: Vec<Filter<&'a ContextKind>>,
    fill_missing: Vec<Filter<&'a CellValue>>,
    data_type: Vec<Filter<&'a DataType>>,
}

impl<'a> ColumnFilterConfig<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identifier: Vec<Filter<&'a Identifier>>,
        building_block: Vec<Filter<&'a str>>,
        header_context: Vec<Filter<&'a Context>>,
        data_context: Vec<Filter<&'a Context>>,
        header_context_kind: Vec<Filter<&'a ContextKind>>,
        data_context_kind: Vec<Filter<&'a ContextKind>>,
        fill_missing: Vec<Filter<&'a CellValue>>,
        data_type: Vec<Filter<&'a DataType>>,
    ) -> Self {
        ColumnFilterConfig {
            identifier,
            building_block,
            header_context,
            data_context,
            header_context_kind,
            data_context_kind,
            fill_missing,
            data_type,
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

    pub fn where_building_blocks_are(mut self, building_blocks: &'a [&str]) -> Self {
        for building_block in building_blocks.iter() {
            self.building_block.push(Filter::Is(building_block));
        }
        self
    }

    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.header_context.push(header_context);
        self
    }

    pub fn where_header_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.header_context.push(Filter::Is(context));
        }
        self
    }

    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.data_context.push(data_context);
        self
    }

    pub fn where_data_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.data_context.push(Filter::Is(context));
        }
        self
    }

    pub fn where_header_context_kind(
        mut self,
        header_context_kind: Filter<&'a ContextKind>,
    ) -> Self {
        self.header_context_kind.push(header_context_kind);
        self
    }

    pub fn where_header_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.header_context_kind.push(Filter::Is(context_kind));
        }
        self
    }

    pub fn where_data_context_kind(mut self, data_context_kind: Filter<&'a ContextKind>) -> Self {
        self.data_context_kind.push(data_context_kind);
        self
    }

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

    pub fn where_fill_missings_are(mut self, fill_missings: &'a [CellValue]) -> Self {
        for fill_missing in fill_missings.iter() {
            self.fill_missing.push(Filter::Is(fill_missing));
        }
        self
    }

    pub fn where_data_types_are(mut self, data_types: &'a [DataType]) -> Self {
        for data_type in data_types.iter() {
            self.data_type.push(Filter::Is(data_type));
        }
        self
    }
}

impl<'a> From<&'a ColumnFilterConfig<'a>> for SeriesContextFilterConfig<'a> {
    fn from(config: &'a ColumnFilterConfig<'a>) -> Self {
        SeriesContextFilterConfig::new(
            config.identifier.clone(),
            config.building_block.clone(),
            config.header_context.clone(),
            config.data_context.clone(),
            config.header_context_kind.clone(),
            config.data_context_kind.clone(),
            config.fill_missing.clone(),
        )
    }
}

impl<'a> std::fmt::Display for ColumnFilterConfig<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ColumnFilterConfig{{ ")?;
        fmt_vec(f, "data_type", &self.data_type)?;
        write!(f, "}}")
    }
}

#[derive(Clone)]
pub struct ColumnFilter<'a> {
    items: &'a ContextualizedDataFrame,
    config: ColumnFilterConfig<'a>,
}

impl<'a> ColumnFilter<'a> {
    pub(crate) fn new(items: &'a ContextualizedDataFrame) -> Self {
        Self {
            items,
            config: ColumnFilterConfig::default(),
        }
    }

    pub(crate) fn from_config(
        items: &'a ContextualizedDataFrame,
        column_filter_config: ColumnFilterConfig<'a>,
    ) -> Self {
        Self {
            items,
            config: column_filter_config,
        }
    }

    #[allow(dead_code)]
    pub fn where_identifier(mut self, identifier: Filter<&'a Identifier>) -> Self {
        self.config.identifier.push(identifier);
        self
    }

    #[allow(dead_code)]
    pub fn where_identifiers_are(mut self, identifiers: &'a [&Identifier]) -> Self {
        for identifier in identifiers.iter() {
            self.config.identifier.push(Filter::Is(identifier));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_building_block(mut self, building_block: Filter<&'a str>) -> Self {
        self.config.building_block.push(building_block);
        self
    }

    #[allow(dead_code)]
    pub fn where_building_blocks_are(mut self, building_blocks: &'a [&str]) -> Self {
        for building_block in building_blocks.iter() {
            self.config.building_block.push(Filter::Is(building_block));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context(mut self, header_context: Filter<&'a Context>) -> Self {
        self.config.header_context.push(header_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.config.header_context.push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context(mut self, data_context: Filter<&'a Context>) -> Self {
        self.config.data_context.push(data_context);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_contexts_are(mut self, contexts: &'a [Context]) -> Self {
        for context in contexts.iter() {
            self.config.data_context.push(Filter::Is(context));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kind(
        mut self,
        header_context_kind: Filter<&'a ContextKind>,
    ) -> Self {
        self.config.header_context_kind.push(header_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_header_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.config
                .header_context_kind
                .push(Filter::Is(context_kind));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kind(mut self, data_context_kind: Filter<&'a ContextKind>) -> Self {
        self.config.data_context_kind.push(data_context_kind);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_context_kinds_are(mut self, context_kinds: &'a [ContextKind]) -> Self {
        for context_kind in context_kinds.iter() {
            self.config.data_context_kind.push(Filter::Is(context_kind));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_fill_missing(mut self, fill_missing: Filter<&'a CellValue>) -> Self {
        self.config.fill_missing.push(fill_missing);
        self
    }

    #[allow(dead_code)]
    pub fn where_fill_missings_are(mut self, fill_missings: &'a [CellValue]) -> Self {
        for fill_missing in fill_missings.iter() {
            self.config.fill_missing.push(Filter::Is(fill_missing));
        }
        self
    }

    #[allow(dead_code)]
    pub fn where_data_type(mut self, data_type: Filter<&'a DataType>) -> Self {
        self.config.data_type.push(data_type);
        self
    }

    #[allow(dead_code)]
    pub fn where_data_types_are(mut self, data_types: &'a [DataType]) -> Self {
        for data_type in data_types.iter() {
            self.config.data_type.push(Filter::Is(data_type));
        }
        self
    }

    pub fn collect(self) -> Vec<&'a Column> {
        let series_filter = SeriesContextFilter::from_config(
            self.items.series_contexts(),
            SeriesContextFilterConfig::from(&self.config),
        );

        let scs = series_filter.collect();
        scs.iter()
            .flat_map(|sc| {
                self.items
                    .identify_columns(sc.get_identifier())
                    .into_iter()
                    .filter(|col| {
                        self.config.data_type.is_empty()
                            || self.config.data_type.iter().any(|f| match f {
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
