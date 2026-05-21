use crate::config::context::Context;
use crate::config::table_context::{Identifier, OutputDataType, SeriesContext};
use crate::config::traits::SeriesContextBuilding;
use crate::extract::ContextualizedDataFrame;
use crate::extract::enums::Filter;
use crate::transform::error::{MappingErrorInfo, PushMappingError, StrategyError};
use crate::transform::strategies::traits::Strategy;
use crate::types::HashableSet;
use polars::datatypes::AnyValue;
use polars::prelude::Column;
use std::any::type_name;
use std::cmp::PartialEq;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct SplitInstruction {
    members: HashSet<String>,
    post_split_data_context: Context,
    post_split_header_context: Context,
    output_data_type: OutputDataType,
}
impl Hash for SplitInstruction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut members: Vec<&String> = self.members.iter().collect();
        members.sort();
        members.hash(state);

        self.post_split_data_context.hash(state);
        self.post_split_header_context.hash(state);
        self.output_data_type.hash(state);
    }
}

impl PartialEq for SplitInstruction {
    fn eq(&self, other: &Self) -> bool {
        self.members == other.members
            && self.post_split_data_context == other.post_split_data_context
            && self.post_split_header_context == other.post_split_header_context
            && self.output_data_type == other.output_data_type
    }
}

impl Eq for SplitInstruction {}
impl SplitInstruction {
    fn col_name(&self) -> String {
        let id = uuid::Uuid::new_v4().simple().to_string();

        format!(
            "split_{}_{}_{}",
            self.post_split_data_context,
            self.post_split_header_context,
            &id[..10]
        )
    }
}

#[derive(Debug)]
pub struct MultiSplitterStrategy {
    groups: HashSet<SplitInstruction>,
    keep_building_block: bool,
    drip_header_data: bool,
}

impl MultiSplitterStrategy {
    pub fn new(
        groups: HashSet<SplitInstruction>,
        keep_building_block: bool,
        drip_header_data: bool,
    ) -> Self {
        MultiSplitterStrategy {
            groups,
            keep_building_block,
            drip_header_data,
        }
    }

    fn context(&self) -> Context {
        Context::MultiContext(HashableSet::from(
            self.groups
                .iter()
                .map(|i| i.post_split_data_context.clone())
                .collect::<Vec<Context>>(),
        ))
    }
}

impl Strategy for MultiSplitterStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        let needed_context = self.context();
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_data_context(Filter::Is(&needed_context))
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        let needed_context = self.context();
        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        for table in tables.iter_mut() {
            let multi_col_names = table
                .filter_columns()
                .where_data_context(Filter::Is(&needed_context))
                .collect_owned_names();

            for multi_col_name in multi_col_names {
                let mut groups: HashMap<&SplitInstruction, Vec<AnyValue>> =
                    self.groups.iter().map(|group| (group, vec![])).collect();

                let mut multi_col = table.data().column(&multi_col_name)?.clone();

                for col_entry_opt in multi_col.str()?.iter() {
                    match col_entry_opt {
                        Some(entry) => {
                            let mut any_match = false;
                            for g in self.groups.iter() {
                                if g.members.contains(entry) {
                                    groups
                                        .get_mut(g)
                                        .expect("should be here")
                                        .push(AnyValue::String(entry));
                                    any_match = true;
                                    continue;
                                } else {
                                    groups
                                        .get_mut(g)
                                        .expect("should be here")
                                        .push(AnyValue::Null);
                                }
                            }

                            if !any_match {
                                error_info.insert_error(
                                    multi_col.name().to_string(),
                                    table.context().name().to_string(),
                                    entry.to_string(),
                                    vec![],
                                );
                            }
                        }
                        None => {
                            for g in self.groups.iter() {
                                groups
                                    .get_mut(g)
                                    .expect("should be here")
                                    .push(AnyValue::Null);
                            }
                        }
                    }
                }

                if !error_info.is_empty() {
                    return Err(StrategyError::MappingError {
                        strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                        message: "Could not find match in Multi Split.".to_string(),
                        info: error_info.into_iter().collect(),
                    });
                }

                let original_sc_building_block = table
                    .get_sc_by_col_name(&multi_col_name)
                    .expect("original sc should still be here")
                    .get_building_block_id()
                    .map(|s| s.to_string());

                let original_sc_header_context = table
                    .get_sc_by_col_name(&multi_col_name)
                    .expect("original sc should still be here")
                    .get_header_context()
                    .clone();

                for (g, col_values) in groups {
                    let new_col = Column::new(g.col_name().into(), col_values);

                    let mut sc = SeriesContext::from_identifier(new_col.name().as_str())
                        .with_header_context(g.post_split_header_context.clone())
                        .with_data_context(g.post_split_data_context.clone());

                    if self.keep_building_block {
                        sc = sc.with_building_block_id(original_sc_building_block.as_ref()); // TODO
                    }

                    table
                        .builder()
                        .insert_col_with_context(
                            new_col,
                            g.post_split_header_context.clone(),
                            g.post_split_data_context.clone(),
                        )?
                        .build()?;
                }

                if original_sc_header_context != Context::None && self.drip_header_data {
                    let drip_data = Column::new(
                        multi_col_name.as_str().into(),
                        vec![multi_col_name.clone(); multi_col.len()],
                    );
                    table
                        .builder()
                        .replace_col(
                            multi_col_name.as_str(),
                            drip_data.take_materialized_series(),
                        )?
                        .build()?;
                }
            }

            if !self.drip_header_data {
                let identifiers: Vec<Identifier> = table
                    .filter_series_context()
                    .where_data_context(Filter::Is(&needed_context))
                    .collect()
                    .iter()
                    .map(|s| s.get_identifier().clone())
                    .collect();

                for multi_col_scs in identifiers {
                    table
                        .builder()
                        .drop_sc_alongside_cols(&multi_col_scs)?
                        .build()?;
                }
            }
        }

        Ok(())
    }
}
