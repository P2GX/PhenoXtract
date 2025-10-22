#![allow(clippy::too_many_arguments)]
use crate::constants::ISO8601_DUR_PATTERN;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::transform::error::TransformError;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use chrono::{TimeZone, Utc};
use log::warn;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::time_element::Element::{Age, Timestamp};
use phenopackets::schema::v2::core::vital_status::Status;
use phenopackets::schema::v2::core::{
    Age as IndividualAge, Individual, OntologyClass, PhenotypicFeature, Sex, TimeElement,
    VitalStatus,
};
use prost_types::Timestamp as TimestampProtobuf;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug)]
pub struct VariantSyntaxParser {
}

impl VariantSyntaxParser {
    pub fn new() -> VariantSyntaxParser {VariantSyntaxParser {}}

    pub fn parse_syntax(variant_description: &str) -> String {
        // what this REALLY should do is send off the variant to VariantValidator

    }
}